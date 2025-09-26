import os
import httpx
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

AMPRE_TOKEN = os.getenv("AMPRE_TOKEN")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "database": os.getenv("DB_NAME", "property_chatbot")
}

# -------------------------------
# Utility Safe Extractor
# -------------------------------
def safe_str(val, max_len=None):
    if val is None:
        return None
    s = str(val)
    return s[:max_len] if max_len else s

# -------------------------------
# Fetcher Class
# -------------------------------
class PropertyFetcher:
    def __init__(self, cursor, conn, token):
        self.cursor = cursor
        self.conn = conn
        self.token = token
        self.base_url = "https://query.ampre.ca/odata"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }

    # -------------------------------
    # Fetch properties batch
    # -------------------------------
    def fetch_properties(self, top=100, skip=0):
        url = (
            f"{self.base_url}/Property"
            f"?$top={top}&$skip={skip}"
            f"&$select=ListingKey,City,ListPrice,BedroomsTotal,"
            f"BathroomsTotalInteger,PropertyType,PropertySubType,"
            f"YearBuilt,StandardStatus,ModificationTimestamp,PublicRemarks,"
            f"UnparsedAddress,PostalCode,StateOrProvince"
        )
        r = httpx.get(url, headers=self.headers, timeout=60)
        if r.status_code == 200:
            return r.json().get("value", [])
        else:
            print(f"⚠️ Property fetch failed: {r.status_code} {r.text[:200]}")
            return []

    # -------------------------------
    # Insert properties
    # -------------------------------
    def insert_properties(self, properties):
        property_data = []

        for p in properties:
            property_data.append((
                safe_str(p.get("ListingKey"), 50),
                safe_str(p.get("UnparsedAddress"), 255),
                safe_str(p.get("City"), 100),
                safe_str(p.get("StateOrProvince"), 10),
                safe_str(p.get("PostalCode"), 10),
                p.get("ListPrice"),
                safe_str(p.get("StandardStatus"), 50),
                safe_str(p.get("PropertyType"), 50),
                safe_str(p.get("PropertySubType"), 100),
                p.get("BedroomsTotal"),
                p.get("BathroomsTotalInteger"),
                p.get("YearBuilt"),
                safe_str(p.get("PublicRemarks")),
                p.get("ModificationTimestamp"),
            ))

        property_query = """
            INSERT INTO properties (
                listing_key, unparsed_address, city, state_province, postal_code,
                list_price, standard_status, property_type, property_subtype,
                bedrooms, bathrooms, year_built, public_remarks, last_updated
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (listing_key) DO UPDATE SET
                unparsed_address = EXCLUDED.unparsed_address,
                city = EXCLUDED.city,
                state_province = EXCLUDED.state_province,
                postal_code = EXCLUDED.postal_code,
                list_price = EXCLUDED.list_price,
                standard_status = EXCLUDED.standard_status,
                property_type = EXCLUDED.property_type,
                property_subtype = EXCLUDED.property_subtype,
                bedrooms = EXCLUDED.bedrooms,
                bathrooms = EXCLUDED.bathrooms,
                year_built = EXCLUDED.year_built,
                public_remarks = EXCLUDED.public_remarks,
                last_updated = EXCLUDED.last_updated;
        """

        try:
            if property_data:
                execute_batch(self.cursor, property_query, property_data, page_size=100)
            self.conn.commit()
            print(f"✅ Inserted/Updated {len(property_data)} properties")
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Error inserting properties: {e}")

    # -------------------------------
    # Fetch media for properties
    # -------------------------------
    def fetch_media_for_properties(self, listing_keys):
        media_records = []
        for key in listing_keys:
            filter_str = f"ResourceName eq 'Property' and ResourceRecordKey eq '{key}'"
            url = (
                f"{self.base_url}/Media"
                f"?$filter={filter_str}"
                f"&$select=ResourceRecordKey,MediaKey,MediaURL,MediaType,"
                f"MediaCategory,Order,PreferredPhotoYN,ShortDescription"
            )
            try:
                r = httpx.get(url, headers=self.headers, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    for item in data.get("value", []):
                        media_records.append((
                            safe_str(item.get("ResourceRecordKey"), 50),
                            item.get("MediaURL"),
                            safe_str(item.get("MediaType"), 50),
                            safe_str(item.get("MediaCategory"), 50),
                            item.get("Order"),
                            item.get("ShortDescription"),
                            item.get("PreferredPhotoYN") is True
                        ))
                else:
                    print(f"⚠️ Media fetch failed ({r.status_code}) for {key}: {r.text[:120]}")
            except Exception as e:
                print(f"❌ Error fetching media for {key}: {e}")

        if media_records:
            self.insert_media(media_records)

    # -------------------------------
    # Insert media
    # -------------------------------
    def insert_media(self, media_records):
        query = """
            INSERT INTO property_media (
                listing_key, media_url, media_type, media_category,
                display_order, description, is_primary
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING;
        """
        try:
            execute_batch(self.cursor, query, media_records, page_size=100)
            self.conn.commit()
            print(f"✅ Inserted {len(media_records)} media records")
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Error inserting media: {e}")

    # -------------------------------
    # Fetch all
    # -------------------------------
    def fetch_all(self, limit=500, fetch_media=True):
        total = 0
        for skip in range(0, limit, 100):
            props = self.fetch_properties(top=100, skip=skip)
            if not props:
                break
            self.insert_properties(props)
            keys = [p.get("ListingKey") for p in props if p.get("ListingKey")]
            if fetch_media and keys:
                print(f"Fetching media for {len(keys)} properties...")
                self.fetch_media_for_properties(keys)
            total += len(props)
            print(f"Batch {skip//100+1}: total fetched {total}")
        print(f"FETCH COMPLETE: {total} properties")

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    fetcher = PropertyFetcher(cursor, conn, AMPRE_TOKEN)
    fetcher.fetch_all(limit=10000, fetch_media=True)

    # Refresh cities table
    cursor.execute("TRUNCATE cities RESTART IDENTITY")
    cursor.execute("""
        INSERT INTO cities (name)
        SELECT DISTINCT city
        FROM properties
        WHERE city IS NOT NULL
    """)
    conn.commit()
    print("✅ Cities table refreshed")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
