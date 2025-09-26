# setup_postgres_db.py
"""
PostgreSQL database setup for property chatbot
Based on Keller Williams website requirements
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "database": os.getenv("DB_NAME", "property_chatbot")
}

def create_database():
    """Create the database if it doesn't exist"""
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
        print(f"✅ Database '{DB_CONFIG['database']}' created successfully")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{DB_CONFIG['database']}' already exists")
    
    cursor.close()
    conn.close()

def create_tables():
    """Create tables based on Keller Williams website structure"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Drop existing tables (for clean setup)
    cursor.execute("DROP TABLE IF EXISTS property_media CASCADE")
    cursor.execute("DROP TABLE IF EXISTS properties CASCADE")
    
    # Main properties table with essential fields
    cursor.execute("""
        CREATE TABLE properties (
            -- Primary identifier
            listing_key VARCHAR(50) PRIMARY KEY,
            
            -- Location fields (as shown on KW site)
            street_number VARCHAR(20),
            street_name VARCHAR(100),
            street_suffix VARCHAR(20),
            city VARCHAR(100),
            state_province VARCHAR(10),
            postal_code VARCHAR(10),
            country VARCHAR(10) DEFAULT 'CA',
            unparsed_address VARCHAR(255),
            latitude DECIMAL(10, 7),
            longitude DECIMAL(10, 7),
            
            -- Price and status (prominent on KW)
            list_price DECIMAL(12, 2),
            original_price DECIMAL(12, 2),
            status VARCHAR(50),
            standard_status VARCHAR(50),
            
            -- Property details (shown in main features)
            bedrooms INTEGER,
            bathrooms INTEGER,
            property_type VARCHAR(50),
            property_subtype VARCHAR(100),
            year_built INTEGER,
            
            -- Size information
            living_area DECIMAL(10, 2),
            lot_size DECIMAL(10, 2),
            
            -- Description (main selling point)
            public_remarks TEXT,
            
            -- Agent/Office info (contact section)
            listing_office VARCHAR(200),
            listing_agent VARCHAR(200),
            agent_email VARCHAR(200),
            agent_phone VARCHAR(50),
            
            -- Virtual tour and media flags
            virtual_tour_url VARCHAR(500),
            has_photos BOOLEAN DEFAULT FALSE,
            photo_count INTEGER DEFAULT 0,
            
            -- Important dates
            listing_date TIMESTAMP,
            days_on_market INTEGER,
            last_updated TIMESTAMP,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Additional features for filtering
            garage_type VARCHAR(100),
            basement BOOLEAN,
            pool BOOLEAN,
            waterfront BOOLEAN,
            
            -- Taxes (shown in property details)
            annual_tax DECIMAL(10, 2)
        )
    """)
    
    # Media table for photos/videos
    cursor.execute("""
        CREATE TABLE property_media (
            id SERIAL PRIMARY KEY,
            listing_key VARCHAR(50) REFERENCES properties(listing_key) ON DELETE CASCADE,
            media_url VARCHAR(500),
            media_type VARCHAR(50),
            media_category VARCHAR(50),
            display_order INTEGER,
            description VARCHAR(255),
            is_primary BOOLEAN DEFAULT FALSE
        )
    """)
    
    # Create indexes separately (Postgres style)
    cursor.execute("CREATE INDEX idx_listing_media ON property_media(listing_key)")
    cursor.execute("CREATE INDEX idx_media_order ON property_media(listing_key, display_order)")
    
    # Create full-text search index for better search capabilities
    cursor.execute("""
        ALTER TABLE properties ADD COLUMN search_vector tsvector;
    """)
    
    cursor.execute("""
        UPDATE properties SET search_vector = 
            to_tsvector('english', 
                COALESCE(city, '') || ' ' || 
                COALESCE(street_name, '') || ' ' || 
                COALESCE(public_remarks, '') || ' ' ||
                COALESCE(property_subtype, '')
            );
    """)
    
    cursor.execute("CREATE INDEX idx_search ON properties USING GIN (search_vector)")
    
    conn.commit()
    print("✅ Tables created successfully")
    
    cursor.close()
    conn.close()


def show_schema_info():
    """Display information about the created schema"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Count tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    
    print("\n" + "=" * 60)
    print("DATABASE SCHEMA CREATED")
    print("=" * 60)
    
    print("\nTables:")
    for table in tables:
        print(f"  - {table[0]}")
        
        # Get column count for each table
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = '{table[0]}'
        """)
        col_count = cursor.fetchone()[0]
        print(f"    ({col_count} columns)")
    
    print("\nKey Features:")
    print("  ✓ Optimized for property search queries")
    print("  ✓ Full-text search enabled")
    print("  ✓ Indexed for fast filtering")
    print("  ✓ Media storage support")
    print("  ✓ Based on Keller Williams website structure")
    
    cursor.close()
    conn.close()

def main():
    print("=" * 60)
    print("POSTGRESQL DATABASE SETUP FOR PROPERTY CHATBOT")
    print("=" * 60)
    
    # Create database
    print("\n1. Creating database...")
    create_database()
    
    # Create tables
    print("\n2. Creating tables...")
    create_tables()
    
    # Show info
    show_schema_info()
    
    print("\n✅ Database setup complete!")
    print("\nNext steps:")
    print("1. Run fetch_to_postgres.py to populate the database")
    print("2. Update chatbot to query PostgreSQL instead of CSV/API")

if __name__ == "__main__":
    main()