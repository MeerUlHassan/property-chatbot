import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os
from typing import Dict, List, Optional
from openai import OpenAI
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

class PropertyChatbotDB:
    """PostgreSQL-based property chatbot"""
    
    def __init__(self):
        self.openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        
    def extract_parameters(self, message: str) -> Dict:
        """Extract search parameters using GPT-4"""
        prompt = """Extract property search parameters from the user message.
        Return JSON with these fields (only include mentioned ones):
        - city: city name
        - min_price: minimum price
        - max_price: maximum price
        - bedrooms: number of bedrooms
        - bathrooms: number of bathrooms
        - property_type: property type
        
        Example: "3 bedroom house in Toronto under 1 million"
        Returns: {"city": "Toronto", "bedrooms": 3, "max_price": 1000000}"""
        
        try:
            response = self.openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": message}
                ],
                temperature=0.3
            )
            return json.loads(response.choices[0].message.content)
        except:
            return {}
    
    def search_properties(self, **params) -> List[Dict]:
        """Search properties in PostgreSQL"""
        cursor = self.conn.cursor()
        
        # Build query dynamically
        conditions = []
        values = []
        
        if params.get('city'):
            conditions.append("LOWER(city) LIKE LOWER(%s)")
            values.append(f"%{params['city']}%")
        
        if params.get('min_price'):
            conditions.append("list_price >= %s")
            values.append(params['min_price'])
            
        if params.get('max_price'):
            conditions.append("list_price <= %s")
            values.append(params['max_price'])
            
        if params.get('bedrooms'):
            conditions.append("bedrooms = %s")
            values.append(params['bedrooms'])
            
        if params.get('bathrooms'):
            conditions.append("bathrooms = %s")
            values.append(params['bathrooms'])
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT 
                listing_key,
                unparsed_address,
                city,
                postal_code,
                list_price,
                bedrooms,
                bathrooms,
                property_type,
                property_subtype,
                year_built,
                public_remarks,
                standard_status,
                (SELECT COUNT(*) FROM property_media WHERE listing_key = p.listing_key) as photo_count
            FROM properties p
            WHERE {where_clause}
            ORDER BY list_price DESC
            LIMIT 10
        """
        
        cursor.execute(query, values)
        properties = cursor.fetchall()
        
        # Get media for each property
        for prop in properties:
            cursor.execute("""
                SELECT media_url, media_type, is_primary 
                FROM property_media 
                WHERE listing_key = %s 
                ORDER BY is_primary DESC, display_order 
                LIMIT 5
            """, (prop['listing_key'],))
            prop['media'] = cursor.fetchall()
        
        cursor.close()
        return properties
    
    def format_response(self, properties: List[Dict], user_message: str) -> str:
        """Format properties into natural language response"""
        if not properties:
            return "I couldn't find any properties matching your criteria. Try adjusting your search or ask 'what cities are available?'"
        
        # Prepare data for GPT
        props_summary = []
        for p in properties[:5]:
            props_summary.append({
                "address": p['unparsed_address'],
                "price": f"${p['list_price']:,.0f}" if p['list_price'] else "Price not listed",
                "beds": p['bedrooms'],
                "baths": p['bathrooms'],
                "type": p['property_subtype'] or p['property_type'],
                "photos": p['photo_count'],
                "description": (p['public_remarks'][:150] + "...") if p['public_remarks'] else ""
            })
        
        try:
            response = self.openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You're a helpful real estate assistant. Present properties clearly with key details."},
                    {"role": "user", "content": f"User asked: {user_message}\n\nProperties found:\n{json.dumps(props_summary)}\n\nPresent these nicely."}
                ],
                temperature=0.7
            )
            return response.choices[0].message.content
        except:
            # Fallback
            result = f"Found {len(properties)} properties:\n\n"
            for i, p in enumerate(properties[:5], 1):
                result += f"{i}. {p['unparsed_address']}\n"
                result += f"   ${p['list_price']:,.0f} | {p['bedrooms']} bed, {p['bathrooms']} bath\n"
                result += f"   {p['photo_count']} photos available\n\n"
            return result
    
    def get_available_cities(self) -> List[str]:
        """Get list of cities with properties"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT city, COUNT(*) as count 
            FROM properties 
            WHERE city IS NOT NULL 
            GROUP BY city 
            ORDER BY count DESC 
            LIMIT 20
        """)
        cities = cursor.fetchall()
        cursor.close()
        return [f"{c['city']} ({c['count']} properties)" for c in cities]
    
    def process_message(self, message: str) -> Dict:
        """Main method to process user messages"""
        
        # Handle help/cities request
        if 'help' in message.lower() or 'cities' in message.lower():
            cities = self.get_available_cities()
            return {
                "success": True,
                "message": f"Available cities:\n{chr(10).join(cities[:10])}\n\nTry: 'Show me houses in Toronto'",
                "properties": [],
                "media_urls": []
            }
        
        # Extract parameters and search
        params = self.extract_parameters(message)
        properties = self.search_properties(**params)
        
        # Format response
        response = self.format_response(properties, message)
        
        # Extract media URLs for first 3 properties
        media_urls = []
        for prop in properties[:3]:
            if prop.get('media'):
                for media in prop['media'][:3]:  # First 3 images per property
                    if media['media_url']:
                        media_urls.append(media['media_url'])
        
        return {
            "success": True,
            "message": response,
            "properties": [dict(p) for p in properties], 
            "media_urls": media_urls[:9]  # Max 9 images
        }
    
    def close(self):
        """Close database connection"""
        self.conn.close()


# Test the chatbot
if __name__ == "__main__":
    bot = PropertyChatbotDB()
    
    test_queries = [
        "Show me houses in Toronto under 1 million",
        "3 bedroom properties in Mississauga",
        "What cities are available?"
    ]
    
    for query in test_queries:
        print(f"\n{'='*50}")
        print(f"Query: {query}")
        print('-'*50)
        result = bot.process_message(query)
        print(f"Response: {result['message'][:300]}...")
        print(f"Properties found: {len(result['properties'])}")
        print(f"Media URLs: {len(result['media_urls'])}")
    
    bot.close()