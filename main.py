from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv
from chatbot_postgres import PropertyChatbotDB

load_dotenv()

app = FastAPI(
    title="Property Finder Chatbot API",
    description="AI-powered chatbot for real estate property searches",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize chatbot
chatbot = PropertyChatbotDB()

# Request/Response models
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None

class PropertyInfo(BaseModel):
    listing_key: str
    address: Optional[str]
    city: Optional[str]
    price: Optional[float]
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    property_type: Optional[str]
    description: Optional[str]

class ChatResponse(BaseModel):
    success: bool
    message: str
    properties: List[Dict]
    property_count: int
    session_id: Optional[str]
    media_urls: List[str] = []

# Endpoints
@app.get("/")
async def root():
    """Root endpoint - API information"""
    return {
        "name": "Property Finder Chatbot API",
        "version": "1.0.0",
        "status": "active",
        "endpoints": {
            "chat": "/api/v1/chat",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "property-chatbot-api"}

@app.post("/api/v1/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Main chat endpoint for property searches
    
    Example requests:
    - "Show me houses in Toronto under 1 million"
    - "Find 3 bedroom homes in Mississauga"
    - "What commercial properties are available?"
    """
    try:
        # Process the message through the PostgreSQL chatbot
        result = chatbot.process_message(request.message)
        
        # Format properties for response
        formatted_properties = []
        for prop in result.get("properties", [])[:10]:
            formatted_properties.append({
                "listing_key": prop.get("listing_key"),
                "address": prop.get("unparsed_address"),
                "city": prop.get("city"),
                "postal_code": prop.get("postal_code"),
                "price": prop.get("list_price"),
                "bedrooms": prop.get("bedrooms"),
                "bathrooms": prop.get("bathrooms"),
                "property_type": prop.get("property_subtype") or prop.get("property_type"),
                "status": prop.get("standard_status"),
                "description": prop.get("public_remarks", "")[:200] if prop.get("public_remarks") else None,
                "photo_count": prop.get("photo_count", 0),
                "media_urls": [m.get("media_url") for m in prop.get("media", [])[:3]] if prop.get("media") else []
            })
        
        return ChatResponse(
            success=result.get("success", True),
            message=result.get("message", ""),
            properties=formatted_properties,
            property_count=len(formatted_properties),
            session_id=request.session_id,
            media_urls=result.get("media_urls", [])
        )
    
    except Exception as e:
        # If all else fails, return a helpful error message
        return ChatResponse(
            success=False,
            message=f"I'm having trouble searching properties right now. Try asking: 'Show me available properties' or 'Help'",
            properties=[],
            property_count=0,
            session_id=request.session_id
        )

@app.get("/api/v1/cities")
async def get_cities():
    """
    Get all available cities with property counts
    
    Returns list of cities sorted by number of properties
    """
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            database=os.getenv("DB_NAME", "property_chatbot"),
            cursor_factory=RealDictCursor
        )
        cursor = conn.cursor()
        
        # Get cities with property counts
        cursor.execute("""
            SELECT 
                city,
                COUNT(*) as property_count,
                MIN(list_price) as min_price,
                MAX(list_price) as max_price,
                AVG(list_price)::INTEGER as avg_price
            FROM properties
            WHERE city IS NOT NULL
            GROUP BY city
            ORDER BY property_count DESC
        """)
        
        cities = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "total_cities": len(cities),
            "cities": [dict(c) for c in cities]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/property/{listing_key}")
async def get_property_details(listing_key: str):
    """
    Get detailed information about a specific property
    
    Args:
        listing_key: The unique property identifier (e.g., "X9502994")
    """
    try:
        # Use the AMPRE client directly
        from ampre_client import AMPREClient
        client = AMPREClient()
        result = client.get_property_details(listing_key)
        
        if result["success"] and result["property"]:
            prop = result["property"]
            return {
                "success": True,
                "property": {
                    "listing_key": prop.get("ListingKey"),
                    "full_address": prop.get("UnparsedAddress"),
                    "price": prop.get("ListPrice"),
                    "bedrooms": prop.get("BedroomsTotal"),
                    "bathrooms": prop.get("BathroomsTotalInteger"),
                    "property_type": prop.get("PropertyType"),
                    "property_subtype": prop.get("PropertySubType"),
                    "lot_size": prop.get("LotSizeArea"),
                    "year_built": prop.get("YearBuilt"),
                    "description": prop.get("PublicRemarks"),
                    "status": prop.get("MlsStatus"),
                    "tax_amount": prop.get("TaxAnnualAmount"),
                    "listing_office": prop.get("ListOfficeName"),
                    "virtual_tour": prop.get("VirtualTourURLUnbranded")
                }
            }
        else:
            raise HTTPException(status_code=404, detail="Property not found")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/search")
async def search_properties(
    city: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    bedrooms: Optional[int] = None,
    bathrooms: Optional[int] = None,
    property_type: Optional[str] = None,
    limit: int = 10
):
    """
    Direct property search endpoint without natural language processing
    """
    try:
        # Use the chatbot's search method directly
        result = chatbot.search_properties(
            city=city,
            min_price=min_price,
            max_price=max_price,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            property_type=property_type
        )
        
        return {
            "success": True,
            "properties": result[:limit]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Run the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )