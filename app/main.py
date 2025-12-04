from fastapi import FastAPI, HTTPException
from app.models import QueryRequest, SearchResponse, ArtistDetail, ArtworkPageResponse
# Import driver juga dari services untuk dipakai session-nya
from app.services import run_custom_query, search_graph, get_artwork_by_id, get_artist_by_name, driver 
from fastapi.middleware.cors import CORSMiddleware
from app.services import (
    run_custom_query, search_graph, get_artwork_by_id, get_artist_by_name, 
    get_location_details, get_movement_details, driver 
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/run-query")
async def run_query(request: QueryRequest):
    try:
        result = run_custom_query(request.query)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {e}")
    
@app.get("/search", response_model=SearchResponse)
async def search(q: str):
    if not q:
        raise HTTPException(status_code=400, detail="Query empty")
    results = search_graph(q)
    return {"results": results}

@app.get("/artwork/{art_id}", response_model=ArtworkPageResponse)
def read_artwork(art_id: int):
    with driver.session() as session:
        result = session.execute_read(get_artwork_by_id, art_id)
        if not result:
            raise HTTPException(status_code=404, detail="Artwork not found")
        return result

@app.get("/artist/{artist_name}", response_model=ArtistDetail)
def read_artist(artist_name: str):
    # Decode URL component otomatis dilakukan FastAPI, tapi kita strip() di service
    with driver.session() as session:
        result = session.execute_read(get_artist_by_name, artist_name)
        if not result:
            raise HTTPException(status_code=404, detail="Artist not found")
        return result
    
@app.get("/location/{name}")
def read_location(name: str):
    with driver.session() as session:
        result = session.execute_read(get_location_details, name)
        if not result:
            raise HTTPException(status_code=404, detail="Location not found")
        return result

@app.get("/movement/{name}")
def read_movement(name: str):
    with driver.session() as session:
        result = session.execute_read(get_movement_details, name)
        if not result:
            raise HTTPException(status_code=404, detail="Movement not found")
        return result