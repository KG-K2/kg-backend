from pydantic import BaseModel
from typing import Optional, List, Any, Union

class QueryRequest(BaseModel):
    query: str

class SearchResult(BaseModel):
    id: Union[str, int]
    type: str
    label: str
    score: float
    details: Optional[dict] = {}

class SearchResponse(BaseModel):
    results: List[SearchResult]

class ArtworkDetail(BaseModel):
    id: int
    title: str
    url: Optional[str] = None
    year: Optional[str] = None     
    medium: Optional[str] = None
    dimensions: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None 
    type: str = "Artwork"

class ArtistDetail(BaseModel):
    id: Union[str, int] 
    name: str
    bio: Optional[str] = None
    nationality: Optional[str] = None
    image: Optional[str] = None 
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    period: Optional[str] = None
    school: Optional[str] = None
    base: Optional[str] = None 
    type: str = "Artist"
    artworks: List[ArtworkDetail] = [] 

class ArtworkPageResponse(BaseModel):
    artwork: ArtworkDetail
    artist: Optional[ArtistDetail] = None