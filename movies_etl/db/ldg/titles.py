from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class Titles(BaseModel):
    movieid: str
    series_title: str
    released_year: int
    certificate: str | None
    runtime: int
    genre: str
    imdb_rating: float
    overview: str
    meta_score: int
    director: str
    votes_count: int
    gross: int
