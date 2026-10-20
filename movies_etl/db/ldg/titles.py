from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class Titles(BaseModel):
    movieid: str
    poster_link: str
    series_title: str
    released_year: int
    certificate: str | None
    runtime: int
    genre: str
    imdb_rating: float
    overview: str
    meta_score: int
    director: str
    no_of_votes: int
    gross: int
