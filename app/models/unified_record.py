from typing import List, Optional
from pydantic import BaseModel, HttpUrl
from datetime import datetime

class UnifiedScholarlyRecord(BaseModel):
    id: str
    oai_identifier: str

    title: str
    authors: List[str]

    institution: str
    repository: str

    date_issued: Optional[str] = None  # "YYYY" o "YYYY-MM-DD"
    type: Optional[str] = None

    url_landing_page: Optional[HttpUrl] = None

    abstract: Optional[str] = None
    keywords: List[str] = []
    language: Optional[str] = None
    collections: List[str] = []

    date_indexed: Optional[datetime] = None
