from typing import List
import xml.etree.ElementTree as ET
from datetime import datetime

from app.models.unified_record import UnifiedScholarlyRecord


NS_DC = "{http://purl.org/dc/elements/1.1/}"


def _get_first(dc_el: ET.Element, tag: str) -> str | None:
    for el in dc_el.findall(f"{NS_DC}{tag}"):
        text = (el.text or "").strip()
        if text:
            return text
    return None


def _get_all(dc_el: ET.Element, tag: str) -> List[str]:
    values: List[str] = []
    for el in dc_el.findall(f"{NS_DC}{tag}"):
        text = (el.text or "").strip()
        if text:
            values.append(text)
    return values


def map_omeka_dc_to_record(
    oai_identifier: str,
    dc_el: ET.Element,
    institution: str,
    repository: str,
) -> UnifiedScholarlyRecord:
    title = _get_first(dc_el, "title") or "Sin título"

    authors = _get_all(dc_el, "creator")
    if not authors:
        authors = _get_all(dc_el, "contributor")

    abstract = _get_first(dc_el, "description")
    keywords = _get_all(dc_el, "subject")
    date_issued = _get_first(dc_el, "date")
    type_ = _get_first(dc_el, "type")
    language = _get_first(dc_el, "language")

    # Buscar URL en dc:identifier
    url_landing = None
    for val in _get_all(dc_el, "identifier"):
        if val.startswith("http://") or val.startswith("https://"):
            url_landing = val
            break

    record = UnifiedScholarlyRecord(
        id=oai_identifier,  # para MVP usamos el mismo id
        oai_identifier=oai_identifier,
        title=title,
        authors=authors,
        institution=institution,
        repository=repository,
        date_issued=date_issued,
        type=type_,
        url_landing_page=url_landing,
        abstract=abstract,
        keywords=keywords,
        language=language,
        collections=[],  # si luego encontramos campo de colección, se añade aquí
        date_indexed=datetime.utcnow(),
    )
    return record
