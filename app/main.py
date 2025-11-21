from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.models.unified_record import UnifiedScholarlyRecord
from app.services.sqlite_backend import init_db, SqliteSearchBackend
from app.services.omeka_connector import OmekaConnector, OmekaRepoConfig
from app.services.dspace_connector import DspaceConnector, DspaceRepoConfig

app = FastAPI(
    title="Buscador Unificado de Repositorios (MVP)",
    version="0.4.0",
    description=(
        "API para cosecha OAI-PMH y búsqueda unificada sobre "
        "Omeka UH y DSpace UCLV, con cosecha incremental."
    ),
)

templates = Jinja2Templates(directory="app/templates")

# Backend de búsqueda / persistencia (SQLite)
backend = SqliteSearchBackend()

# --- Configuración de repositorios ---

OMEKA_UH_CONFIG = OmekaRepoConfig(
    repo_id="omeka_uh",
    base_url="https://accesoabierto.uh.cu/s/scriptorium/oai",
    metadata_prefix="oai_dc",
    set_spec="2161263",
    institution="Universidad de La Habana",
    repository="Omeka UH Scriptorium",
)

DSPACE_UCLV_CONFIG = DspaceRepoConfig(
    repo_id="dspace_uclv",
    base_url="https://dspace.uclv.edu.cu/server/oai/request",
    metadata_prefix="oai_dc",
    set_spec=None,  # o un set concreto si decides filtrar colecciones
    institution='Universidad Central "Marta Abreu" de Las Villas',
    repository="DSpace UCLV",
)

omeka_connector = OmekaConnector(OMEKA_UH_CONFIG, backend)
dspace_connector = DspaceConnector(DSPACE_UCLV_CONFIG, backend)


@app.on_event("startup")
def on_startup() -> None:
    """
    Inicializa la base de datos SQLite al arrancar el servicio.
    """
    init_db()


# --- UI HTML ---

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    """
    Página principal del buscador unificado (HTML).
    """
    return templates.TemplateResponse("search.html", {"request": request})


# --- Healthcheck ---

@app.get("/health")
def health() -> dict:
    """
    Comprobación simple de vida del servicio.
    """
    return {"status": "ok"}


# --- Inserción manual de registros ---

@app.post("/records", response_model=UnifiedScholarlyRecord)
def add_record(record: UnifiedScholarlyRecord) -> UnifiedScholarlyRecord:
    """
    Inserta (o actualiza) un registro manualmente en el índice.
    Útil para pruebas o cargas puntuales fuera de OAI-PMH.
    """
    backend.index_record(record)
    return record


# --- BÚSQUEDA SIMPLE ---

@app.get("/search", response_model=List[UnifiedScholarlyRecord])
def search(
    q: str = Query(..., description="Texto a buscar en título, resumen, autores o palabras clave"),
    limit: int = Query(10, ge=1, le=100, description="Número máximo de resultados"),
) -> List[UnifiedScholarlyRecord]:
    """
    Búsqueda simple sobre campos textuales (LIKE).
    """
    return backend.search_simple(q, limit=limit)


# --- BÚSQUEDA AVANZADA ---

@app.get("/search/advanced", response_model=List[UnifiedScholarlyRecord])
def search_advanced(
    q: Optional[str] = Query(None, description="Texto a buscar (opcional)"),
    type: Optional[str] = Query(None, description="Tipo de documento (dc:type)"),
    repository: Optional[str] = Query(None, description="Nombre del repositorio"),
    year_from: Optional[int] = Query(None, ge=1000, le=9999, description="Año desde"),
    year_to: Optional[int] = Query(None, ge=1000, le=9999, description="Año hasta"),
    page: int = Query(1, ge=1, description="Número de página (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Tamaño de página"),
) -> List[UnifiedScholarlyRecord]:
    """
    Búsqueda con filtros por tipo, repositorio y rango de años, con paginación.
    """
    return backend.search_advanced(
        query=q,
        type_filter=type,
        repository_filter=repository,
        year_from=year_from,
        year_to=year_to,
        page=page,
        page_size=page_size,
    )


# --- HARVEST OMEKA ---

@app.post("/harvest/omeka/full")
def harvest_omeka_full() -> dict:
    """
    Cosecha completa del Omeka UH (set 2161263).
    Actualiza la fecha de última cosecha en harvest_state.
    """
    try:
        count = omeka_connector.harvest_full()
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error cosechando Omeka (full): {e}",
        )
    return {"harvested_records": count}


@app.post("/harvest/omeka/incremental")
def harvest_omeka_incremental() -> dict:
    """
    Cosecha incremental del Omeka UH, usando from=última fecha guardada.
    """
    try:
        count = omeka_connector.harvest_incremental()
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error cosechando Omeka (incremental): {e}",
        )
    return {"harvested_records": count}


# --- HARVEST DSPACE UCLV ---

@app.post("/harvest/dspace/uclv/full")
def harvest_dspace_uclv_full() -> dict:
    """
    Cosecha completa del DSpace UCLV (o del set configurado).
    """
    try:
        count = dspace_connector.harvest_full()
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error cosechando DSpace UCLV (full): {e}",
        )
    return {"harvested_records": count}


@app.post("/harvest/dspace/uclv/incremental")
def harvest_dspace_uclv_incremental() -> dict:
    """
    Cosecha incremental del DSpace UCLV, usando from=última fecha guardada.
    """
    try:
        count = dspace_connector.harvest_incremental()
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error cosechando DSpace UCLV (incremental): {e}",
        )
    return {"harvested_records": count}


# --- ESTADÍSTICAS DE CONTENIDO ---

@app.get("/stats/content")
def stats_content() -> dict:
    """
    Estadísticas básicas de contenido:
    - por repositorio
    - por tipo de documento
    - por año de publicación (date_issued)
    """
    return {
        "by_repository": backend.stats_by_repository(),
        "by_type": backend.stats_by_type(),
        "by_year": backend.stats_by_year(),
    }
