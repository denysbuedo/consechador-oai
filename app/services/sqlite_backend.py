import sqlite3
from contextlib import contextmanager
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.models.unified_record import UnifiedScholarlyRecord


DB_PATH = "search_index.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        cursor = conn.cursor()

        # Tabla principal con los campos estructurados
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id TEXT PRIMARY KEY,
            oai_identifier TEXT,
            title TEXT,
            authors TEXT,
            institution TEXT,
            repository TEXT,
            date_issued TEXT,
            type TEXT,
            url_landing_page TEXT,
            abstract TEXT,
            keywords TEXT,
            language TEXT,
            collections TEXT,
            date_indexed TEXT
        )
        """)

	# NUEVO: tabla para guardar estado de cosecha por repositorio
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS harvest_state (
            repo_id TEXT PRIMARY KEY,
            last_harvest_date TEXT
        )
        """)

        # Índices básicos para acelerar búsquedas y stats
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_repository ON records(repository)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_type ON records(type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_date_issued ON records(date_issued)")

        conn.commit()


class SqliteSearchBackend:
    def index_record(self, record: UnifiedScholarlyRecord) -> None:
        authors_str = "; ".join(record.authors)
        keywords_str = "; ".join(record.keywords)
        collections_str = "; ".join(record.collections)

        with get_conn() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                INSERT OR REPLACE INTO records (
                    id, oai_identifier, title, authors, institution, repository,
                    date_issued, type, url_landing_page, abstract, keywords,
                    language, collections, date_indexed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.id,
                record.oai_identifier,
                record.title,
                authors_str,
                record.institution,
                record.repository,
                record.date_issued,
                record.type,
                str(record.url_landing_page) if record.url_landing_page else None,
                record.abstract,
                keywords_str,
                record.language,
                collections_str,
                (record.date_indexed or datetime.utcnow()).isoformat()
            ))

            conn.commit()

    # --- BÚSQUEDA SIMPLE (como la que ya usas) ---

    def search_simple(self, query: str, limit: int = 10) -> List[UnifiedScholarlyRecord]:
        pattern = f"%{query}%"

        with get_conn() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT *
                FROM records
                WHERE title LIKE ?
                   OR abstract LIKE ?
                   OR authors LIKE ?
                   OR keywords LIKE ?
                LIMIT ?
            """, (pattern, pattern, pattern, pattern, limit))

            rows = cursor.fetchall()
            return [self._row_to_record(row) for row in rows]

    # --- BÚSQUEDA AVANZADA CON FILTROS Y PAGINACIÓN ---

    def search_advanced(
        self,
        query: Optional[str],
        type_filter: Optional[str],
        repository_filter: Optional[str],
        year_from: Optional[int],
        year_to: Optional[int],
        page: int,
        page_size: int,
    ) -> List[UnifiedScholarlyRecord]:
        conditions = []
        params: list[Any] = []

        if query:
            pattern = f"%{query}%"
            conditions.append("(title LIKE ? OR abstract LIKE ? OR authors LIKE ? OR keywords LIKE ?)")
            params.extend([pattern, pattern, pattern, pattern])

        if type_filter:
            conditions.append("type = ?")
            params.append(type_filter)

        if repository_filter:
            conditions.append("repository = ?")
            params.append(repository_filter)

        # date_issued se guarda como texto; cogemos el año con substr
        if year_from is not None:
            conditions.append("CAST(substr(date_issued, 1, 4) AS INTEGER) >= ?")
            params.append(year_from)

        if year_to is not None:
            conditions.append("CAST(substr(date_issued, 1, 4) AS INTEGER) <= ?")
            params.append(year_to)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        offset = (page - 1) * page_size

        sql = f"""
            SELECT *
            FROM records
            {where_clause}
            ORDER BY date_issued DESC
            LIMIT ? OFFSET ?
        """
        params.extend([page_size, offset])

        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            return [self._row_to_record(row) for row in rows]

    # --- ESTADÍSTICAS ---

    def stats_by_repository(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT repository, COUNT(*) as count
                FROM records
                GROUP BY repository
                ORDER BY count DESC
            """)
            rows = cursor.fetchall()
            return [{"repository": r[0], "count": r[1]} for r in rows]

    def stats_by_type(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT type, COUNT(*) as count
                FROM records
                GROUP BY type
                ORDER BY count DESC
            """)
            rows = cursor.fetchall()
            return [{"type": r[0], "count": r[1]} for r in rows]

    def stats_by_year(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT CAST(substr(date_issued, 1, 4) AS INTEGER) AS year,
                       COUNT(*) as count
                FROM records
                WHERE date_issued IS NOT NULL AND substr(date_issued, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                GROUP BY year
                ORDER BY year
            """)
            rows = cursor.fetchall()
            return [{"year": r[0], "count": r[1]} for r in rows]

    # --- Helper ---

    def _row_to_record(self, row: tuple) -> UnifiedScholarlyRecord:
        (
            id_, oai_identifier, title, authors, institution, repository,
            date_issued, type_, url_landing_page, abstract, keywords,
            language, collections, date_indexed
        ) = row

        return UnifiedScholarlyRecord(
            id=id_,
            oai_identifier=oai_identifier,
            title=title,
            authors=[a.strip() for a in (authors or "").split(";") if a.strip()],
            institution=institution,
            repository=repository,
            date_issued=date_issued,
            type=type_,
            url_landing_page=url_landing_page,
            abstract=abstract,
            keywords=[k.strip() for k in (keywords or "").split(";") if k.strip()],
            language=language,
            collections=[c.strip() for c in (collections or "").split(";") if c.strip()],
            date_indexed=datetime.fromisoformat(date_indexed) if date_indexed else None
        )
