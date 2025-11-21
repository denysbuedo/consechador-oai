from typing import Optional
from datetime import datetime

from app.services.sqlite_backend import get_conn


def get_last_harvest_date(repo_id: str) -> Optional[str]:
    """
    Devuelve la última fecha de cosecha almacenada para un repo (YYYY-MM-DD),
    o None si nunca se ha cosechado.
    """
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT last_harvest_date FROM harvest_state WHERE repo_id = ?",
            (repo_id,),
        )
        row = cursor.fetchone()
        if row and row[0]:
            return row[0]
    return None


def update_last_harvest_date(repo_id: str, date_str: Optional[str] = None) -> None:
    """
    Actualiza la última fecha de cosecha. Si no se especifica, usa hoy (UTC).
    """
    if date_str is None:
        date_str = datetime.utcnow().date().isoformat()

    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO harvest_state (repo_id, last_harvest_date)
            VALUES (?, ?)
            ON CONFLICT(repo_id) DO UPDATE SET last_harvest_date = excluded.last_harvest_date
            """,
            (repo_id, date_str),
        )
        conn.commit()
