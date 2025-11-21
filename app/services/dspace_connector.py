from dataclasses import dataclass
from typing import Optional

from app.services.oai_client import OaiClient
from app.services.sqlite_backend import SqliteSearchBackend
from app.services.mapper_dspace import map_dspace_dc_to_record
from app.services.harvest_state import get_last_harvest_date, update_last_harvest_date


@dataclass
class DspaceRepoConfig:
    repo_id: str
    base_url: str
    metadata_prefix: str
    set_spec: Optional[str]
    institution: str
    repository: str


class DspaceConnector:
    def __init__(self, config: DspaceRepoConfig, backend: SqliteSearchBackend):
        self.config = config
        self.backend = backend
        self.client = OaiClient(
            base_url=config.base_url,
            metadata_prefix=config.metadata_prefix,
            set_spec=config.set_spec,
        )

    def harvest_full(self) -> int:
        """
        Cosecha completa (sin usar estado previo).
        """
        count = 0
        for oai_identifier, dc_el in self.client.iter_records():
            record = map_dspace_dc_to_record(
                oai_identifier=oai_identifier,
                dc_el=dc_el,
                institution=self.config.institution,
                repository=self.config.repository,
            )
            self.backend.index_record(record)
            count += 1

        update_last_harvest_date(self.config.repo_id)
        return count

    def harvest_incremental(self) -> int:
        """
        Cosecha incremental: usa from=última_fecha_guardada si existe.
        """
        last_date = get_last_harvest_date(self.config.repo_id)
        count = 0
        for oai_identifier, dc_el in self.client.iter_records(from_date=last_date):
            record = map_dspace_dc_to_record(
                oai_identifier=oai_identifier,
                dc_el=dc_el,
                institution=self.config.institution,
                repository=self.config.repository,
            )
            self.backend.index_record(record)
            count += 1

        if count > 0:
            update_last_harvest_date(self.config.repo_id)
        return count
