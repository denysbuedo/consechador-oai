from typing import Iterator, Optional, Tuple

import httpx
from lxml import etree as ET


class OaiClient:
    """
    Cliente simple para OAI-PMH (ListRecords) con soporte de resumptionToken.
    Usa lxml con recover=True para tolerar XML malformado y trata errores 500
    en páginas con resumptionToken como fin de la cosecha.
    """

    def __init__(
        self,
        base_url: str,
        metadata_prefix: str = "oai_dc",
        set_spec: Optional[str] = None,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("?")
        self.metadata_prefix = metadata_prefix
        self.set_spec = set_spec
        self.timeout = timeout

    def iter_records(
        self,
        from_date: Optional[str] = None,
        until_date: Optional[str] = None,
    ) -> Iterator[Tuple[str, ET._Element]]:
        """
        Itera sobre los registros OAI-PMH y devuelve tuplas:
        (oai_identifier, elemento <dc> con metadatos)

        Si durante la paginación se obtiene un HTTP 500 con resumptionToken,
        se asume que el servidor tiene un bug y se termina la cosecha sin
        lanzar excepción (dejando los registros anteriores indexados).
        """
        params = {
            "verb": "ListRecords",
            "metadataPrefix": self.metadata_prefix,
        }
        if self.set_spec:
            params["set"] = self.set_spec
        if from_date:
            params["from"] = from_date
        if until_date:
            params["until"] = until_date

        resumption_token: Optional[str] = None

        parser = ET.XMLParser(recover=True)

        with httpx.Client(timeout=self.timeout) as client:
            while True:
                if resumption_token:
                    req_params = {
                        "verb": "ListRecords",
                        "resumptionToken": resumption_token,
                    }
                else:
                    req_params = params

                try:
                    resp = client.get(self.base_url, params=req_params)
                    resp.raise_for_status()
                except httpx.HTTPStatusError as e:
                    status = e.response.status_code
                    url = str(e.request.url)

                    # Caso especial: error 500 al pedir una página con resumptionToken.
                    # Lo tratamos como "fin de la cosecha" para no romper todo.
                    if resumption_token and status >= 500:
                        print(
                            f"[OaiClient] HTTP {status} con resumptionToken={resumption_token}, "
                            f"URL={url}. Terminando cosecha."
                        )
                        break

                    # Para otros casos (primer request, 4xx, etc.) sí consideramos error fatal
                    raise RuntimeError(
                        f"Error HTTP {status} en petición OAI: {url}"
                    ) from e

                # Parseo XML con recover=True (tolerante)
                try:
                    root = ET.fromstring(resp.content, parser=parser)
                except ET.XMLSyntaxError as e:
                    raise RuntimeError(
                        f"Error parseando respuesta OAI (XMLSyntaxError): {e}"
                    )

                ns = {
                    "oai": "http://www.openarchives.org/OAI/2.0/",
                    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
                    "dc": "http://purl.org/dc/elements/1.1/",
                }

                # Errores OAI explícitos
                error_el = root.find(".//oai:error", namespaces=ns)
                if error_el is not None:
                    raise RuntimeError(
                        f"OAI error: code={error_el.get('code')} text={error_el.text}"
                    )

                # Registros
                for rec in root.findall(".//oai:record", namespaces=ns):
                    header_el = rec.find("oai:header", namespaces=ns)
                    if header_el is None:
                        continue
                    oai_id_el = header_el.find("oai:identifier", namespaces=ns)
                    if oai_id_el is None or not (oai_id_el.text or "").strip():
                        continue
                    oai_identifier = oai_id_el.text.strip()

                    md_el = rec.find("oai:metadata", namespaces=ns)
                    if md_el is None:
                        continue

                    dc_el = md_el.find("oai_dc:dc", namespaces=ns)
                    if dc_el is None:
                        dc_el = md_el.find("dc:dc", namespaces=ns)
                    if dc_el is None:
                        continue

                    yield oai_identifier, dc_el

                # Resumption token para seguir con la siguiente página
                rt_el = root.find(".//oai:resumptionToken", namespaces=ns)
                if rt_el is not None and (rt_el.text or "").strip():
                    resumption_token = rt_el.text.strip()
                else:
                    break
