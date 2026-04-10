"""Camada de extração da API PNCP."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class Extract:
    """Obtém dados da API pública do PNCP."""

    @staticmethod
    def _normalize_base_url(value: str) -> str:
        """Normaliza a base URL removendo sufixos de documentação do Swagger."""

        normalized = value.strip().strip('"').strip("'").rstrip("/")
        doc_suffixes = (
            "/swagger-ui/index.html",
            "/swagger-ui",
            "/v3/api-docs",
            "/api-docs",
        )
        for suffix in doc_suffixes:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)]
                break
        return normalized

    def __init__(
        self,
        base_url: str | None = None,
        timeout: int = 30,
    ) -> None:
        """Inicializa o cliente HTTP usado nas requisições ao PNCP."""

        raw_base_url = base_url or os.getenv(
            "PNCP_BASE_URL", "https://pncp.gov.br/api/pncp"
        )
        self.base_url = self._normalize_base_url(raw_base_url)
        self.timeout = timeout

    def _request_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        """Executa uma requisição GET e retorna o JSON decodificado."""

        query_string = f"?{urlencode(params)}" if params else ""
        url = f"{self.base_url}{path}{query_string}"
        request = Request(url, headers={"Accept": "application/json"})

        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            if allow_not_found and exc.code == 404:
                return []
            raise RuntimeError(
                f"Falha HTTP ao consultar PNCP ({exc.code}) em {path}."
            ) from exc
        except URLError as exc:
            raise RuntimeError(
                f"Falha de conexão ao consultar PNCP em {path}."
            ) from exc

        if not payload:
            return []

        return json.loads(payload)

    @staticmethod
    def _coerce_list(payload: Any) -> list[dict[str, Any]]:
        """Retorna uma lista de dicionários a partir de diferentes formatos de resposta."""

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            for key in ("content", "items", "data", "results", "dados"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]

        return []

    def search_orgaos(
        self,
        razao_social: str,
        pagina: int = 1,
        tamanho_pagina: int = 10,
    ) -> list[dict[str, Any]]:
        """Pesquisa órgãos pelo nome razão social e retorna a lista de resultados."""

        payload = self._request_json(
            "/v1/orgaos/",
            params={
                "razaoSocial": razao_social,
                "pagina": pagina,
                "tamanhoPagina": tamanho_pagina,
            },
        )
        return self._coerce_list(payload)

    def get_orgao_by_cnpj(self, cnpj: str) -> dict[str, Any]:
        """Retorna os detalhes de um órgão a partir de um CNPJ."""

        payload = self._request_json(f"/v1/orgaos/{cnpj}", allow_not_found=True)
        return payload if isinstance(payload, dict) else {}

    def get_unidades(self, cnpj: str) -> list[dict[str, Any]]:
        """Retorna as unidades vinculadas a um órgão informado."""

        payload = self._request_json(
            f"/v1/orgaos/{cnpj}/unidades",
            allow_not_found=True,
        )
        return self._coerce_list(payload)
