"""Ponto de entrada da orquestração do ETL do projeto PNCP."""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Any

try:
    from . import Extract, Load
except ImportError:
    from __init__ import Extract, Load


def _setup_logging() -> None:
    """Configura logging padrão para execução do ETL."""

    log_level = os.getenv("ETL_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


LOGGER = logging.getLogger("pncp_etl")


class ETLPipeline:
    """Coordena as etapas de extração, transformação e carga."""

    def __init__(
        self,
        search_term: str | None = None,
        page: int | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        mongo_uri: str | None = None,
    ) -> None:
        """Monta o pipeline usando valores padrão do ambiente, quando houver."""

        self.search_term = search_term or os.getenv("PNCP_SEARCH_TERM", "Prefeitura")
        self.page = page if page is not None else int(os.getenv("PNCP_PAGE", "1"))
        self.page_size = (
            page_size
            if page_size is not None
            else int(os.getenv("PNCP_PAGE_SIZE", "10"))
        )
        self.max_pages = (
            max_pages
            if max_pages is not None
            else int(os.getenv("PNCP_MAX_PAGES", "1"))
        )
        self.extractor = Extract()
        self.dry_run = False
        try:
            self.loader = Load(mongo_uri=mongo_uri)
        except ValueError:
            self.loader = None
            self.dry_run = True
            LOGGER.warning(
                "MONGODB_URI não configurada. Executando em modo dry-run (sem persistência)."
            )

    @staticmethod
    def _first_non_empty(*values: Any) -> Any:
        """Retorna o primeiro valor que não estiver vazio."""

        for value in values:
            if value not in (None, "", [], {}, ()):
                return value
        return None

    def _transform_orgao(self, orgao: dict[str, Any], source: str) -> dict[str, Any]:
        """Normaliza o documento do órgão antes da carga."""

        source_id = self._first_non_empty(orgao.get("cnpj"), orgao.get("id"))
        return {
            "source_id": str(source_id),
            "source": source,
            "cnpj": orgao.get("cnpj"),
            "razao_social": orgao.get("razaoSocial"),
            "nome_fantasia": orgao.get("nomeFantasia"),
            "cnpj_ente_responsavel": orgao.get("cnpjEnteResponsavel"),
            "esfera_id": orgao.get("esferaId"),
            "poder_id": orgao.get("poderId"),
            "codigo_natureza_juridica": orgao.get("codigoNaturezaJuridica"),
            "situacao_cadastral": orgao.get("situacaoCadastral"),
            "status_ativo": orgao.get("statusAtivo"),
            "data_inclusao": orgao.get("dataInclusao"),
            "data_atualizacao": orgao.get("dataAtualizacao"),
            "data_validacao": orgao.get("dataValidacao"),
            "raw_payload": orgao,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    def _transform_unidade(
        self,
        unidade: dict[str, Any],
        orgao_cnpj: str,
        orgao_razao_social: str | None,
    ) -> dict[str, Any]:
        """Normaliza o documento da unidade antes da carga."""

        source_id = self._first_non_empty(
            unidade.get("id"),
            f"{orgao_cnpj}:{unidade.get('codigoUnidade')}",
        )
        municipio = unidade.get("municipio") or {}
        orgao = unidade.get("orgao") or {}
        uf = municipio.get("uf") or {}

        return {
            "source_id": str(source_id),
            "orgao_cnpj": orgao.get("cnpj") or orgao_cnpj,
            "orgao_razao_social": orgao.get("razaoSocial") or orgao_razao_social,
            "codigo_unidade": unidade.get("codigoUnidade"),
            "nome_unidade": unidade.get("nomeUnidade"),
            "municipio_nome": municipio.get("nome"),
            "municipio_codigo_ibge": municipio.get("codigoIbge"),
            "uf_sigla": uf.get("siglaUF"),
            "uf_nome": uf.get("nomeUF"),
            "status_ativo": unidade.get("statusAtivo"),
            "data_inclusao": unidade.get("dataInclusao"),
            "data_atualizacao": unidade.get("dataAtualizacao"),
            "raw_payload": unidade,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    def run(self) -> dict[str, Any]:
        """Executa o pipeline completo e retorna um resumo da execução."""

        started_at = datetime.now(timezone.utc).isoformat()
        LOGGER.info(
            "Iniciando ETL PNCP | termo=%s pagina_inicial=%s tamanho_pagina=%s max_paginas=%s",
            self.search_term,
            self.page,
            self.page_size,
            self.max_pages,
        )

        orgaos_raw: list[dict[str, Any]] = []
        pagina = max(1, self.page)
        limite = max(1, self.max_pages)
        for _ in range(limite):
            lote = self.extractor.search_orgaos(
                razao_social=self.search_term,
                pagina=pagina,
                tamanho_pagina=self.page_size,
            )
            if not lote:
                break
            orgaos_raw.extend(lote)
            pagina += 1

        orgaos_processed: list[dict[str, Any]] = []
        unidades_processed: list[dict[str, Any]] = []
        erros: list[dict[str, str]] = []

        for orgao_summary in orgaos_raw:
            cnpj = orgao_summary.get("cnpj")
            detail = orgao_summary

            try:
                if cnpj:
                    detail = self.extractor.get_orgao_by_cnpj(cnpj) or orgao_summary
                    unidades_raw = self.extractor.get_unidades(cnpj)
                else:
                    unidades_raw = []

                orgao_processed = self._transform_orgao(
                    detail,
                    source="/v1/orgaos/{cnpj}",
                )
                orgaos_processed.append(orgao_processed)

                for unidade in unidades_raw:
                    unidades_processed.append(
                        self._transform_unidade(
                            unidade,
                            orgao_cnpj=orgao_processed["cnpj"]
                            or str(orgao_processed["source_id"]),
                            orgao_razao_social=orgao_processed["razao_social"],
                        )
                    )
            except Exception as exc:  # pragma: no cover - proteção de execução
                erros.append(
                    {
                        "cnpj": str(cnpj) if cnpj else "sem-cnpj",
                        "erro": str(exc),
                    }
                )
                LOGGER.warning("Falha ao processar orgao %s: %s", cnpj, exc)
                continue

        if self.loader is not None:
            orgaos_result = self.loader.save_orgaos(orgaos_processed)
            unidades_result = self.loader.save_unidades(unidades_processed)
        else:
            orgaos_result = {"matched": 0, "modified": 0, "upserted": 0}
            unidades_result = {"matched": 0, "modified": 0, "upserted": 0}

        summary = {
            "search_term": self.search_term,
            "orgaos_extracted": len(orgaos_raw),
            "orgaos_transformados": len(orgaos_processed),
            "orgaos_loaded": orgaos_result,
            "unidades_extracted": len(unidades_processed),
            "unidades_loaded": unidades_result,
            "orgaos_com_erro": len(erros),
            "erros": erros[:20],
            "dry_run": self.dry_run,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        if self.loader is not None:
            self.loader.register_run(summary)
            self.loader.close()

        LOGGER.info(
            "ETL finalizado | orgaos=%s unidades=%s erros=%s",
            len(orgaos_processed),
            len(unidades_processed),
            len(erros),
        )
        return summary


def main() -> None:
    """Executa o pipeline ETL e imprime o resumo da execução."""

    _setup_logging()
    pipeline = ETLPipeline()
    summary = pipeline.run()
    print(summary)


if __name__ == "__main__":
    main()
