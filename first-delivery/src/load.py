"""Camada de persistência em MongoDB Atlas para o ETL do PNCP."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne


class Load:
    """Persiste os dados transformados no MongoDB Atlas."""

    def __init__(
        self,
        mongo_uri: str | None = None,
        database_name: str | None = None,
    ) -> None:
        """Inicializa o cliente MongoDB e o banco de dados de destino."""

        self.mongo_uri = mongo_uri or os.getenv("MONGODB_URI")
        if not self.mongo_uri:
            raise ValueError(
                "Define the MONGODB_URI environment variable with your Atlas connection string."
            )

        self.database_name = database_name or os.getenv("MONGODB_DATABASE", "pncp")
        self._client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=10000)
        self._client.admin.command("ping")
        self._database = self._client[self.database_name]
        self._ensure_indexes()

    @staticmethod
    def _utc_now() -> str:
        """Retorna a data e hora atual em UTC no formato ISO 8601."""

        return datetime.now(timezone.utc).isoformat()

    def _ensure_indexes(self) -> None:
        """Cria os índices usados para manter as cargas idempotentes."""

        self._database["orgaos"].create_index("source_id", unique=True)
        self._database["unidades"].create_index("source_id", unique=True)
        self._database["etl_runs"].create_index("started_at")

    def _upsert_many(
        self,
        collection_name: str,
        documents: list[dict[str, Any]],
    ) -> dict[str, int]:
        """Faz upsert em lote dos documentos na coleção informada."""

        valid_documents = [doc for doc in documents if doc.get("source_id")]
        if not valid_documents:
            return {"matched": 0, "modified": 0, "upserted": 0}

        collection = self._database[collection_name]
        operations = [
            UpdateOne(
                {"source_id": document["source_id"]},
                {"$set": document},
                upsert=True,
            )
            for document in valid_documents
        ]
        result = collection.bulk_write(operations, ordered=False)
        return {
            "matched": result.matched_count,
            "modified": result.modified_count,
            "upserted": len(result.upserted_ids),
        }

    def close(self) -> None:
        """Fecha a conexão com o MongoDB."""

        self._client.close()

    def save_orgaos(self, documents: list[dict[str, Any]]) -> dict[str, int]:
        """Persiste os registros de órgãos no MongoDB."""

        return self._upsert_many("orgaos", documents)

    def save_unidades(self, documents: list[dict[str, Any]]) -> dict[str, int]:
        """Persiste os registros de unidades no MongoDB."""

        return self._upsert_many("unidades", documents)

    def register_run(self, summary: dict[str, Any]) -> None:
        """Registra um resumo da execução do ETL para auditoria."""

        self._database["etl_runs"].insert_one(
            {
                **summary,
                "started_at": summary.get("started_at", self._utc_now()),
                "completed_at": self._utc_now(),
            }
        )
