# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A DuckDB implementation of the cache."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from overrides import overrides

from airbyte_lib.caches.base import SQLCacheBase, SQLCacheConfigBase
from airbyte_lib.file_writers import ParquetWriter, ParquetWriterConfig


class DuckDBCacheConfig(SQLCacheConfigBase, ParquetWriterConfig):
    """Configuration for the DuckDB cache.

    Also inherits config from the ParquetWriter, which is responsible for writing files to disk.
    """

    db_path: str
    schema_name: str = "main"

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        # return f"duckdb:///{self.db_path}?schema={self.schema_name}"
        return f"duckdb:///{self.db_path}"

    def get_database_name(self) -> str:
        """Return the name of the database."""
        if self.db_path == ":memory:":
            return "memory"

        # Return the file name without the extension
        return self.db_path.split("/")[-1].split(".")[0]


class DuckDBCacheBase(SQLCacheBase):
    """A DuckDB implementation of the cache.

    Parquet is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.
    """

    config_class = DuckDBCacheConfig
    supports_merge_insert = True

    @overrides
    def _setup(self) -> None:
        """Create the database parent folder if it doesn't yet exist."""
        config = cast(DuckDBCacheConfig, self.config)

        if config.db_path == ":memory:":
            return

        Path(config.db_path).parent.mkdir(parents=True, exist_ok=True)


class DuckDBCache(DuckDBCacheBase):
    """A DuckDB implementation of the cache.

    Parquet is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.
    """

    file_writer_class = ParquetWriter

    @overrides
    def _merge_temp_table_to_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Merge the temp table into the main one.

        This implementation requires MERGE support in the SQL DB.
        Databases that do not support this syntax can override this method.
        """
        if not self._get_primary_keys(stream_name):
            raise RuntimeError(  # noqa: TRY003  # Too-long exception message
                f"Primary keys not found for stream {stream_name}. "
                "Cannot run merge updates without primary keys."
            )

        _ = stream_name
        final_table = self._fully_qualified(final_table_name)
        staging_table = self._fully_qualified(temp_table_name)
        self._execute_sql(
            # https://duckdb.org/docs/sql/statements/insert.html
            # NOTE: This depends on primary keys being set properly in the final table.
            f"""
            INSERT OR REPLACE INTO {final_table} BY NAME
            (SELECT * FROM {staging_table})
            """
        )

    @overrides
    def _ensure_compatible_table_schema(
        self,
        stream_name: str,
        table_name: str,
        raise_on_error: bool = True,
    ) -> bool:
        """Return true if the given table is compatible with the stream's schema.

        In addition to the base implementation, this also checks primary keys.
        """
        # call super
        if not super()._ensure_compatible_table_schema(stream_name, table_name, raise_on_error):
            return False

        pk_cols = self._get_primary_keys(stream_name)
        table = self.get_sql_table(table_name)
        table_pk_cols = table.primary_key.columns.keys()
        if set(pk_cols) != set(table_pk_cols):
            if raise_on_error:
                raise RuntimeError(  # noqa: TRY003  # Too-long exception message
                    f"Primary keys do not match for table {table_name}. "
                    f"Expected: {pk_cols}. "
                    f"Found: {table_pk_cols}.",
                )
            return False

        return True
