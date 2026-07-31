from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True, slots=True)
class ParquetFileMetadata:
    path: Path
    compression: str
    rows: int


class ParquetEventStore:
    def __init__(self, base_path: str | Path) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write_rows(self, table: str, rows: list[dict[str, Any]]) -> list[Path]:
        if not table.replace("_", "").isalnum():
            raise ValueError("invalid table name")
        written: list[Path] = []
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            event_time = _event_time(row)
            symbol = str(row.get("symbol", "UNKNOWN")).upper()
            key = (event_time.date().isoformat(), symbol)
            normalized = dict(row)
            normalized["event_time"] = event_time
            grouped.setdefault(key, []).append(normalized)
        for (event_date, symbol), partition_rows in grouped.items():
            partition = self.base_path / table / f"event_date={event_date}" / f"symbol={symbol}"
            partition.mkdir(parents=True, exist_ok=True)
            filename = f"part-{len(list(partition.glob('*.parquet'))):06d}.parquet"
            path = partition / filename
            pq.write_table(pa.Table.from_pylist(partition_rows), path, compression="ZSTD")
            written.append(path)
        return written

    def query(self, sql: str) -> list[dict[str, Any]]:
        con = duckdb.connect(database=":memory:")
        try:
            for table_path in self.base_path.iterdir() if self.base_path.exists() else []:
                if table_path.is_dir():
                    view_name = table_path.name
                    glob = str(table_path / "**" / "*.parquet")
                    escaped_glob = glob.replace("'", "''")
                    con.execute(
                        f"CREATE OR REPLACE VIEW {view_name} AS "
                        f"SELECT * FROM read_parquet('{escaped_glob}', hive_partitioning=true)"
                    )
            columns = con.execute(sql).description
            result = con.fetchall()
        finally:
            con.close()
        names = [column[0] for column in columns]
        return [dict(zip(names, row, strict=True)) for row in result]

    def file_metadata(self, path: str | Path) -> ParquetFileMetadata:
        parquet_file = pq.ParquetFile(path)
        compression = parquet_file.metadata.row_group(0).column(0).compression
        return ParquetFileMetadata(Path(path), str(compression), parquet_file.metadata.num_rows)


def _event_time(row: dict[str, Any]) -> datetime:
    value = row.get("event_time") or row.get("timestamp") or datetime.now(tz=UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
