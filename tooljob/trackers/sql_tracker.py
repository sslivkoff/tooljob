from __future__ import annotations

import time
import typing

import toolsql

from .. import spec
from . import tracker


class SqlTracker(tracker.Tracker):

    db_config: toolsql.DBConfig

    def __init__(
        self, db_config: toolsql.DBConfig, **kwargs: typing.Any
    ) -> None:
        self.db_config = db_config
        super().__init__(**kwargs)

    def is_job_complete(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> bool:
        return self.get_job_end_time(i=i, job_data=job_data) is not None

    def are_jobs_complete(
        self,
        indices: typing.Sequence[int] | None = None,
        *,
        job_datas: typing.Sequence[typing.Any] | None = None,
    ) -> typing.Sequence[bool]:
        end_times = self.get_jobs_end_times(indices, job_datas=job_datas)
        return [end_time is not None for end_time in end_times]

    #
    # # database-specific methods
    #

    @classmethod
    def get_db_schema(cls) -> toolsql.DBSchema:
        raw_schema: toolsql.TableSchemaShorthand = {
            'columns': [
                {'name': 'job_hash', 'type': 'TEXT', 'primary': True},
                {'name': 'name', 'type': 'TEXT'},
                {'name': 'job_data', 'type': 'JSON'},
                {'name': 'start_time', 'type': 'DATETIME'},
                {'name': 'end_time', 'type': 'DATETIME'},
            ],
        }
        jobs_table = toolsql.normalize_shorthand_table_schema(raw_schema)
        return {'name': 'tooljob', 'tables': {'jobs': jobs_table}}

    def create_db_tables(self) -> None:
        db_schema = self.get_db_schema()
        with toolsql.connect(self.db_config) as conn:
            for table in db_schema['tables'].values():
                toolsql.create_table(table=table, conn=conn)

    def start_job(self, i: int) -> None:
        job_data = self.batch.get_job_data(i)
        job_hash = self.batch.get_job_hash(job_data=job_data)
        row = {
            'job_data': job_data,
            'job_hash': job_hash,
            'start_time': time.time(),
            'end_time': None,
        }
        with toolsql.connect(self.db_config) as conn:
            toolsql.insert(table='jobs', row=row, conn=conn)

    def end_job(self, i: int) -> None:
        job_data = self.batch.get_job_data(i)
        job_hash = self.batch.get_job_hash(job_data=job_data)
        with toolsql.connect(self.db_config) as conn:
            toolsql.update(
                table='jobs',
                values={'end_time': time.time()},
                where_equals={'job_hash': job_hash},
                conn=conn,
            )

    def delete_job_records(
        self,
        indices: typing.Sequence[int] | None = None,
        *,
        job_datas: typing.Sequence[typing.Any] | None = None,
    ) -> None:
        job_hashes = self.batch.get_job_hashes(
            indices=indices, job_datas=job_datas
        )
        with toolsql.connect(self.db_config) as conn:
            toolsql.delete(
                where_in={'job_hash': [job_hashes]}, table='jobs', conn=conn
            )

    #
    # # time accessors
    #

    def get_job_start_time(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> int | float | None:
        job_hash = self.batch.get_job_hash(i=i, job_data=job_data)
        with toolsql.connect(self.db_config) as conn:
            return toolsql.select(
                where_equals={'job_hash': job_hash},
                table='jobs',
                conn=conn,
                columns=['start_time'],
                output_format='cell_or_none',
            )

    def get_job_end_time(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> int | float | None:
        job_hash = self.batch.get_job_hash(i=i, job_data=job_data)
        with toolsql.connect(self.db_config) as conn:
            return toolsql.select(
                where_equals={'job_hash': job_hash},
                table='jobs',
                conn=conn,
                columns=['end_time'],
                output_format='cell_or_none',
            )

    def get_jobs_start_times(
        self,
        indices: typing.Sequence[int] | None = None,
        *,
        job_datas: typing.Sequence[typing.Any] | None = None,
    ) -> typing.Sequence[int | float | None]:
        job_hashes = self.batch.get_job_hashes(
            indices=indices, job_datas=job_datas
        )
        with toolsql.connect(self.db_config) as conn:
            return toolsql.select(
                where_in={'job_hash': job_hashes},
                table='jobs',
                conn=conn,
                columns=['start_time'],
                output_format='single_column',
            )

    def get_jobs_end_times(
        self,
        indices: typing.Sequence[int] | None = None,
        *,
        job_datas: typing.Sequence[typing.Any] | None = None,
    ) -> typing.Sequence[int | float | None]:
        job_hashes = self.batch.get_job_hashes(
            indices=indices, job_datas=job_datas
        )
        with toolsql.connect(self.db_config) as conn:
            return toolsql.select(
                where_in={'job_hash': job_hashes},
                table='jobs',
                conn=conn,
                columns=['end_time'],
                output_format='single_column',
            )

