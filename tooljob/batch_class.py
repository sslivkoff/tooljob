from __future__ import annotations

import concurrent
import hashlib
import json
import time
import typing

from typing_extensions import Literal

if typing.TYPE_CHECKING:
    import polars as pl

    import toolsql

import toolstr
import tooltime

from . import spec
from . import trackers


class Batch:

    name: str | None = None
    parameters: typing.Sequence[str]
    jobs: typing.Sequence[spec.JobData] | None = None

    #
    # # manadatory implementations
    #

    def execute_job(self, i: int) -> typing.Any:
        raise NotImplementedError()

    #
    # # __init__
    #

    def __init__(
        self,
        *,
        jobs: typing.Sequence[spec.JobData] | None = None,
        tracker: str | None = None,
        output_dir: str | None = None,
        output_filetype: str | None = None,
        db_config: toolsql.DBConfig | None = None,
        bucket_path: str | None = None,
        name: str | None = None,
    ) -> None:

        self.name = name
        self.jobs = jobs
        self.tracker = trackers.create_tracker(
            tracker=tracker,
            output_dir=output_dir,
            output_filetype=output_filetype,
            db_config=db_config,
            bucket_path=bucket_path,
            batch=self,
        )

    #
    # # names
    #

    def get_job_list_name(self) -> str:
        if self.name is not None:
            return self.name
        else:
            return type(self).__name__

    def get_job_name(
        self,
        i: int | None = None,
        *,
        job_data: spec.JobData | None = None,
    ) -> str:
        if job_data is None:
            if i is None:
                raise Exception('must specify job_data or i')
            job_data = self.get_job_data(i)
        simple_types = (str, int, bool)
        if isinstance(job_data, simple_types):
            return self.get_job_list_name() + str(job_data)
        elif isinstance(job_data, dict):
            for key, value in job_data.items():
                if not isinstance(value, simple_types):
                    raise NotImplementedError(
                        'must define job_name() for this type of job_data'
                    )
            tokens = [
                key + '_' + job_data[key]
                for key, value in sorted(job_data.items())
            ]
            return self.get_job_list_name() + '__'.join(tokens)
        else:
            raise NotImplementedError(
                'must define get_job_name() for this type of job_data'
            )

    def parse_job_name(self, name: str) -> spec.JobData:
        job_data = {}
        for pair in name.split('__'):
            key, value = pair.split('_')
            job_data[key] = self._parse_name_token(value)
        return job_data

    @staticmethod
    def _parse_name_token(token: str) -> bool | int | str:
        try:
            return bool(token)
        except Exception:
            try:
                return int(token)
            except Exception:
                return token

    #
    # # job data
    #

    def get_n_jobs(self) -> int:
        if hasattr(self, 'jobs') and self.jobs is not None:
            return len(self.jobs)
        else:
            raise NotImplementedError(
                'must specify jobs or implement get_n_jobs()'
            )

    def get_job_data(self, i: int) -> spec.JobData:
        if hasattr(self, 'jobs') and self.jobs is not None:
            return self.jobs[i]
        else:
            raise NotImplementedError(
                'must specify jobs or implement get_job_data()'
            )

    #
    # # job completion
    #

    def get_remaining_jobs(self) -> typing.Sequence[int]:
        jobs = range(self.get_n_jobs())
        return [
            j
            for j, complete in enumerate(self.are_jobs_complete(jobs))
            if not complete
        ]

    def are_jobs_complete(
        self, indices: typing.Sequence[int]
    ) -> typing.Sequence[bool]:
        return [self.tracker.is_job_complete(i) for i in indices]

    #
    # # hashes
    #

    def get_job_hash(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> str:
        if job_data is None:
            if i is None:
                raise Exception('must specify i or job_hash')
            job_data = self.get_job_data(i)
        job_data_str = json.dumps(job_data, sort_keys=True)
        job_hash = hashlib.md5(job_data_str.encode()).hexdigest()
        return job_hash

    def get_job_hashes(
        self,
        indices: typing.Sequence[int] | None = None,
        *,
        job_datas: typing.Sequence[typing.Any] | None = None,
    ) -> typing.Sequence[str]:
        if job_datas is None:
            if indices is None:
                indices = list(range(self.get_n_jobs()))
            job_datas = [self.get_job_data(i) for i in indices]
        return [self.get_job_hash(job_data) for job_data in job_datas]

    #
    # # execution
    #

    def orchestrate_jobs(
        self,
        executor: Literal['serial', 'parallel'] = 'parallel',
        n_processes: int | None = None,
    ) -> None:

        self.print_status()

        remaining_jobs = self.get_remaining_jobs()
        if len(remaining_jobs) == 0:
            print('\nAll jobs already completed')
            return

        start_time = time.time()
        print('\n\nRunning remaining jobs...\n')
        print('start time: ', tooltime.timestamp_to_iso_pretty(start_time))

        # execute jobs
        if executor == 'serial':
            self.serial_execute(jobs=remaining_jobs)
        elif executor == 'parallel':
            self.parallel_execute(jobs=remaining_jobs, n_processes=n_processes)
        else:
            raise Exception('unknown executor: ' + str(executor))

        # finalize
        self.print_conclusion(
            start_time=start_time,
            end_time=time.time(),
            jobs=remaining_jobs,
        )

    def serial_execute(self, jobs: typing.Sequence[int]) -> None:
        for job in jobs:
            self.run_job(job)

    def parallel_execute(
        self,
        jobs: typing.Sequence[int],
        n_processes: int | None = None,
    ) -> None:
        with concurrent.futures.ProcessPoolExecutor(n_processes) as executor:
            futures = [executor.submit(self.run_job, i=job) for job in jobs]
            concurrent.futures.wait(futures)

    def run_job(self, i: int) -> None:
        self.start_job(i=i)
        self.execute_job(i=i)
        self.end_job(i=i)

    def start_job(self, i: int) -> None:
        pass

    def end_job(self, i: int) -> None:
        pass

    #
    # # times
    #

    def get_job_start_time(
        self, i: int | None = None, *, job_data: typing.Any | None = None
    ) -> int | float | None:
        raise NotImplementedError()

    def get_job_end_time(
        self, i: int | None = None, *, job_data: typing.Any | None = None
    ) -> int | float | None:
        raise NotImplementedError()

    def get_jobs_start_times(
        self,
        indices: typing.Sequence[int] | None = None,
        *,
        job_datas: typing.Sequence[typing.Any] | None = None,
    ) -> typing.Sequence[int | float | None]:
        if indices is not None:
            return [self.get_job_start_time(i=i) for i in indices]
        elif job_datas is not None:
            return [
                self.get_job_start_time(job_data=job_data)
                for job_data in job_datas
            ]
        else:
            raise Exception('must specify indices or job_datas')

    def get_jobs_end_times(
        self,
        indices: typing.Sequence[int] | None = None,
        *,
        job_datas: typing.Sequence[typing.Any] | None = None,
    ) -> typing.Sequence[int | float | None]:
        if indices is not None:
            return [self.get_job_end_time(i=i) for i in indices]
        elif job_datas is not None:
            return [
                self.get_job_end_time(job_data=job_data)
                for job_data in job_datas
            ]
        else:
            raise Exception('must specify indices or job_datas')

    #
    # # summary
    #

    def print_status(self) -> None:
        toolstr.print_text_box(str(type(self).__name__) + ' Job Summary')
        print('- n_jobs:', self.get_n_jobs())
        print('- n_remaining:', len(self.get_remaining_jobs()))

    def print_summary(self) -> None:
        toolstr.print_header('Parameters')
        for parameter in self.parameters:
            toolstr.print_bullet(key=parameter, value=getattr(self, parameter))

    def print_conclusion(
        self,
        start_time: int | float,
        end_time: int | float,
        jobs: typing.Sequence[int],
        **kwargs: typing.Any,
    ) -> None:

        print('end time: ', tooltime.timestamp_to_iso_pretty(end_time))

        done_jobs = len([self.tracker.is_job_complete(i) for i in jobs])
        print(done_jobs, 'jobs completed')

        duration = end_time - start_time
        seconds_per_job = duration / done_jobs
        jobs_per_second = done_jobs / duration
        print()
        print('- duration:', toolstr.format(duration, decimals=3), 'seconds')
        print(
            '- seconds per job:',
            toolstr.format(seconds_per_job, decimals=3),
        )
        print(
            '- jobs per minute:',
            toolstr.format(jobs_per_second * 86400 / 24 / 60, decimals=2),
        )
        print(
            '- jobs per hour:',
            toolstr.format(jobs_per_second * 86400 / 24, decimals=2),
        )
        print(
            '- jobs per day:',
            toolstr.format(jobs_per_second * 86400, decimals=2),
        )

    def summarize_jobs_per_second(self, sample_time: int = 60) -> pl.DataFrame:

        import polars as pl

        names = [self.get_job_name(i) for i in range(self.get_n_jobs())]
        times = [self.get_job_end_time(i) for i in range(self.get_n_jobs())]
        df = pl.from_dict({'job': names, 'times': times})
        df = df.with_column(
            (pl.col('times') / sample_time).cast(int).alias('sample')
        )
        df = df.with_columns(
            [
                pl.col('job'),
                pl.count().over('sample').alias('jobs_per_second')
                / sample_time,
            ]
        )
        return df[['job', 'jobs_per_second']]

    def summarize_total_time(self) -> float | None:
        raw_times = self.get_jobs_end_times(list(range(self.get_n_jobs())))
        times = [float(time) for time in raw_times if time is not None]
        if len(times) > 0:
            return max(times) - min(times)
        else:
            return None

