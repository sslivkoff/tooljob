from __future__ import annotations

import time
import typing

if typing.TYPE_CHECKING:
    from typing_extensions import Literal

    import polars as pl
    import toolcli
    import toolsql

from . import spec
from . import trackers


class Batch:
    name: str | None = None
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
        outputs: typing.Mapping[str, str] | None = None,
        db_config: toolsql.DBConfig | None = None,
        bucket_path: str | None = None,
        name: str | None = None,
        styles: toolcli.StyleTheme | None = None,
        verbose: bool = False,
    ) -> None:
        self.name = name
        self.jobs = jobs
        if styles is None:
            styles = {}
        self.styles = styles
        self.verbose = verbose
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
        parameters: typing.Mapping[str, str] | None = None,
    ) -> str:
        if job_data is None:
            if i is None:
                raise Exception('must specify job_data or i')
            job_data = self.get_job_data(i)

        if isinstance(job_data, (str, int, bool)):
            return self.get_job_list_name() + str(job_data)

        elif isinstance(job_data, dict):
            for key, value in job_data.items():
                if not isinstance(value, (str, int, bool)):
                    raise NotImplementedError(
                        'must define job_name() for this type of job_data'
                    )
            tokens = [
                key + '_' + job_data[key]
                for key, value in sorted(job_data.items())
            ]

            # add additional parameters to name
            if parameters is not None:
                for key, value in sorted(parameters.items()):
                    tokens.append(key + '_' + value)

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
        import hashlib
        import json

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
        import tooltime

        self.print_status()

        # check whether to circuit break
        remaining_jobs = self.get_remaining_jobs()
        if len(remaining_jobs) == 0:
            print('\nAll jobs already completed')
            return

        # print summary
        start_time = time.time()
        print()
        print()
        self.print_header('Running remaining jobs...')
        self.print_bullet(
            key='start time',
            value=tooltime.timestamp_to_iso_pretty(start_time),
            bullet_str='',
        )

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
        import tqdm

        color = self._get_progress_bar_color()
        for job in tqdm.tqdm(jobs, colour=color):
            self.run_job(job)

    def parallel_execute(
        self,
        jobs: typing.Sequence[int],
        n_processes: int | None = None,
    ) -> None:
        import concurrent.futures
        import tqdm

        color = self._get_progress_bar_color()
        with concurrent.futures.ProcessPoolExecutor(n_processes) as executor:
            futures = [executor.submit(self.run_job, i=job) for job in jobs]
            with tqdm.tqdm(total=len(jobs), colour=color) as pbar:
                for future in concurrent.futures.as_completed(futures):
                    pbar.update(1)

    def _get_progress_bar_color(self) -> str | None:
        if self.styles is None:
            return None
        else:
            return self.styles.get('content')

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

    def print_text_box(self, text: str) -> None:
        import toolstr

        toolstr.print_text_box(
            text,
            text_style=self.styles.get('metavar'),
            style=self.styles.get('content'),
        )

    def print_header(self, text: str) -> None:
        import toolstr

        toolstr.print_header(
            text,
            text_style=self.styles.get('metavar'),
            style=self.styles.get('content'),
        )

    def print_bullet(
        self, key: str, value: typing.Any, **kwargs: typing.Any
    ) -> None:
        import toolstr

        toolstr.print_bullet(key=key, value=value, styles=self.styles, **kwargs)

    def get_attribute_list(self) -> typing.Sequence[str]:
        attributes = list(vars(self).keys())
        if self.jobs is None:
            del attributes[attributes.index('jobs')]
        return attributes

    def get_formatted_attribute(self, key: str) -> str | None:
        return str(getattr(self, key))

    def print_status(self) -> None:
        import types
        import toolstr

        self.print_text_box('Collecting dataset ' + self.get_job_list_name())
        print()
        self.print_header('Parameters')
        self.print_bullet(key='n_jobs', value=self.get_n_jobs())
        toolstr.print_bullet(
            key='n_jobs_remaining',
            value=len(self.get_remaining_jobs()),
        )

        for obj, skip_keys in [
            (self, ['styles', 'tracker']),
            (self.tracker, ['batch']),
        ]:
            for parameter in obj.get_attribute_list():  # type: ignore
                if hasattr(obj, parameter):
                    value = getattr(obj, parameter)
                else:
                    value = None
                if (
                    not parameter.startswith('_')
                    and not isinstance(value, types.MethodType)
                    and parameter not in skip_keys
                ):
                    value_str = obj.get_formatted_attribute(parameter)  # type: ignore
                    if value_str is not None:
                        self.print_bullet(key=parameter, value=value_str)

        self.print_additional_status()

    def print_additional_status(self) -> None:
        pass

    def print_conclusion(
        self,
        start_time: int | float,
        end_time: int | float,
        jobs: typing.Sequence[int],
    ) -> None:
        import toolstr
        import tooltime

        self.print_bullet(
            key='end time',
            value='  ' + tooltime.timestamp_to_iso_pretty(end_time),
            bullet_str='',
        )

        done_jobs = len([self.tracker.is_job_complete(i) for i in jobs])
        print()
        print(done_jobs, 'jobs completed')
        print()
        print()
        self.print_header('Execution Summary')

        duration = end_time - start_time
        seconds_per_job = duration / done_jobs
        jobs_per_second = done_jobs / duration
        jobs_per_minute = jobs_per_second * 60
        jobs_per_day = jobs_per_second * 86400
        self.print_bullet(
            'duration',
            toolstr.format(duration, decimals=3) + ' seconds',
        )
        self.print_bullet(
            'seconds per job',
            toolstr.format(seconds_per_job, decimals=3),
        )
        self.print_bullet(
            'jobs per minute', toolstr.format(jobs_per_minute, decimals=2),
        )
        self.print_bullet(
            'jobs per hour',
            toolstr.format(jobs_per_second * 86400 / 24, decimals=2),
        )
        self.print_bullet(
            'jobs per day', toolstr.format(jobs_per_day, decimals=2),
        )
        self.print_additional_conclusion(
            start_time=start_time, end_time=end_time, jobs=jobs
        )

    def print_additional_conclusion(
        self,
        start_time: int | float,
        end_time: int | float,
        jobs: typing.Sequence[int],
    ) -> None:
        pass

    def summarize_jobs_per_second(self, sample_time: int = 60) -> pl.DataFrame:
        import polars as pl

        names = [self.get_job_name(i) for i in range(self.get_n_jobs())]
        times = [self.get_job_end_time(i) for i in range(self.get_n_jobs())]
        df = pl.from_dict({'job': names, 'times': times})
        df = df.with_columns(
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

