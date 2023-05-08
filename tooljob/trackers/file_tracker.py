from __future__ import annotations

import os
import typing

from .. import spec
from . import tracker


class FileTracker(tracker.Tracker):

    output_dir: str
    output_filetype: str

    def __init__(
        self,
        output_dir: str,
        output_filetype: str,
        **kwargs: typing.Any,
    ) -> None:
        self.output_dir = os.path.abspath(os.path.expanduser(output_dir))
        self.output_filetype = output_filetype
        if not os.path.isdir(output_dir):
            print('output_dir does not exist, creating now')
            os.makedirs(output_dir)
        super().__init__(**kwargs)

    #
    # # interface methods
    #

    def get_remaining_jobs(self) -> typing.Sequence[int]:
        return [
            i
            for i in range(self.batch.get_n_jobs())
            if self.get_job_output_filename(i=i) in os.listdir(self.output_dir)
        ]

    def is_job_complete(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> bool:
        return os.path.exists(self.get_job_output_path(i=i, job_data=job_data))

    #
    # # filesystem-specific methods
    #

    def get_job_output_filename(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> str:
        job_name = self.batch.get_job_name(i=i, job_data=job_data)
        return job_name + '.' + self.output_filetype

    def get_job_output_path(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> str:
        filename = self.get_job_output_filename(i=i, job_data=job_data)
        return os.path.join(self.output_dir, filename)

    def parse_job_output_path(self, path: str) -> typing.Any:
        job_name, ext = os.path.splitext(os.path.basename(path))
        return self.batch.parse_job_name(job_name)

    #
    # # sumary methods
    #

    def get_job_start_time(self, i: int) -> int | float | None:
        path = self.get_job_output_path(i)
        if os.path.isfile(path):
            return os.path.getctime(path)
        else:
            return None

    def get_job_end_time(self, i: int) -> int | float | None:
        path = self.get_job_output_path(i)
        if os.path.isfile(path):
            return os.path.getmtime(path)
        else:
            return None

    def print_status(self) -> None:
        import toolstr

        total_size = 0
        for i in range(self.batch.get_n_jobs()):
            path = self.get_job_output_path(i)
            if os.path.isfile(path):
                total_size = os.path.getsize(path)
        print('- output_dir size:', toolstr.format_nbytes(total_size))

