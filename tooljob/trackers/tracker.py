from __future__ import annotations

import typing
from .. import batch_class
from .. import spec


class Tracker:
    def __init__(self, batch: batch_class.Batch, **kwargs: typing.Any):
        self.batch = batch

    def get_remaining_jobs(self) -> typing.Sequence[int]:
        raise NotImplementedError('get_remaining_jobs() not implemented')

    def is_job_complete(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> bool:
        raise NotImplementedError('is_job_complete() not implemented')

    def get_attribute_list(self) -> typing.Sequence[str]:
        return list(vars(self).keys())

    def get_formatted_attribute(self, key: str) -> str | None:
        return str(getattr(self, key))

