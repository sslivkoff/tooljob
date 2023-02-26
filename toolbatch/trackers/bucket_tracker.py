from __future__ import annotations

import typing

from . import tracker


class BucketTracker(tracker.Tracker):

    def __init__(self, **kwargs: typing.Any):
        raise NotImplementedError()

