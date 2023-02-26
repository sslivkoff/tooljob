from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import toolsql

from . import tracker


def create_tracker(
    tracker: str | None = None,
    output_dir: str | None = None,
    output_filetype: str | None = None,
    db_config: toolsql.DBConfig | None = None,
    bucket_path: str | None = None,
) -> tracker.Tracker:

    if tracker is None:
        pass

    if tracker == 'file':
        from . import file_tracker

        assert (
            output_dir is not None
        ), 'must specify output_dir for file tracker'
        assert (
            output_filetype is not None
        ), 'must specify output_dir for file tracker'

        return file_tracker.FileTracker(
            output_dir=output_dir,
            output_filetype=output_filetype,
        )

    elif tracker == 'sql':
        from . import sql_tracker

        assert db_config is not None, 'must specify db_config for sql tracker'

        return sql_tracker.SqlTracker(
            db_config=db_config,
        )

    elif tracker == 'bucket':
        from . import bucket_tracker

        assert (
            bucket_path is not None
        ), 'must specify bucket_path for bucket tracker'

        return bucket_tracker.BucketTracker(
            bucket_path=bucket_path,
        )

    else:
        raise Exception('no tracker specified')

