from __future__ import annotations

import typing

if typing.TYPE_CHECKING:

    import toolsql

    from .. import batch_class
    from .. import spec
    from . import tracker


def create_tracker(
    tracker: str | None = None,
    *,
    output_dir: str | None = None,
    output_filetype: str | None = None,
    outputs: spec.ShorthandOutputsSpec | None = None,
    db_config: toolsql.DBConfig | None = None,
    bucket_path: str | None = None,
    batch: batch_class.Batch | None = None,
) -> tracker.Tracker:

    # determine tracker
    if tracker is None:
        if output_dir is not None and output_filetype is not None:
            tracker = 'file'
        elif db_config is not None:
            tracker = 'sql'
        elif bucket_path is not None:
            tracker = 'bucket'
        else:
            raise Exception('invalid tracker: ' + str(tracker))

    if tracker == 'file':
        from . import file_tracker

        assert (
            output_dir is not None
        ), 'must specify output_dir for file tracker'
        assert (
            output_filetype is not None
        ), 'must specify output_dir for file tracker'

        return file_tracker.FileTracker(
            batch=batch,
            output_dir=output_dir,
            output_filetype=output_filetype,
        )

    elif tracker == 'multifile':
        from . import multifile_tracker

        assert outputs is not None, 'must specify outputs for MultifileTracker'

        return multifile_tracker.MultifileTracker(
            batch=batch,
            outputs=outputs,
            output_dir=output_dir,
            output_filetype=output_filetype,
        )

    elif tracker == 'sql':
        from . import sql_tracker

        assert db_config is not None, 'must specify db_config for sql tracker'

        return sql_tracker.SqlTracker(
            batch=batch,
            db_config=db_config,
        )

    elif tracker == 'bucket':
        from . import bucket_tracker

        raise NotImplementedError('bucket tracker')

        assert (
            bucket_path is not None
        ), 'must specify bucket_path for bucket tracker'

        return bucket_tracker.BucketTracker(
            batch=batch,
            bucket_path=bucket_path,
        )

    else:
        raise Exception('no tracker specified')

