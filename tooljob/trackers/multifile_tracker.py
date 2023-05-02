from __future__ import annotations

import os
import typing

from .. import spec
from . import tracker


def _get_output_dict(
    output_dir: str | None = None,
    output_filetype: str | None = None,
    default_output_dir: str | None = None,
    default_output_filetype: str | None = None,
) -> spec.OutputSpec:
    if output_dir is None:
        if default_output_dir is None:
            raise Exception('must specify output_dir for each output')
        else:
            output_dir = default_output_dir

    if output_filetype is None:
        if default_output_filetype is None:
            raise Exception('must specify output_filetype for each output')
        else:
            output_filetype = default_output_filetype

    return {
        'output_dir': output_dir,
        'output_filetype': output_filetype,
    }


class MultifileTracker(tracker.Tracker):
    """

    every job has the same number of
    """

    outputs: spec.OutputsSpec

    def __init__(
        self,
        outputs: spec.ShorthandOutputsSpec,
        output_dir: str | None,
        output_filetype: str | None,
        **kwargs: typing.Any,
    ) -> None:
        # compile output specifications
        strict_outputs: typing.MutableMapping[str, spec.OutputSpec] = {}
        if isinstance(outputs, list):
            for output in outputs:
                if isinstance(output, str):
                    strict_outputs[output] = _get_output_dict(
                        default_output_dir=output_dir,
                        default_output_filetype=output_filetype,
                    )
                elif isinstance(output, dict):
                    name = output.get('name')
                    if name is None:
                        raise Exception('must specify "name" in each output')
                    strict_outputs[name] = _get_output_dict(
                        default_output_dir=output_dir,
                        default_output_filetype=output_filetype,
                        **output,
                    )
                else:
                    raise Exception()
        elif isinstance(outputs, dict):
            for name, output in outputs.items():
                if output.get('name') is not None and name != output.get('name'):
                    raise Exception('names do not match in outputs spec')
                strict_outputs[name] = _get_output_dict(
                    default_output_dir=output_dir,
                    default_output_filetype=output_filetype,
                    **output,
                )
        else:
            raise Exception('invalid format for outputs')
        self.outputs = strict_outputs

        # create missing directories
        for output in self.outputs.values():
            output_output_dir = output['output_dir']
            if not os.path.isdir(output_output_dir):
                print('output_dir does not exist, creating now')
                os.makedirs(output_output_dir)

        # create base tracker
        super().__init__(**kwargs)

    #
    # # interface methods
    #

    def is_job_complete(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> bool:
        return all(
            os.path.exists(path)
            for path in self.get_job_output_paths(i=i, job_data=job_data).values()
        )

    #
    # # filesystem-specific methods
    #

    def get_job_output_filenames(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> typing.Mapping[str, str]:
        output_filenames = {}
        for output_name, output in self.outputs.items():
            job_name = self.batch.get_job_name(
                i=i,
                job_data=job_data,
                parameters={'output_name': output_name},
            )
            output_filenames[output_name] = (
                job_name + '.' + output['output_filetype']
            )
        return output_filenames

    def get_job_output_paths(
        self, i: int | None = None, *, job_data: spec.JobData | None = None
    ) -> typing.Mapping[str, str]:
        filenames = self.get_job_output_filenames(i=i, job_data=job_data)
        return {
            output_name: os.path.join(
                self.outputs[output_name]['output_dir'], filename
            )
            for output_name, filename in filenames.items()
        }

    def parse_job_output_path(self, path: str) -> typing.Any:
        job_name, ext = os.path.splitext(os.path.basename(path))
        return self.batch.parse_job_name(job_name)

    #
    # # sumary methods
    #

    def get_job_start_time(self, i: int) -> int | float | None:
        paths = self.get_job_output_paths(i)
        times = [
            os.path.getctime(path)
            for path in paths.values()
            if os.path.isfile(path)
        ]
        return min(times)

    def get_job_end_time(self, i: int) -> int | float | None:
        paths = self.get_job_output_paths(i)
        times = [
            os.path.getmtime(path)
            for path in paths.values()
            if os.path.isfile(path)
        ]
        return max(times)

    def print_status(self) -> None:
        import toolstr

        total_size = 0
        for i in range(self.batch.get_n_jobs()):
            paths = self.get_job_output_paths(i)
            for path in paths.values():
                if os.path.isfile(path):
                    total_size = os.path.getsize(path)
        print('- output_dir size:', toolstr.format_nbytes(total_size))

