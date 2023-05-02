from __future__ import annotations

import typing


if typing.TYPE_CHECKING:
    JobData = typing.Mapping[str, typing.Any]

    class ShorthandOutputSpec(typing.TypedDict, total=False):
        name: str
        output_dir: str
        output_filetype: str

    class OutputSpec(typing.TypedDict):
        output_dir: str
        output_filetype: str

    ShorthandOutputsSpec = typing.Union[
        typing.Mapping[str, OutputSpec],
        typing.Sequence[str],
    ]
    OutputsSpec = typing.Mapping[str, OutputSpec]

