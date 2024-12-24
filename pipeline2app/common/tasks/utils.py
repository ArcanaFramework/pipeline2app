import typing as ty
import json
from pydra.design import python
from pydra.engine.core import File
from pydra.engine.specs import BaseSpec, SpecInfo
from pydra.engine.task import FunctionTask
from fileformats.core import DataType, FileSet
from frametree.core.row import DataRow


def identity(**fields):
    return fields


def identity_task(task_name, fields):
    task = FunctionTask(
        identity,
        input_spec=SpecInfo(
            name=f"{task_name}Inputs",
            bases=(BaseSpec,),
            fields=[(s, DataType) for s in fields],
        ),
        output_spec=SpecInfo(
            name=f"{task_name}Outputs", bases=(BaseSpec,), fields=[("row", DataRow)]
        ),
    )
    return task


@python.define(outputs=["out_file"])
def identity_converter(in_file: FileSet) -> FileSet:
    return in_file


@python.define
def extract_from_json(in_file: File, field_name: str) -> ty.Any:
    with open(in_file) as f:
        dct = json.load(f)
    return dct[field_name]  # FIXME: Should use JSONpath syntax
