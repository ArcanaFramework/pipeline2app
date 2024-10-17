import typing as ty
from pathlib import Path
from pipeline2app.core.image import App
from frametree.core.frameset import FrameSet
from frametree.common import FileSystem, Samples


def get_pipeline_image(license_path: str, app_cls: ty.Type[App] = App) -> App:
    return app_cls(
        title="A test of the license installation",
        name="to_be_overridden",
        org=ORG,
        version="1.0",
        authors=[{"name": "Some One", "email": "some.one@an.email.org"}],
        docs={
            "info_url": "http://concatenate.readthefakedocs.io",
        },
        readme="This is a test README",
        packages={
            "pip": ["fileformats", "frametree"],
        },
        licenses={
            LICENSE_NAME: {
                "destination": license_path,
                "info_url": "http://license.test",
                "description": "This is a license to test the build structure",
            }
        },
        commands={
            "check-license": {
                "task": "pipeline2app.testing.tasks:check_license",
                "row_frequency": "common:Samples[sample]",
                "inputs": [
                    {
                        "name": LICENSE_INPUT_FIELD,
                        "datatype": "text/plain",
                        "field": "expected_license_contents",
                        "help": "the path to the license",
                    },
                ],
                "outputs": [
                    {
                        "name": LICENSE_OUTPUT_FIELD,
                        "datatype": "text/plain",
                        "field": "out",
                        "help": "the validated license path",
                    }
                ],
                "parameters": [
                    {
                        "name": LICENSE_PATH_PARAM,
                        "datatype": "field/text",
                        "field": "expected_license_path",
                        "required": True,
                        "help": "the expected contents of the license file",
                    }
                ],
            },
        },
    )


def make_dataset(dataset_dir: Path) -> FrameSet:

    contents_dir = dataset_dir / "sample1"
    contents_dir.mkdir(parents=True)

    with open(contents_dir / (LICENSE_INPUT_PATH + ".txt"), "w") as f:
        f.write(LICENSE_CONTENTS)

    dataset = FileSystem().define_frameset(dataset_dir, axes=Samples)
    dataset.save()
    return dataset


ORG = "pipeline2app-tests"
REGISTRY = "a.docker.registry.io"
IMAGE_VERSION = "1.0"


LICENSE_CONTENTS = "license contents"

LICENSE_NAME = "testlicense"

LICENSE_INPUT_FIELD = "license_file"

LICENSE_OUTPUT_FIELD = "validated_license_file"

LICENSE_PATH_PARAM = "license_path"

LICENSE_INPUT_PATH = "contents-file"

LICENSE_OUTPUT_PATH = "validated-file"
