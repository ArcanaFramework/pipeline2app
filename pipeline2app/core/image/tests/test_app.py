import os
import docker
from frametree.common import FileSystem, Samples
from pipeline2app.core.image import App, P2AImage
from pipeline2app.core import PACKAGE_NAME


def test_native_python_install(tmp_path):

    SAMPLE_INDEX = "1"
    OUTPUT_COL_NAME = "printed_version"

    dataset_dir = tmp_path / "dataset"
    sample_dir = dataset_dir / SAMPLE_INDEX
    sample_dir.mkdir(parents=True)
    sample_file = sample_dir / "sample.txt"
    sample_file.write_text("sample")

    dataset = FileSystem().define_frameset(dataset_dir, axes=Samples)
    dataset.save()

    test_spec = {
        "name": "native_python_test",
        "title": "a test image spec",
        "command": {
            "task": "common:shell",
            "inputs": {
                "dummy": {
                    "datatype": "text/text-file",
                    "help": "a dummy input that isn't actually used",
                    "configuration": {
                        "position": 0,
                    },
                },
            },
            "outputs": {
                OUTPUT_COL_NAME: {
                    "datatype": "field/text",
                    "help": "the print to stdout",
                    "configuration": {
                        "callable": "common:value_from_stdout",
                    },
                }
            },
            "parameters": {
                "duplicates": {
                    "field": "duplicates",
                    "default": 2,
                    "datatype": "field/integer",
                    "required": True,
                    "help": "a parameter",
                }
            },
            "row_frequency": "common:Samples[sample]",
            "configuration": {
                "executable": [
                    "pipeline2app",
                    "--version",
                ]
            },
        },
        "version": {"package": "1.0", "build": "1"},
        "packages": {
            "system": ["vim"],  # just to test it out
            "pip": {"pipeline2app": None, "frametree": None},  # just to test out the
        },
        "base_image": {
            "name": "python",
            "tag": "3.12.5-slim-bookworm",
            "python": "python3",
            "package_manager": "apt",
            "conda_env": None,
        },
        "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
        "docs": {
            "info_url": "http://concatenate.readthefakedocs.io",
        },
    }

    app = App.load(test_spec)

    app.make(build_dir=tmp_path / "build-dir", use_local_packages=True)

    volume_mount = str(dataset_dir) + ":/dataset:rw"
    args = [
        "/dataset",
        "--input",
        "dummy",
        "sample",
        "--output",
        OUTPUT_COL_NAME,
        OUTPUT_COL_NAME,
    ]

    dc = docker.from_env()
    try:
        dc.containers.run(
            app.reference,
            command=args,
            stderr=True,
            volumes=[volume_mount],
            user=f"{os.getuid()}:{os.getgid()}",
        )
    except docker.errors.ContainerError as e:
        raise RuntimeError(
            f"'docker run -v {volume_mount} {app.reference} {' '.join(args)}' errored:\n"
            + e.stderr.decode("utf-8")
        )

    dataset = FileSystem().load_frameset(dataset_dir)

    def strip_ver_timestamp(ver_str):
        parts = str(ver_str).split("+")
        try:
            parts[1] = parts[1].split(".")[0]
        except IndexError:
            pass
        return "+".join(parts).strip()

    assert str(dataset[OUTPUT_COL_NAME][SAMPLE_INDEX]).split(",")[0] == PACKAGE_NAME


def test_add_resources(tmp_path):

    img = P2AImage(
        name="test-resource-add-image",
        version={"package": "1.0", "build": "1"},
        packages={
            "system": ["vim"],  # just to test it out
            "pip": {
                "pipeline2app": None,
            },  # just to test out the
        },
        base_image={
            "name": "python",
            "tag": "3.12.5-slim-bookworm",
            "python": "python3",
            "package_manager": "apt",
            "conda_env": None,
        },
        resources={
            "a-resource": "/internal/path/to/a/resource.txt",
            "another-resource": "/internal/path/to/another/resource",
        },
    )

    foo_file = tmp_path / "resources" / "foo.txt"
    foo_file.parent.mkdir(parents=True)
    foo_file.write_text("foo")

    resources_dir = tmp_path / "resources"
    another_resource_sub_dir = resources_dir / "another-resource"
    another_resource_sub_dir.mkdir(parents=True)
    (another_resource_sub_dir / "bar.txt").write_text("bar")

    img.make(
        build_dir=tmp_path / "build-dir",
        use_local_packages=True,
        resources={
            "a-resource": foo_file,
        },
        resources_dir=resources_dir,
    )

    dc = docker.from_env()
    args = ["cat", "/internal/path/to/a/resource.txt"]
    try:
        result = dc.containers.run(
            img.reference,
            command=args,
            stderr=True,
        )
    except docker.errors.ContainerError as e:
        raise RuntimeError(
            f"'docker run {img.reference} {' '.join(args)}' errored:\n"
            + e.stderr.decode("utf-8")
        )

    assert result == b"foo"

    args = ["cat", "/internal/path/to/another/resource/bar.txt"]
    try:
        result = dc.containers.run(
            img.reference,
            command=args,
            stderr=True,
        )
    except docker.errors.ContainerError as e:
        raise RuntimeError(
            f"'docker run {img.reference} {' '.join(args)}' errored:\n"
            + e.stderr.decode("utf-8")
        )
    assert result == b"bar"
