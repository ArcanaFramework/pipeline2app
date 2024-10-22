import typing as ty
from pathlib import Path
import random
import docker
import logging
from traceback import format_exc
from pipeline2app.core.image import P2AImage
from pipeline2app.core.version import Version
from pipeline2app.core.utils import DOCKER_HUB, GITHUB_CONTAINER_REGISTRY
import pytest

logger = logging.getLogger("pipeline2app")

VERSIONS = [
    "1.0-alpha0",
    "1.0-alpha2",
    "1.0-beta0",
    "1.0-post1",
    "1.0-post2",
    "1.1-alpha",
]


@pytest.fixture
def image_spec(command_spec) -> ty.Dict[str, ty.Any]:
    return {
        "org": "australian-imaging-service",
        "name": "test-pipeline",
        "version": "1.0.0",
        "title": "A pipeline to test Pipeline2app's deployment tool",
        "commands": {"concatenate-test": command_spec},
        "authors": [{"name": "Thomas G. Close", "email": "thomas.close@sydney.edu.au"}],
        "docs": {
            "info_url": "http://concatenate.readthefakedocs.io",
        },
        "readme": "This is a test pipeline",
        "packages": {
            "system": ["vim"],
        },
    }


def test_sort_versions():

    rng = random.Random(42)
    shuffled = rng.shuffle(VERSIONS)

    sorted_versions = sorted(Version.parse(v) for v in shuffled)

    assert sorted_versions == [Version.parse(v) for v in VERSIONS]
    assert [str(v) for v in sorted_versions] == VERSIONS


@pytest.mark.parametrize("registry", [GITHUB_CONTAINER_REGISTRY, DOCKER_HUB])
def test_registry_tags(tmp_path: Path, registry: str, image_spec: ty.Dict[str, ty.Any]):

    dc = docker.from_env()

    pushed = []

    for version in VERSIONS:
        build_dir = tmp_path / f"build-{version}"

        image_spec["version"] = version

        image = P2AImage(registry=registry, **image_spec)

        try:
            dc.api.pull(image.reference)
        except docker.errors.APIError as e:
            if e.response.status_code == 404:
                image.make(build_dir=build_dir)
                try:
                    dc.api.push(image.reference)
                except Exception as e:
                    raise RuntimeError(
                        f"Could not push '{image.reference}':\n\n{format_exc()}"
                    ) from e
            else:
                raise
        pushed.append(image)
