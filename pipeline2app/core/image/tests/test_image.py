import typing as ty
from pathlib import Path
import random
import docker
import os
import logging
from copy import copy
from traceback import format_exc
from pipeline2app.core.image import App
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

    shuffled = copy(VERSIONS)
    rng.shuffle(shuffled)

    sorted_versions = sorted(Version.parse(v) for v in shuffled)

    assert sorted_versions == [Version.parse(v) for v in VERSIONS]
    assert [str(v) for v in sorted_versions] == VERSIONS


REQUIRED_ENVVARS = ("GHCR_USERNAME", "GHCR_TOKEN", "DOCKER_USERNAME", "DOCKER_TOKEN")


@pytest.mark.skipIf(
    any(e not in os.environ for e in REQUIRED_ENVVARS),
    reason=f"Not all required environment variables are set ({REQUIRED_ENVVARS})",
)
@pytest.mark.parametrize("registry", [GITHUB_CONTAINER_REGISTRY, DOCKER_HUB])
def test_registry_tags(tmp_path: Path, registry: str, image_spec: ty.Dict[str, ty.Any]):

    registry_prefix = registry.split(".")[0].upper()
    username = os.environ.get(f"{registry_prefix}_USERNAME")
    token = os.environ.get(f"{registry_prefix}_TOKEN")

    dc = docker.from_env()
    response = dc.login(username=username, password=token, registry=registry)
    if response.status_code != 200:
        response.raise_for_status()

    pushed = []

    for version in VERSIONS:
        build_dir = tmp_path / f"build-{version}"

        image_spec_cpy = copy(image_spec)

        image_spec["version"] = version
        if registry == DOCKER_HUB:
            image_spec["org"] = "australianimagingservice"

        image = App(registry=registry, **image_spec_cpy)

        try:
            dc.api.pull(image.reference)
        except docker.errors.APIError as e:
            if e.response.status_code in (404, 500):
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

    app = App(registry=registry, **image_spec)
    assert sorted(app.registry_tags()) == sorted(pushed)
