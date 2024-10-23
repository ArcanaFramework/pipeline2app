import typing as ty
from pathlib import Path
import random
import docker
import os
import logging
from copy import copy
from traceback import format_exc
from pipeline2app.core.image import App
from pipeline2app.core.image.components import Version
from pipeline2app.core.utils import DOCKER_HUB, GITHUB_CONTAINER_REGISTRY
import pytest

logger = logging.getLogger("pipeline2app")

VERSIONS = [
    "1.0-alpha0",
    "1.0-alpha2",
    "1.0-beta0",
    "1.0.0",
    "1.0-post1",
    "1.0-post2",
    "1.1-alpha0",
    "1.2.0",
    "1.11.0",
    "1.12.1-rc1",
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


REQUIRED_ENVVARS = ("GHCR_USERNAME", "GHCR_TOKEN", "DOCKER_USERNAME", "DOCKER_TOKEN")

REGISTRIES = [GITHUB_CONTAINER_REGISTRY, DOCKER_HUB, "localhost"]


@pytest.fixture(params=REGISTRIES)
def docker_registry(request: pytest.FixtureRequest, local_docker_registry: str) -> str:
    return request.param if request.param != "localhost" else local_docker_registry


@pytest.fixture
def image_tags(image_spec, docker_registry, tmp_path) -> ty.List[str]:

    registry_prefix = docker_registry.split(".")[0].upper()
    username = os.environ.get(f"{registry_prefix}_USERNAME")
    token = os.environ.get(f"{registry_prefix}_TOKEN")

    dc = docker.from_env()

    if username is not None and token is not None:
        response = dc.login(username=username, password=token, registry=docker_registry)
        if response["Status"] != "Login Succeeded":
            logger.warning("Could not login to '%s':\n\n%s", docker_registry, response)

    pushed = []

    for version in VERSIONS:
        build_dir = tmp_path / f"build-{version}"

        image_spec_cpy = copy(image_spec)

        image_spec_cpy["version"] = version
        if docker_registry == DOCKER_HUB:
            image_spec_cpy["org"] = "australianimagingservice"

        image = App(registry=docker_registry, **image_spec_cpy)

        try:
            dc.api.pull(image.reference)
        except docker.errors.APIError as e:
            if e.response.status_code in (404, 500):
                image.make(build_dir=build_dir)
                try:
                    dc.api.push(image.reference)
                except Exception:
                    pytest.skip(
                        f"Could not push '{image.reference}':\n\n{format_exc()}"
                    )
            else:
                raise
        pushed.append(image.tag)

    return sorted(pushed)


def test_sort_versions():

    rng = random.Random(42)

    shuffled = copy(VERSIONS)
    rng.shuffle(shuffled)

    sorted_versions = sorted(Version.parse(v) for v in shuffled)

    assert sorted_versions == [Version.parse(v) for v in VERSIONS]
    assert [str(v) for v in sorted_versions] == VERSIONS


UNKNOWN_VERSIONS = ["1.0.0-unknown0", "1.0.0-alpha0junk"]


@pytest.mark.parametrize("version", UNKNOWN_VERSIONS)
def test_bad_version(version):
    Version.parse(version).release = version


def test_registry_tags(
    image_tags: ty.List[str],
    tmp_path: Path,
    docker_registry: str,
    image_spec: ty.Dict[str, ty.Any],
) -> None:

    image_spec_cpy = copy(image_spec)
    if docker_registry == DOCKER_HUB:
        image_spec_cpy["org"] = "australianimagingservice"

    app = App(registry=docker_registry, **image_spec_cpy)
    assert sorted(app.registry_tags()) == sorted(image_tags)
