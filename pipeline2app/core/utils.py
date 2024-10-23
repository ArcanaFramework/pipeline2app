from __future__ import annotations
import typing as ty
import logging
from pathlib import Path, PosixPath
import tempfile
import tarfile
import docker

logger = logging.getLogger("pipeline2app")


def extract_file_from_docker_image(
    image_tag: str, file_path: PosixPath, out_path: ty.Optional[Path] = None
) -> Path:
    """Extracts a file from a Docker image onto the local host

    Parameters
    ----------
    image_tag : str
        the name/tag of the image to extract the file from
    file_path : PosixPath
        the path to the file inside the image

    Returns
    -------
    Path or None
        path to the extracted file or None if image doesn't exist
    """
    tmp_dir = Path(tempfile.mkdtemp())
    if out_path is None:
        out_path = tmp_dir / "extracted-dir"
    dc = docker.from_env()

    def get_image(tag: str) -> ty.Any:
        for img in dc.images.list():
            if tag in img.tags:
                return img
        return None

    img = get_image(image_tag)

    if img is None:
        try:
            dc.api.pull(image_tag)
        except docker.errors.APIError as e:
            if e.response.status_code == 500:
                logger.warning(
                    "Could not pull %s image from registry, attempting to check changes "
                    "with local version",
                    image_tag,
                )
            else:
                raise
    try:
        container = dc.containers.get(dc.api.create_container(image_tag)["Id"])
    except docker.errors.APIError as e:
        if e.response.status_code == 404:
            return None
        else:
            raise
    try:
        tarfile_path = tmp_dir / "tar-file.tar.gz"
        with open(tarfile_path, mode="w+b") as f:
            try:
                stream, _ = dc.api.get_archive(container.id, str(file_path))
            except docker.errors.NotFound:
                pass
            else:
                for chunk in stream:
                    f.write(chunk)
                f.flush()
    finally:
        container.remove()
    with tarfile.open(tarfile_path) as f:
        f.extractall(out_path)
    return out_path / PosixPath(file_path).name


def is_relative_to(a: Path, b: Path) -> bool:
    """Check if path a is relative to path b. Only required for Python < 3.9, after 3.8
    support is dropped can use pathlib.Path.is_relative_to instead.

    Parameters
    ----------
    a : Path
        the path to check if it is relative to b
    b : Path
        the path to check if a is relative to

    Returns
    -------
    bool
        True if a is relative to b, False otherwise
    """
    try:
        a.relative_to(b)
    except ValueError:
        return False
    else:
        return True


DOCKER_HUB = "docker.io"
GITHUB_CONTAINER_REGISTRY = "ghcr.io"
