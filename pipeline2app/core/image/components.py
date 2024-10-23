from __future__ import annotations
import typing as ty
from pathlib import Path, PurePath
import json
import pkg_resources
import logging
from urllib.parse import urlparse
import re
from itertools import zip_longest
from typing_extensions import Self
import site
import attrs
from pipeline2app.core import PACKAGE_NAME
from pipeline2app.core.exceptions import Pipeline2appBuildError
from frametree.core.serialize import ObjectListConverter

logger = logging.getLogger("pipeline2app")


@attrs.define(kw_only=True)
class BaseImage:

    DEFAULT_IMAGE = "debian"
    DEFAULT_IMAGE_TAG = "bookworm-slim"
    DEFAULT_CONDA_ENV = "pipeline2app"
    DEFAULT_USER = "root"

    name: str = attrs.field(default=DEFAULT_IMAGE)
    tag: str = attrs.field()
    package_manager: str = attrs.field()
    python: str = attrs.field(default=None)
    conda_env: str = attrs.field()
    user: str = attrs.field(default=DEFAULT_USER)

    @property
    def reference(self) -> str:
        if self.tag:
            reference = f"{self.name}:{self.tag}"
        else:
            reference = self.name
        return reference

    @name.validator
    def name_validator(self, _: attrs.Attribute[str], name: str) -> None:
        if name == "alpine":
            raise ValueError(
                "Neurodocker (the package used to build the images) does not currently "
                "support alpine base images"
            )

    @tag.default
    def tag_default(self) -> ty.Optional[str]:
        if self.name == self.DEFAULT_IMAGE:
            tag = self.DEFAULT_IMAGE_TAG
        else:
            tag = None
        return tag

    @package_manager.default
    def package_manager_default(self) -> ty.Optional[str]:
        if self.name in ("ubuntu", "debian"):
            package_manager = "apt"
        elif self.name in ("fedora", "centos"):
            package_manager = "yum"
        else:
            package_manager = None
        return package_manager

    @package_manager.validator
    def package_manager_validator(
        self, _: attrs.Attribute[ty.Optional[str]], package_manager: ty.Optional[str]
    ) -> None:
        if package_manager is None:
            raise ValueError(
                f"Package manager must be supplied explicitly for unknown base image "
                f"'{self.name}' (note only 'apt' and 'yum' package managers are "
                "currently supported)"
            )
        if package_manager not in ("yum", "apt"):
            raise ValueError(
                f"Unsupported package manager '{package_manager}' provided. Only 'apt' "
                "and 'yum' package managers are currently supported by Neurodocker"
            )

    @conda_env.default
    def conda_env_default(self) -> ty.Optional[str]:
        if self.python:
            conda_env = None
        else:
            conda_env = self.DEFAULT_CONDA_ENV
        return conda_env


@attrs.define
class ContainerAuthor:

    name: str
    email: str
    affliation: ty.Optional[str] = None


@attrs.define
class KnownIssue:

    description: str
    url: ty.Optional[str] = None


@attrs.define
class Docs:

    info_url: str = attrs.field()
    description: ty.Optional[str] = None
    known_issues: ty.List[KnownIssue] = attrs.field(
        factory=list,
        converter=ObjectListConverter(KnownIssue),  # type: ignore[misc]
        metadata={"serializer": ObjectListConverter.aslist},
    )

    @info_url.validator
    def info_url_validator(self, _: attrs.Attribute[str], info_url: str) -> None:
        parsed = urlparse(info_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(
                f"Could not parse info url '{info_url}', please include URL scheme"
            )


def optional_path_converter(value: ty.Optional[str]) -> ty.Optional[Path]:
    return Path(value) if value is not None else None


@attrs.define
class License:
    """Specification of a software license that needs to be present in the container
    when the command is run.

    Parameters
    ----------
    name : str
        a name to refer to the license with. Must be unique among the licenses used
        pipelines applied to a dataset, ideally for a site. Typically named closely
        after the package it is used for along with a version number if the license,
        needs to be updated with new versions e.g. freesurfer, fsl, matlab_v2022a etc...
    destination : PurePath
        destination within the container to install the license
    description : str
        a short description of the license and what it is used for
    info_url : str
        link to website with information about license, particularly how to download it
    source : Path, optional
        path to the location of a valid license file
    store_in_image : bool
        whether the license can be stored in the image or not according to the license
        conditions
    """

    name: str = attrs.field()
    destination: PurePath = attrs.field(converter=PurePath)
    description: str = attrs.field()
    info_url: str = attrs.field()
    source: ty.Optional[Path] = attrs.field(
        default=None, converter=optional_path_converter
    )
    store_in_image: bool = False

    @info_url.validator
    def info_url_validator(self, _: attrs.Attribute[str], info_url: str) -> None:
        parsed = urlparse(info_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(
                f"Could not parse info url '{info_url}', please include URL scheme"
            )

    # FIXME: this doesn't work inside images
    # @source.validator
    # def source_validator(self, _, source):
    #     if source is not None and not source.exists():
    #         raise ValueError(
    #             f"Source file for {self.name} license, '{str(source)}', does not exist"
    #         )

    @classmethod
    def column_path(self, name: str) -> str:
        """The column name (and resource name) for the license if it is to be downloaded
        from the source dataset"""
        return name + self.COLUMN_SUFFIX + "@"

    COLUMN_SUFFIX = "_LICENSE"


def base_package_version_converter(v: str) -> str:
    return str(v) if v is not None else None


@attrs.define
class BasePackage:

    name: str
    version: str = attrs.field(default=None, converter=base_package_version_converter)


def pip_package_extras_converter(
    extras: ty.Union[str, ty.Iterable[str]]
) -> ty.List[str]:
    if isinstance(extras, str):
        extras = extras.split(",")
    return list(extras)


@attrs.define
class PipPackage(BasePackage):
    """Specification of a Python package"""

    url: ty.Optional[str] = None
    file_path: ty.Optional[str] = None
    extras: ty.List[str] = attrs.field(
        factory=list, converter=pip_package_extras_converter
    )

    @classmethod
    def unique(
        cls, pip_specs: ty.Iterable[PipPackage], remove_pipeline2app: bool = False
    ) -> ty.List[PipPackage]:
        """Merge a list of Pip install specs so each package only appears once

        Parameters
        ----------
        pip_specs : ty.Iterable[PipPackage]
            the pip specs to merge
        remove_pipeline2app : bool
            remove pipeline2app if present from the merged list

        Returns
        -------
        list[PipPackage]
            the merged pip specs

        Raises
        ------
        Pipeline2appError
            if there is a mismatch between two entries of the same package
        """
        dct: ty.Dict[str, PipPackage] = {}
        for pip_spec in pip_specs:
            if isinstance(pip_spec, dict):
                pip_spec = PipPackage(**pip_spec)
            if pip_spec.name == PACKAGE_NAME and remove_pipeline2app:
                continue
            try:
                prev_spec = dct[pip_spec.name]
            except KeyError:
                dct[pip_spec.name] = pip_spec
            else:
                if (
                    prev_spec.version != pip_spec.version
                    or prev_spec.url != pip_spec.url
                    or prev_spec.file_path != pip_spec.file_path
                ):
                    raise RuntimeError(
                        f"Cannot install '{pip_spec.name}' due to conflict "
                        f"between requested versions, {pip_spec} and {prev_spec}"
                    )
                prev_spec.extras.extend(pip_spec.extras)
        return list(dct.values())

    def local_package_location(self, pypi_fallback: bool = False) -> PipPackage:
        """Detect the installed locations of the packages, including development
        versions.

        Parameters
        ----------
        package : [PipPackage]
            the packages (or names of) the versions to detect
        pypi_fallback : bool, optional
            Fallback to PyPI version if requested version isn't installed locally

        Returns
        -------
        PipPackage
            the pip specification for the installation location of the package
        """
        try:
            pkg = next(
                p for p in pkg_resources.working_set if p.project_name == self.name
            )
        except StopIteration:
            if pypi_fallback:
                logger.info(
                    f"Did not find local installation of package {self.name} "
                    "falling back to installation from PyPI"
                )
                return self
            raise Pipeline2appBuildError(
                f"Did not find {self.name} in installed working set:\n"
                + "\n".join(
                    sorted(
                        p.key + "/" + p.project_name for p in pkg_resources.working_set
                    )
                )
            )
        if (
            self.version
            and (
                not (pkg.version.endswith(".dirty") or self.version.endswith(".dirty"))
            )
            and pkg.version != self.version
        ):
            msg = (
                f"Requested package {self.name}=={self.version} does "
                "not match installed " + pkg.version
            )
            if pypi_fallback:
                logger.warning(msg + " falling back to installation from PyPI")
                return self
            raise Pipeline2appBuildError(msg)
        if pkg.location is None:
            raise Pipeline2appBuildError(
                f"Could not find location of package {self.name} in installed working set, "
                f"{pkg} has no local location"
            )
        pkg_loc = Path(pkg.location).resolve()
        # Determine whether installed version of requirement is locally
        # installed (and therefore needs to be copied into image) or can
        # be just downloaded from PyPI
        if pkg_loc not in site_pkg_locs:
            # Copy package into Docker image and instruct pip to install from
            # that copy
            local_spec = PipPackage(
                name=self.name, file_path=str(pkg_loc), extras=self.extras
            )
        else:
            # Check to see whether package is installed via "direct URL" instead
            # of through PyPI
            direct_url_path = Path(pkg.egg_info) / "direct_url.json"
            if direct_url_path.exists():
                with open(direct_url_path) as f:
                    url_spec = json.load(f)
                url = url_spec["url"]
                vcs_info = url_spec.get(
                    "vcs_info", url_spec
                )  # Fallback to trying to find VCS info in the base url-spec dict
                if url.startswith("file://"):
                    local_spec = PipPackage(
                        name=self.name,
                        file_path=url[len("file://") :],
                        extras=self.extras,
                    )
                else:
                    vcs_info = url_spec.get("vcs_info", url_spec)
                    if "vcs" in vcs_info:
                        url = vcs_info["vcs"] + "+" + url
                    if "commit_id" in vcs_info:
                        url += "@" + vcs_info["commit_id"]
                    local_spec = PipPackage(name=self.name, url=url, extras=self.extras)
            else:
                local_spec = PipPackage(
                    name=self.name, version=pkg.version, extras=self.extras
                )
        return local_spec


@attrs.define
class SystemPackage(BasePackage):

    pass


@attrs.define
class CondaPackage(BasePackage):

    pass

    # REQUIRED = ["numpy", "traits"]  # FIXME: Not sure if traits is actually required


@attrs.define
class NeurodockerTemplate:

    name: str
    version: str
    args: ty.Dict[str, ty.Any] = attrs.field(factory=dict)


def python_package_converter(
    packages: ty.List[ty.Union[str, ty.Dict[str, ty.Any]]]
) -> ty.List[PipPackage]:
    """
    Split out and merge any extras specifications (e.g. "pipeline2app[test]")
    between dependencies of the same package
    """
    return PipPackage.unique(
        ObjectListConverter(PipPackage)(
            packages,
        ),
        remove_pipeline2app=True,
    )


@attrs.define
class Packages:

    system: ty.List[SystemPackage] = attrs.field(
        factory=list,
        converter=ObjectListConverter(SystemPackage),  # type: ignore[misc]
        metadata={"serializer": ObjectListConverter.asdict},
    )
    pip: ty.List[PipPackage] = attrs.field(
        factory=list,
        converter=python_package_converter,
        metadata={"serializer": ObjectListConverter.asdict},
    )
    conda: ty.List[CondaPackage] = attrs.field(
        factory=list,
        converter=ObjectListConverter(CondaPackage),  # type: ignore[misc]
        metadata={"serializer": ObjectListConverter.asdict},
    )
    neurodocker: ty.List[NeurodockerTemplate] = attrs.field(
        factory=list,
        converter=ObjectListConverter(NeurodockerTemplate),  # type: ignore[misc]
        metadata={"serializer": ObjectListConverter.asdict},
    )


site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]


@attrs.define
class Resource:

    name: str
    path: Path  # the path to the resource within the container
    description: str = ""


@attrs.define
class Version:

    release: ty.Union[ty.Tuple[int, ...], str]
    suffix_label: str = ""
    suffix_number: int = 0

    @classmethod
    def parse(cls, version: ty.Union[str, Self]) -> Self:
        if not isinstance(version, str):
            if not isinstance(version, cls):
                raise ValueError(
                    f"Cannot parse version from object of type {type(version)}"
                )
            return version
        match = cls.version_re.match(version)
        if match is None:
            return cls(version)
        release = match.group("release")
        try:
            release = tuple(int(r) for r in release.split("."))
        except ValueError:
            pass
        suffix_label = match.group("suffix_l") or ""
        if suffix_label and suffix_label not in cls.SUFFIX_LABELS:
            raise ValueError(
                f"Invalid suffix label {suffix_label}, must be one of {cls.SUFFIX_LABELS}"
            )
        suffix_number = int(match.group("suffix_n")) if match.group("suffix_n") else 0
        return cls(release, suffix_label, suffix_number)

    @classmethod
    def tostr(cls, obj: Self, **kwargs: ty.Any) -> str:
        return str(obj)

    def compare(self, other: "Version") -> int:
        if (isinstance(self.release, str) and isinstance(other.release, tuple)) or (
            isinstance(self.release, tuple) and isinstance(other.release, str)
        ):
            raise ValueError("Cannot compare versions with different release types")

        if isinstance(self.release, tuple) and isinstance(other.release, tuple):
            for s, o in zip_longest(self.release, other.release, fillvalue=0):
                if s < o:
                    return -1
                if s > o:
                    return 1
        else:
            if self.release < other.release:  # type: ignore[operator]
                return -1
            if self.release > other.release:  # type: ignore[operator]
                return 1
        if self.suffix_label == "post" and other.suffix_label != "post":
            return 1
        if self.suffix_label != "post" and other.suffix_label == "post":
            return -1
        if not self.suffix_label and other.suffix_label:
            return 1
        if self.suffix_label and not other.suffix_label:
            return -1
        if self.suffix_label:
            if not other.suffix_label:
                return 1
            label_index = self.SUFFIX_LABELS.index(self.suffix_label)
            other_label_index = self.SUFFIX_LABELS.index(other.suffix_label)
            if label_index < other_label_index:
                return -1
            if label_index > other_label_index:
                return 1
            if self.suffix_number < other.suffix_number:
                return -1
            if self.suffix_number > other.suffix_number:
                return 1
        elif other.suffix_label:
            return -1
        return 0

    def __str__(self) -> str:
        release_str = (
            ".".join(str(r) for r in self.release)
            if isinstance(self.release, tuple)
            else self.release
        )
        return release_str + (
            f"-{self.suffix_label}{self.suffix_number}" if self.suffix_label else ""
        )

    def __lt__(self, other: "Version") -> bool:
        return self.compare(other) < 0

    def __le__(self, other: "Version") -> bool:
        return self.compare(other) <= 0

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            try:
                other_version = Version(other)
            except ValueError:
                return False
        elif not isinstance(other, Version):
            return False
        else:
            other_version = other
        return self.compare(other_version) == 0

    def __ne__(self, other: object) -> bool:
        return not (self.release == other)

    def __gt__(self, other: "Version") -> bool:
        return self.compare(other) > 0

    def __ge__(self, other: "Version") -> bool:
        return self.compare(other) >= 0

    def __repr__(self) -> str:
        return f"Version({str(self)})"

    def __hash__(self) -> int:
        return hash(str(self))

    @classmethod
    def latest(cls, versions: ty.List[ty.Union[str, "Version"]]) -> "Version":
        version_objs = [
            v if isinstance(v, cls) else cls.parse(v) for v in versions  # type: ignore[arg-type]
        ]
        return sorted(version_objs)[-1]

    def bump_postfix(self) -> "Version":
        suffix_label = self.suffix_label if self.suffix_label else "post"
        suffix_number = self.suffix_number + 1
        return Version(self.release, suffix_label, suffix_number)

    SUFFIX_LABELS = ["alpha", "beta", "rc", "post"]

    version_re = re.compile(
        (
            r"^v?(?P<release>[a-zA-Z0-9_\.]+)"
            r"(?P<suffix>-(?P<suffix_l>"
            + "|".join(SUFFIX_LABELS)
            + r")(?P<suffix_n>[0-9]+)?)?$"
        ),
        re.VERBOSE | re.IGNORECASE,
    )
