from __future__ import annotations
import typing as ty
from pathlib import Path
from itertools import chain
import re
import logging
import shutil
import attrs
import yaml
import toml
from neurodocker.reproenv import DockerRenderer
from pipeline2app.core import __version__
from frametree.core.serialize import (
    ObjectConverter,
    ObjectListConverter,
    ClassResolver,
)
from typing_extensions import Self
from fileformats.core import DataType
from frametree.core.axes import Axes
from pipeline2app.core.utils import is_relative_to
from ..command.base import ContainerCommand
from .base import P2AImage
from .components import ContainerAuthor, License, Docs, PipPackage


logger = logging.getLogger("pipeline2app")


@attrs.define(kw_only=True)
class App(P2AImage):
    """A container image that contains a command with specific inputs and outputs to run.

    Parameters
    ----------
    name : str
        a name of the app to be used in naming of packages/images
    title : str
        a single line description of the app to be exposed in UIs
    version : str
        version of the package/build of the app
    org : str
        the organisation the image will be tagged within
    base_image : BaseImage, optional
        the base image to build from
    packages : Packages, optional
        the package manager used to install system packages (should match OS on base image)
    registry : str, optional
        the container registry the image is to be installed at
    authors : list[ContainerAuthor]
        list of authors of the package
    command : ContainerCommand
        description of the command that is to be run within the image
    licenses : list[ty.Dict[str, str]], optional
        specification of licenses required by the commands in the container. Each dict
        should contain the 'name' of the license and the 'destination' it should be
        installed inside the container.
    docs : Docs
        information for automatically generated documentation
    loaded_from : Path
        the file the spec was loaded from, if applicable
    """

    SUBPACKAGE = "deploy"

    title: str
    authors: ty.List[ContainerAuthor] = attrs.field(
        converter=ObjectListConverter(ContainerAuthor),  # type: ignore[misc]
        metadata={"serializer": ObjectListConverter.asdict},
    )
    licenses: ty.List[License] = attrs.field(
        factory=dict,
        converter=ObjectListConverter(License),  # type: ignore[misc]
        metadata={"serializer": ObjectListConverter.asdict},
    )
    docs: Docs = attrs.field(converter=ObjectConverter(Docs))  # type: ignore[misc]
    commands: ty.List[ContainerCommand] = attrs.field(
        converter=ObjectListConverter(ContainerCommand)  # type: ignore[misc]
    )
    loaded_from: Path = attrs.field(default=None, metadata={"asdict": False})
    pipeline2app_version: str = __version__

    @commands.validator
    def _validate_commands(
        self,
        attribute: attrs.Attribute[ty.List[ContainerCommand]],
        commands: ty.List[ContainerCommand],
    ) -> None:
        if not commands:
            raise ValueError("At least one command must be defined within that app")

    def __attrs_post_init__(self) -> None:
        # Set back-references to this image in the command spec
        for command in self.commands:
            command.image = self

    def command(self, name: ty.Optional[str] = None) -> ContainerCommand:
        if name is None:
            command = self.commands[0]  # Default to the first command
        else:
            try:
                command = next(c for c in self.commands if c.name == name)
            except StopIteration:
                raise KeyError(f"No command with name '{name}' found")
        return command

    def add_entrypoint(self, dockerfile: DockerRenderer, build_dir: Path) -> None:
        dockerfile.entrypoint(
            self.activate_conda() + ["pipeline2app", "pipeline-entrypoint"]
        )

    def construct_dockerfile(
        self,
        build_dir: Path,
        use_local_packages: bool = False,
        pypi_fallback: bool = False,
        pipeline2app_install_extras: ty.Sequence[str] = (),
        resources: ty.Optional[ty.Dict[str, Path]] = None,
        resources_dir: ty.Optional[Path] = None,
        **kwargs: ty.Any,
    ) -> DockerRenderer:
        """Constructs a dockerfile that wraps a with dependencies

        Parameters
        ----------
        build_dir : Path
            Path to the directory the Dockerfile will be written into copy any local
            files to
        **kwargs
            Passed onto the P2AImage.construct_dockerfile() method

        Returns
        -------
        DockerRenderer
            Neurodocker Docker renderer to construct dockerfile from
        """

        dockerfile = super().construct_dockerfile(
            build_dir=build_dir,
            use_local_packages=use_local_packages,
            pypi_fallback=pypi_fallback,
            pipeline2app_install_extras=pipeline2app_install_extras,
            resources=resources,
            resources_dir=resources_dir,
            **kwargs,
        )

        self.install_licenses(
            dockerfile,
            build_dir,
        )

        self.make_work_dir(dockerfile)

        self.add_entrypoint(dockerfile, build_dir)

        return dockerfile

    def install_licenses(
        self,
        dockerfile: DockerRenderer,
        build_dir: Path,
    ) -> None:
        """Generate Neurodocker instructions to install licenses within the container
        image

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        build_dir : Path
            path to build dir
        """
        # Copy licenses into build directory
        license_build_dir = build_dir / "licenses"
        license_build_dir.mkdir()
        for lic in self.licenses:
            if lic.store_in_image:
                if lic.source:
                    build_path = license_build_dir / lic.name
                    shutil.copyfile(lic.source, build_path)
                    dockerfile.copy(
                        source=[str(build_path.relative_to(build_dir))],
                        destination=str(lic.destination),
                    )
                else:
                    logger.warning(
                        "License file for '%s' was not provided, will attempt to download "
                        "from '%s' dataset-level column or site-wide license dataset at "
                        "runtime",
                        lic.name,
                        lic.column_path(lic.name),
                    )

    def make_work_dir(self, dockerfile: DockerRenderer) -> None:
        """Creates the internal work directory for the container and permissives sets
        permissions"""
        dockerfile.run("mkdir -p /wl")
        dockerfile.run("chmod 777 /wl")

    @classmethod
    def load(  # type: ignore[override]
        cls,
        yml: ty.Union[Path, ty.Dict[str, ty.Any]],
        root_dir: ty.Optional[Path] = None,
        license_paths: ty.Optional[ty.Dict[str, Path]] = None,
        licenses_to_download: ty.Optional[ty.Set[str]] = None,
        default_axes: ty.Optional[ty.Type[Axes]] = None,
        source_packages: ty.Sequence[Path] = (),
        **kwargs: ty.Any,
    ) -> Self:
        """Loads a deploy-build specification from a YAML file

        Parameters
        ----------
        yml : Path or dict
            path to the YAML file to load or loaded dictionary
        root_dir : Path, optional
            path to the root directory from which a tree of specs are being loaded from.
            The name of the root directory is taken to be the organisation the image
            belongs to, and all nested directories above the YAML file will be joined by
            '.' and prepended to the name of the loaded spec.
        license_paths : dict[str, Path], optional
            Licenses that are provided at build time to be included in the image.
        licenses_to_download : set[str], optional
            Licenses that are to be downloaded at runtime. If `license_paths` is not
            None (i.e. how to access required licenses are to be specified) then required
            licenses that are not in license_paths need to be explicitly listed in
            `licenses_to_download` otherwise an error is raised
        default_axes : type[Axes]
            the default data space to assume when one isn't explicitly defined
        source_packages : Sequence[Path]
            Paths to source packages to include in the image, will be used to determine
            the local version of the package to install
        **kwargs
            additional keyword arguments that override/augment the values loaded from
            the spec file

        Returns
        -------
        Self
            The loaded spec object
        """

        if isinstance(yml, str):
            yml = Path(yml)
        if root_dir is None:
            root_dir = Path.cwd()
        if isinstance(yml, Path):
            yml_dict = cls._load_yaml(yml)
            if not isinstance(yml_dict, dict):
                raise ValueError(f"{yml!r} didn't contain a dict!")
            if is_relative_to(yml, root_dir):
                rel_parts = yml.relative_to(root_dir).parent.parts + (yml.stem,)
                if "name" not in yml_dict:
                    yml_dict["name"] = ".".join(rel_parts[1:])

                if "org" not in yml_dict:
                    yml_dict["org"] = rel_parts[0]
            else:
                yml_dict["name"] = yml.stem

            yml_dict["loaded_from"] = yml.absolute()
        else:
            yml_dict = yml

        yml_dict.pop("type", None)  # Remove "type" from dict if present

        # Override/augment loaded values from spec
        yml_dict.update(kwargs)

        # If data-space is not defined, default to `default_axes`
        commands = yml_dict["commands"]
        if isinstance(commands, dict):
            commands = commands.values()
        for cmd in commands:
            if (
                "row_frequency" in cmd
                and re.match(r"\w+", cmd["row_frequency"])
                and default_axes
            ):
                cmd["row_frequency"] = default_axes[cmd["row_frequency"]]

        image = cls(**yml_dict)

        # Replace any pip packages with local source packages
        for source_package in source_packages:
            pyproject_toml = source_package / "pyproject.toml"
            package_name = None
            if pyproject_toml.exists():
                config = toml.load(pyproject_toml)
                package_name = config.get("project", {}).get("name")
            if package_name is None:
                package_name = source_package.name
            new_pip_pkg = None
            for pip_pkg in image.packages.pip:
                if pip_pkg.name == package_name:
                    new_pip_pkg = PipPackage(
                        name=package_name,
                        file_path=str(source_package),
                        extras=pip_pkg.extras,
                    )
                    image.packages.pip.remove(pip_pkg)
                    image.packages.pip.append(new_pip_pkg)
                    break
            if not new_pip_pkg:
                raise ValueError(
                    f"Could not find package {package_name} in the pip packages of the "
                    "image spec: " + '", "'.join(str(p) for p in image.packages.pip)
                )

        # Explicitly override directive in loaded spec to store license in the image

        if license_paths is not None:
            for lic in image.licenses:
                if licenses_to_download and lic.name in licenses_to_download:
                    lic.store_in_image = False
                if lic.store_in_image:
                    try:
                        lic.source = license_paths[lic.name]
                    except KeyError:
                        raise RuntimeError(
                            f"{lic.name} license has not been provided when it is "
                            "specified to be stored in the image"
                        )

        return image

    @classmethod
    def load_tree(
        cls, spec_path: Path, root_dir: ty.Optional[Path] = None, **kwargs: ty.Any
    ) -> ty.List[Self]:
        """Walk the given directory structure and load all specs found within it

        Parameters
        ----------
        spec_path : Path
            Path to spec or directory tree containing specs of the pipelines to build
        root_dir : Path
            path to the base of the spec directory
        """
        spec_path = Path(spec_path)
        if root_dir is None:
            if spec_path.is_file():
                raise ValueError(
                    "If a single file is provided, the root_dir must be specified"
                )
            logger.info("No root directory specified, using the spec directory")
            root_dir = spec_path
        root_dir = Path(root_dir)
        if spec_path.is_file():
            return [cls.load(spec_path, root_dir=root_dir, **kwargs)]
        specs = []
        for path in chain(spec_path.rglob("*.yml"), spec_path.rglob("*.yaml")):
            if not any(p.startswith(".") for p in path.parts):
                logging.info("Found container image specification file '%s'", path)
                specs.append(cls.load(path, root_dir=root_dir, **kwargs))

        return specs

    def autodoc(self, doc_dir: Path, flatten: bool) -> None:
        header = {
            "title": self.name,
            "weight": 10,
        }

        if self.org is None:
            raise ValueError(
                f"Organisation must be defined to generate documentation from App spec: {self}"
            )

        if self.loaded_from:
            header["source_file"] = str(self.loaded_from)

        if flatten:
            out_dir = doc_dir
        else:
            assert isinstance(doc_dir, Path)

            out_dir = doc_dir / self.org

            assert doc_dir in out_dir.parents or out_dir == doc_dir

            out_dir.mkdir(parents=True, exist_ok=True)

        with open(f"{out_dir}/{self.name}.md", "w") as f:
            f.write("---\n")
            yaml.dump(header, f)
            f.write("\n---\n\n")

            f.write("## Package Info\n")
            tbl_info = MarkdownTable(f, "Key", "Value")
            tbl_info.write_row("Name", self.name)
            tbl_info.write_row("Title", self.title)
            tbl_info.write_row("Version", str(self.version))
            tbl_info.write_row("Base image", escaped_md(self.base_image.reference))
            tbl_info.write_row(
                "Maintainer", f"{self.authors[0].name} ({self.authors[0].email})"
            )
            tbl_info.write_row("Info URL", self.docs.info_url)

            for known_issue in self.docs.known_issues:
                tbl_info.write_row(
                    "Known issues",
                    (
                        known_issue.description + f" ({known_issue.url})"
                        if known_issue.url
                        else ""
                    ),
                )

            desc = self.docs.description or self.title
            f.write(f"\n{desc}\n\n")

            if self.licenses:
                f.write("### Required licenses\n")

                tbl_lic = MarkdownTable(f, "Name", "URL", "Description")
                for lic in self.licenses:
                    tbl_lic.write_row(
                        lic.name,
                        escaped_md(lic.info_url),
                        lic.description.strip(),
                    )

                f.write("\n")

            f.write("## Commands\n")

            for command in self.commands:

                tbl_cmd = MarkdownTable(f, "Key", "Value")

                # if command.configuration is not None:
                #     config = command.configuration
                #     # configuration keys are variable depending on the workflow class
                tbl_cmd.write_row("Task", ClassResolver.tostr(command.task))
                freq_name = (
                    command.row_frequency.name
                    if not isinstance(command.row_frequency, str)
                    else re.match(r".*\[(\w+)\]", command.row_frequency).group(1)
                )
                tbl_cmd.write_row("Operates on", freq_name)

                f.write("#### Inputs\n")
                tbl_inputs = MarkdownTable(
                    f,
                    "Name",
                    "Required data-type",
                    "Default column data-type",
                    "Description",
                )
                if command.inputs is not None:
                    for inpt in command.inputs:
                        tbl_inputs.write_row(
                            escaped_md(inpt.name),
                            self._data_format_html(inpt.datatype),
                            self._data_format_html(inpt.column_defaults.datatype),
                            inpt.help,
                        )
                    f.write("\n")

                f.write("#### Outputs\n")
                tbl_outputs = MarkdownTable(
                    f,
                    "Name",
                    "Required data-type",
                    "Default column data-type",
                    "Description",
                )
                if command.outputs is not None:
                    for outpt in command.outputs:
                        tbl_outputs.write_row(
                            escaped_md(outpt.name),
                            self._data_format_html(outpt.datatype),
                            self._data_format_html(outpt.column_defaults.datatype),
                            outpt.help,
                        )
                    f.write("\n")

                if command.parameters is not None:
                    f.write("#### Parameters\n")
                    tbl_params = MarkdownTable(f, "Name", "Data type", "Description")
                    for param in command.parameters:
                        tbl_params.write_row(
                            escaped_md(param.name),
                            escaped_md(ClassResolver.tostr(param.datatype)),
                            param.help,
                        )
                    f.write("\n")

    # @classmethod
    # def load_in_image(cls, spec_path: Path = IN_DOCKER_SPEC_PATH):
    #     yml_dct = cls._load_yaml(spec_path)
    #     klass = ClassResolver(cls)(yml_dct.pop("type"))
    #     return klass.load(yml_dct)

    @classmethod
    def _data_format_html(cls, datatype: ty.Union[str, DataType]) -> str:
        datatype_str = datatype.mime_like if not isinstance(datatype, str) else datatype

        return (
            f'<span data-toggle="tooltip" data-placement="bottom" title="{datatype_str}" '
            f'aria-label="{datatype_str}">{datatype_str}</span>'
        )

    DOCKERFILE_README_TEMPLATE = """
        The following Docker image was generated by Pipeline2app v{} to enable the
        commands to be run in the XNAT container service. See
        https://raw.githubusercontent.com/Australian-Imaging-Service/pipeline2app/main/LICENSE
        for licence.

        {}

        """


class MarkdownTable:
    def __init__(self, f: ty.IO[str], *headers: str) -> None:
        self.headers = tuple(headers)

        self.f = f
        self._write_header()

    def _write_header(self) -> None:
        self.write_row(*self.headers)
        self.write_row(*("-" * len(x) for x in self.headers))

    def write_row(self, *cols_tple: str) -> None:
        cols = list(cols_tple)
        if len(cols) > len(self.headers):
            raise ValueError(
                f"More entries in row ({len(cols)} than columns ({len(self.headers)})"
            )

        # pad empty column entries if there's not enough
        cols += [""] * (len(self.headers) - len(cols))

        # TODO handle new lines in col
        self.f.write(
            "|" + "|".join(str(col).replace("|", "\\|") for col in cols) + "|\n"
        )


def escaped_md(value: str) -> str:
    if not value:
        return ""
    return f"`{value}`"
