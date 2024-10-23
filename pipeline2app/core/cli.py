import sys
import logging
import shutil
from pathlib import Path
import json
import typing as ty
import re
from collections import defaultdict
from traceback import format_exc
import tempfile
import click
import yaml
import docker
import docker.errors
from pydra.engine.core import TaskBase
from frametree.core.serialize import (
    package_from_module,
    pydra_asdict,
    ClassResolver,
)
from frametree.core.packaging import submodules
import pipeline2app
from pipeline2app.core import __version__
from pipeline2app.core.image import Metapackage, App
from pipeline2app.core.utils import (
    DOCKER_HUB,
)
from pipeline2app.core.command import entrypoint_opts
from pipeline2app.core import PACKAGE_NAME


logger = logging.getLogger("pipeline2app")


# Define the base CLI entrypoint
@click.group()
@click.version_option(version=__version__)
def cli() -> None:
    """Base command line group, installed as "pipeline2app"."""
    return None


@cli.command(
    name="make",
    help="""Construct and build a docker image containing a pipeline to be run on data
stored in a data repository or structure (e.g. XNAT Container Service Pipeline or BIDS App)

TARGET is the type of image to build. For standard images just the pipeline2app
sub-package is required (e.g. 'xnat' or 'common'). However, specific App subclasses can
be specified using <module-path>:<app-class-name> format, e.g. pipeline2app.xnat:XnatApp

SPEC_PATH is the file system path to the specification to build, or directory
containing multiple specifications
""",
)
@click.argument("target", type=str)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--registry",
    default=DOCKER_HUB,
    help="The Docker registry to deploy the pipeline to",
)
@click.option(
    "--build-dir",
    default=None,
    type=click.Path(path_type=Path),
    help=(
        "Specify the directory to build the Docker image in. "
        "Defaults to `.build` in the directory containing the "
        "YAML specification"
    ),
)
@click.option(
    "--release",
    default=None,
    nargs=2,
    metavar="<release-name> <release-version>",
    type=str,
    help=("Name of the release for the package as a whole (i.e. for all pipelines)"),
)
@click.option(
    "--tag-latest/--dont-tag-latest",
    default=False,
    type=bool,
    help='whether to tag the release as the "latest" or not',
)
@click.option(
    "--save-manifest",
    default=None,
    type=click.Path(writable=True),
    help="File path at which to save the build manifest",
)
@click.option(
    "--logfile",
    default=None,
    type=click.Path(path_type=Path),
    help="Log output to file instead of stdout",
)
@click.option("--loglevel", default="info", help="The level to display logs at")
@click.option(
    "--use-local-packages/--dont-use-local-packages",
    type=bool,
    default=False,
    help=(
        "Use locally installed Python packages, instead of pulling "
        "them down from PyPI"
    ),
)
@click.option(
    "--install-extras",
    "install_extras_str",
    type=str,
    default=None,
    help=(
        "Install extras to use when installing Pipeline2app inside the "
        "container image. Typically only used in tests to provide "
        "'test' extra"
    ),
)
@click.option(
    "--for-localhost/--not-for-localhost",
    type=bool,  # FIXME: This should be replaced with option to set XNAT CS IP address
    default=False,
    help=(
        "Build the image so that it can be run in Pipeline2app's test "
        "configuration (only for internal use)"
    ),
)
@click.option(
    "--raise-errors/--log-errors",
    type=bool,
    default=False,
    help=("Raise exceptions instead of logging failures"),
)
@click.option(
    "--generate-only/--build",
    type=bool,
    default=False,
    help="Just create the build directory and dockerfile",
)
@click.option(
    "--license",
    type=(str, click.Path(exists=True, path_type=Path)),
    default=(),
    nargs=2,
    metavar="<license-name> <path-to-license-file>",
    multiple=True,
    help=(
        "Licenses provided at build time to be stored in the image (instead of "
        "downloaded at runtime)"
    ),
)
@click.option(
    "--license-to-download",
    type=str,
    default=(),
    multiple=True,
    help=(
        "Specify licenses that are not provided at runtime and instead downloaded "
        "from the data store at runtime in order to satisfy their conditions"
    ),
)
@click.option(
    "--check-registry/--dont-check-registry",
    type=bool,
    default=None,
    help=(
        "Check the registry to see if an existing image with the "
        "same tag is present, and if so whether the specification "
        "matches (and can be skipped) or not (raise an error)"
    ),
)
@click.option(
    "--push/--dont-push",
    type=bool,
    default=False,
    help=("push built images to registry"),
)
@click.option(
    "--clean-up/--dont-clean-up",
    type=bool,
    default=False,
    help=("Remove built images after they are pushed to the registry"),
)
@click.option(
    "--spec-root",
    type=click.Path(path_type=Path, exists=True),
    default=None,
    help=("The root path to consider the specs to be relative to, defaults to CWD"),
)
@click.option(
    "--source-package",
    "-s",
    type=click.Path(path_type=Path, exists=True),
    multiple=True,
    default=(),
    help=(
        "Path to a local Python package to be included in the image. Needs to have a "
        "package definition that can be built into a source distribution and the name of "
        "the directory needs to match that of the package to be installed. Multiple "
        "packages can be specified by repeating the option."
    ),
)
@click.option(
    "--export-file",
    "-e",
    "export_files",
    type=str,
    nargs=2,
    multiple=True,
    default=(),
    metavar="<internal-dir> <external-dir>",
    help=(
        "Path to be exported from the Docker build directory for convenience. Multiple "
        "files can be specified by repeating the option."
    ),
)
@click.option(
    "--resource",
    type=str,
    nargs=2,
    multiple=True,
    default=(),
    metavar="<name> <path>",
    help=(
        "Supply resources to be copied into the image, the name should match up with "
        "the name of a resource within the spec"
    ),
)
@click.option(
    "--resources-dir",
    type=click.Path(path_type=Path, exists=True),
    metavar="<path>",
    default=None,
    help=(
        "A directory containing resources to be copied into the image, the names of the "
        "sub-directories within the resources dir should match up with "
        "the name of a resource within the spec"
    ),
)
@click.option(
    "--stream-logs/--no-stream-logs",
    type=bool,
    default=None,
    help=(
        "Stream the build logs to stdout as they are generated. "
        "Defaults to True if the log-level <= info"
    ),
)
def make(
    target: str,
    spec_path: Path,
    registry: str,
    release: ty.Optional[ty.Tuple[str, str]],
    tag_latest: bool,
    save_manifest: ty.Optional[Path],
    logfile: Path,
    loglevel: str,
    build_dir: Path,
    use_local_packages: bool,
    install_extras_str: str,
    raise_errors: bool,
    generate_only: bool,
    for_localhost: bool,
    license: ty.List[ty.Tuple[str, Path]],
    license_to_download: ty.List[str],
    check_registry: ty.Optional[bool],
    push: bool,
    clean_up: bool,
    resource: ty.List[ty.Tuple[str, str]],
    resources_dir: ty.Optional[Path],
    spec_root: Path,
    source_package: ty.Sequence[Path],
    export_files: ty.Sequence[ty.Tuple[Path, Path]],
    stream_logs: ty.Optional[bool],
) -> None:

    if check_registry is None:
        check_registry = push

    if isinstance(spec_path, bytes):  # FIXME: This shouldn't be necessary
        spec_path = Path(spec_path.decode("utf-8"))
    if isinstance(build_dir, bytes):  # FIXME: This shouldn't be necessary
        build_dir = Path(build_dir.decode("utf-8"))

    resources: ty.List[ty.Tuple[str, Path]] = []
    for rname, rpath_str in resource:
        rpath = Path(rpath_str)
        if not rpath.exists():
            raise ValueError(
                f"Resource path {str(rpath)!r} provided to {rname!r} does not exist"
            )
        resources.append((rname, rpath))

    if tag_latest and not release:
        raise ValueError("'--tag-latest' flag requires '--release'")

    if spec_root is None:
        if spec_path.is_file():
            spec_root = spec_path.parent.parent
        else:
            spec_root = spec_path.parent
        logger.info(
            "`--spec-root` was not explicitly provided so assuming it is the parent '%s'",
            str(spec_root),
        )

    path_parts = spec_path.relative_to(spec_root).parts

    if spec_path.is_file() and len(path_parts) < 2:
        raise ValueError(
            f"Spec paths ({spec_path}) must be placed within (a) nested director(y|ies) "
            "from the spec root. The top-level nested directory will be interpreted as "
            "the name of the Docker package and subsequent directories will be used to "
            "qualify the image name with '.' separated prefixes"
        )

    package_name = path_parts[0]

    if build_dir is None:
        if spec_path.is_file():
            build_dir = spec_path.parent / (".build-" + spec_path.stem)
        else:
            build_dir = spec_path / ".build"

    if not build_dir.exists():
        build_dir.mkdir()

    install_extras: ty.List[str] = (
        install_extras_str.split(",") if install_extras_str else []
    )

    logging.basicConfig(filename=logfile, level=getattr(logging, loglevel.upper()))

    temp_dir = tempfile.mkdtemp()

    target_cls: App = ClassResolver(App, package=PACKAGE_NAME)(target)

    dc = docker.from_env()

    license_paths = {}
    for lic_name, lic_src in license:
        if isinstance(lic_src, bytes):  # FIXME: This shouldn't be necessary
            lic_src = Path(lic_src.decode("utf-8"))
        license_paths[lic_name] = lic_src

    # Load image specifications from YAML files stored in directory tree

    # Don't error if the modules the task, data stores, data types, etc...
    # aren't present in the build environment
    # FIXME: need to test for this
    with ClassResolver.FALLBACK_TO_STR:
        image_specs: ty.List[App] = target_cls.load_tree(
            spec_path,
            root_dir=spec_root,
            registry=registry,
            license_paths=license_paths,
            licenses_to_download=set(license_to_download),
            source_packages=source_package,
        )

    # Check the target registry to see a) if the images with the same tag
    # already exists and b) whether it was built with the same specs

    errors = False
    manifest: ty.Dict[str, ty.Any] = {}
    if release or save_manifest:
        manifest = {
            "package": package_name,
            "images": [],
        }
        if release:
            manifest["release"] = ":".join(release)

    for image_spec in image_specs:
        image_reference = image_spec.reference
        if (
            check_registry
            and image_spec.latest_published
            and image_spec.latest_published >= image_spec.version
        ):
            latest_reference = f"{image_spec.path}:{image_spec.latest_published}"
            if image_spec.matches_image(latest_reference):
                logger.info(
                    "Skipping '%s' build as identical image already exists in registry '%s'",
                    image_reference,
                    latest_reference,
                )
                continue
            image_reference = (
                f"{image_spec.path}:{image_spec.latest_published.bump_postfix()}"
            )

        spec_build_dir = (
            build_dir / image_spec.loaded_from.relative_to(spec_path.absolute())
        ).with_suffix("")
        if spec_build_dir.exists():
            shutil.rmtree(spec_build_dir)
        spec_build_dir.mkdir(parents=True)
        try:
            image_spec.make(
                build_dir=spec_build_dir,
                for_localhost=for_localhost,
                use_local_packages=use_local_packages,
                generate_only=generate_only,
                resources=resources,
                resources_dir=resources_dir,
                pipeline2app_install_extras=install_extras,
                no_cache=clean_up,
                stream_logs=stream_logs,
                reference=image_reference,
            )
        except Exception:
            if raise_errors:
                raise
            logger.error(
                "Could not build %s pipeline:\n%s", image_reference, format_exc()
            )
            errors = True
            continue
        else:
            click.echo(image_reference)
            logger.info("Successfully built %s pipeline", image_reference)

        if push:
            try:
                dc.api.push(image_reference)
            except Exception:
                if raise_errors:
                    raise
                logger.error(
                    "Could not push '%s':\n\n%s", image_reference, format_exc()
                )
                errors = True
            else:
                logger.info("Successfully pushed '%s' to registry", image_reference)
        if clean_up:

            def remove_image_and_containers(image_ref: str) -> None:
                logger.info(
                    "Removing '%s' image and associated containers to free up disk space "
                    "as '--clean-up' is set",
                    image_ref,
                )
                for container in dc.containers.list(filters={"ancestor": image_ref}):
                    container.stop()
                    container.remove()
                dc.images.remove(image_ref, force=True)
                result = dc.containers.prune()
                dc.images.prune(filters={"dangling": False})
                logger.info(
                    "Removed '%s' image and associated containers and freed up %s of disk space ",
                    image_ref,
                    result["SpaceReclaimed"],
                )

            remove_image_and_containers(image_reference)
            remove_image_and_containers(image_spec.base_image.reference)

        if release or save_manifest:
            path, tag = image_reference.split(":")
            manifest["images"].append(
                {
                    "name": path,
                    "version": tag,
                }
            )
    if release:
        metapkg = Metapackage(
            name=release[0],
            version=release[1],
            org=package_name,
            manifest=manifest,
        )
        metapkg.make(use_local_packages=use_local_packages)
        if push:
            try:
                dc.api.push(metapkg.reference)
            except Exception:
                if raise_errors:
                    raise
                logger.error(
                    "Could not push release metapackage '%s':\n\n%s",
                    metapkg.reference,
                    format_exc(),
                )
                errors = True
            else:
                logger.info(
                    "Successfully pushed release metapackage '%s' to registry",
                    metapkg.reference,
                )

            if tag_latest:
                # Also push release to "latest" tag
                image = dc.images.get(metapkg.reference)
                latest_tag = metapkg.path + ":latest"
                image.tag(latest_tag)

                try:
                    dc.api.push(latest_tag)
                except Exception:
                    if raise_errors:
                        raise
                    logger.error(
                        "Could not push latest tag for release metapackage '%s':\n\n%s",
                        metapkg.path,
                        format_exc(),
                    )
                    errors = True
                else:
                    logger.info(
                        (
                            "Successfully pushed latest tag for release metapackage '%s' "
                            "to registry"
                        ),
                        metapkg.path,
                    )
        if save_manifest:
            with open(save_manifest, "w") as f:
                json.dump(manifest, f, indent="    ")

    for src_path, dest_path in export_files:
        dest_path = Path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        full_src_path = build_dir / src_path
        if not full_src_path.exists():
            logger.warning(
                "Could not find file '%s' to export from build directory", full_src_path
            )
            continue
        if full_src_path.is_dir():
            shutil.copytree(full_src_path, dest_path)
        else:
            shutil.copy(full_src_path, dest_path)

    shutil.rmtree(temp_dir)
    if errors:
        sys.exit(1)


@cli.command(
    name="list-images",
    help="""Walk through the specification paths and list tags of the images
that will be build from them.

SPEC_ROOT is the file system path to the specification to build, or directory
containing multiple specifications

DOCKER_ORG is the Docker organisation the images should belong to""",
)
@click.argument("spec_root", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--registry",
    default=None,
    help="The Docker registry to deploy the pipeline to",
)
def list_images(spec_root: Path, registry: str) -> None:
    if isinstance(spec_root, bytes):  # FIXME: This shouldn't be necessary
        spec_root = Path(spec_root.decode("utf-8"))

    for image_spec in App.load_tree(spec_root, registry=registry):
        click.echo(image_spec.reference)


@cli.command(
    name="make-docs",
    help="""Build docs for one or more yaml wrappers

SPEC_ROOT is the path of a YAML spec file or directory containing one or more such files.

The generated documentation will be saved to OUTPUT.
""",
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@click.argument("output", type=click.Path(path_type=Path))
@click.option(
    "--registry",
    default=DOCKER_HUB,
    help="The Docker registry to deploy the pipeline to",
)
@click.option("--flatten/--no-flatten", default=False)
@click.option("--loglevel", default="warning", help="The level to display logs at")
@click.option(
    "--default-axes",
    default=None,
    help=("The default axes to assume if it isn't explicitly stated in the command"),
)
@click.option(
    "--spec-root",
    type=click.Path(path_type=Path),
    default=None,
    help=("The root path to consider the specs to be relative to, defaults to CWD"),
)
def make_docs(
    spec_path: Path,
    output: Path,
    registry: str,
    flatten: bool,
    loglevel: str,
    default_axes: ty.Optional[str],
    spec_root: Path,
) -> None:
    # # FIXME: Workaround for click 7.x, which improperly handles path_type
    # if type(spec_path) is bytes:
    #     spec_path = Path(spec_path.decode("utf-8"))
    # if type(output) is bytes:
    #     output = Path(output.decode("utf-8"))

    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    output.mkdir(parents=True, exist_ok=True)

    default_axes = ClassResolver.fromstr(default_axes)

    with ClassResolver.FALLBACK_TO_STR:
        image_specs = App.load_tree(
            spec_path,
            registry=registry,
            root_dir=spec_root,
            default_axes=default_axes,
        )

    for image_spec in image_specs:
        image_spec.autodoc(output, flatten=flatten)
        logging.info("Successfully created docs for %s", image_spec.path)


@cli.command(
    name="required-packages",
    help="""Detect the Python packages required to run the
specified workflows and return them and their versions""",
)
@click.argument("task_locations", nargs=-1)
def required_packages(task_locations: ty.List[str]) -> None:
    required_modules: ty.Set[str] = set()
    for task_location in task_locations:
        workflow = ClassResolver(
            TaskBase, alternative_types=[ty.Callable], package=PACKAGE_NAME
        )(task_location)
        pydra_asdict(workflow, required_modules)

    for pkg in package_from_module(required_modules):
        click.echo(f"{pkg.key}=={pkg.version}")


@cli.command(
    name="inspect-docker-exec", help="""Extract the executable from a Docker image"""
)
@click.argument("image_tag", type=str)
def inspect_docker_exec(image_tag: str) -> None:
    """Pulls a given Docker image tag and inspects the image to get its
    entrypoint/cmd

    IMAGE_TAG is the tag of the Docker image to inspect"""
    dc = docker.from_env()

    dc.images.pull(image_tag)

    image_attrs = dc.api.inspect_image(image_tag)["Config"]

    executable = image_attrs["Entrypoint"]
    if executable is None:
        executable = image_attrs["Cmd"]

    click.echo(executable)


@cli.command(
    help="""Displays the changelogs found in the release manifest of a deployment build

MANIFEST_JSON is a JSON file containing a list of container images built in the release
and the commands present in them"""
)
@click.argument("manifest_json", type=click.File())
@click.argument("images", nargs=-1)
def changelog(manifest_json: ty.TextIO, images: ty.List[str]) -> None:
    manifest = json.load(manifest_json)

    for entry in manifest["images"]:
        click.echo(
            f"{entry['name']} [{entry['version']}] changes "
            f"from {entry['previous_version']}:\n{entry['changelog']}"
        )


@cli.command(
    name="pipeline-entrypoint",
    help="""Loads/creates a dataset, then applies and launches a pipeline
in a single command. To be used within the command configuration of an XNAT
Container Service ready Docker image.

ADDRESS string containing the nickname of the data store, the ID of the
dataset (e.g. XNAT project ID or file-system directory) and the dataset's name
in the format <store-nickname>//<dataset-id>[@<dataset-name>]

""",
)
@click.argument("address")
@entrypoint_opts.data_columns  # type: ignore[misc]
@entrypoint_opts.parameterisation  # type: ignore[misc]
@entrypoint_opts.execution  # type: ignore[misc]
@entrypoint_opts.dataset_config  # type: ignore[misc]
@entrypoint_opts.debugging  # type: ignore[misc]
def pipeline_entrypoint(
    address: str,
    spec_path: Path,
    command: ty.Optional[str],
    **kwargs: ty.Any,
) -> None:
    image_spec = App.load(spec_path)

    image_spec.command(command).execute(
        address,
        **kwargs,
    )


@cli.group()
def ext() -> None:
    """Command-line group for extension hooks"""
    return None


@cli.command(
    name="bootstrap",
    help="""Generate a YAML specification file for a Pipeline2app App""",
)
@click.argument("output_file", type=click.Path(path_type=Path))
@click.option("--title", "-t", type=str, default=None, help="The title of the image")
@click.option(
    "--docs-url",
    "-u",
    type=str,
    default="https://place-holder.url",
    help="URL explaining the tool/workflow that is being wrapped into an app",
)
@click.option(
    "--registry",
    "-r",
    type=str,
    default="docker.io",
    help="The Docker registry of the image",
)
@click.option(
    "--description",
    "-d",
    type=str,
    default=None,
    help="A longer form description of the tool/workflow implemented in the pipeline",
)
@click.option(
    "--author",
    "-a",
    "authors",
    nargs=2,
    multiple=True,
    type=str,
    metavar="<name> <email>",
    help="The name of the author of the image",
)
@click.option(
    "--base-image",
    "-b",
    type=str,
    nargs=2,
    multiple=True,
    metavar="<attr> <value>",
    help=(
        "Set one of the attributes of the base-image, e.g. '--base-image name debian', "
        "'--base-image package_manager apt', '--base-image tag focal', "
        "'--base-image conda_env base', or '--base-image python /usr/bin/python3.7'"
    ),
)
@click.option(
    "--version", "-v", type=str, default="0.1", help="The version of the image"
)
@click.option(
    "--command-task",
    "-t",
    type=str,
    default=None,
    help="The command to execute in the image",
)
@click.option(
    "--packages-pip",
    "-y",
    type=str,
    multiple=True,
    metavar="<package-name>[==<version>]",
    help="Packages to install via pip",
)
@click.option(
    "--packages-system",
    "-s",
    type=str,
    multiple=True,
    metavar="<package-name>[==<version>]",
    help="Packages to install via the system package manager",
)
@click.option(
    "--packages-neurodocker",
    "-n",
    type=str,
    multiple=True,
    metavar="<package-name>[==<version>]",
    help="Packages to install via NeuroDocker",
)
@click.option(
    "--command-input",
    "-i",
    "command_inputs",
    type=str,
    multiple=True,
    nargs=2,
    metavar="<name> <attrs>",
    help=(
        "Input specifications, name and attribute pairs. Attributes are comma-separated "
        "name/value pairs, e.g. "
        "'datatype=str,configuration.argstr=,configuration.position=0,help=The input image''"
    ),
)
@click.option(
    "--command-output",
    "-o",
    "command_outputs",
    type=str,
    multiple=True,
    nargs=2,
    metavar="<name> <attrs>",
    help=(
        "Output specifications, name and attribute pairs. Attributes are comma-separated "
        "name/value pairs, e.g. "
        "'datatype=str,configuration.argstr=,configuration.position=1,help=The output image'"
    ),
)
@click.option(
    "--command-parameter",
    "-p",
    "command_parameters",
    type=str,
    multiple=True,
    nargs=2,
    metavar="<name> <attrs>",
    help=(
        "Parameter specifications, name and attribute pairs. Attributes are comma-separated "
        "name/value pairs, e.g. 'datatype=str,help='compression level'"
    ),
)
@click.option(
    "--command-configuration",
    "-c",
    type=str,
    multiple=True,
    nargs=2,
    metavar="<name> <value>",
    help="Command configuration value",
)
@click.option(
    "--frequency",
    "-f",
    type=str,
    default="common:Clinical[session]",
    help=(
        "The level in the data tree that the pipeline will operate on, e.g. "
        "common:Clinical[session] designates that the pipeline runs on 'sessions' "
        "as opposed to 'subjects'"
    ),
)
@click.option(
    "--license",
    "-l",
    "licenses",
    nargs=4,
    multiple=True,
    type=str,
    metavar="<license-name> <path-to-license-file> <info-url> <description>",
    help=(
        "Licenses that are required at runtime within the image. The name is used to "
        "refer to the license, when providing a license file at build time or alternatively "
        "installing the license in the data store. The path to the license file is where the "
        "license will be installed within the image. The info URL is where the details of the "
        "license can be found and where it can be acquired from. The description gives a brief "
        "description of the license and what it is required for"
    ),
)
@click.option(
    "--name",
    type=str,
    default="your-command",
    help="The name of the command",
)
def bootstrap(
    output_file: str,
    title: str,
    docs_url: str,
    registry: str,
    authors: ty.List[ty.Tuple[str, str]],
    base_image: ty.List[ty.Tuple[str, str]],
    version: str,
    description: str,
    command_task: str,
    packages_pip: ty.List[str],
    packages_system: ty.List[str],
    packages_neurodocker: ty.List[str],
    command_inputs: ty.List[ty.Tuple[str, str, str]],
    command_outputs: ty.List[ty.Tuple[str, str, str]],
    command_parameters: ty.List[ty.Tuple[str, str, str]],
    command_configuration: ty.List[ty.Tuple[str, str]],
    frequency: str,
    licenses: ty.List[ty.Tuple[str, str, str, str]],
    name: str,
) -> None:

    # Make the output directory if it doesn't exist
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    def unwrap_fields(
        fields: ty.List[ty.Tuple[str, str]]
    ) -> ty.Dict[str, ty.Dict[str, ty.Any]]:
        fields_dict = {}
        for field_name, attrs_str in fields:
            attrs = [re.split(r"(?<!\\)=", a) for a in re.split(r"(?<!\\),", attrs_str)]
            unwrap_attrs = defaultdict(dict)
            for name, value in attrs:
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                if "." in name:
                    parts = name.split(".")
                    dct = unwrap_attrs[parts[0]]
                    for part in parts[1:-1]:
                        dct = dct[part]
                    dct[parts[-1]] = value
                else:
                    unwrap_attrs[name] = value
            unwrap_attrs["help"] = ""
            fields_dict[field_name] = dict(unwrap_attrs)
        return fields_dict

    ver_split_re = re.compile(r">=|<=|==|>|<")

    def split_versions(packages: ty.List[str]) -> ty.Dict[str, ty.Optional[str]]:
        return dict(
            tuple(ver_split_re.split(p, maxsplit=1)) if "=" in p else (p, None)  # type: ignore[misc]
            for p in packages
        )

    spec = {
        "schema_version": App.SCHEMA_VERSION,
        "title": title,
        "version": version,
        "registry": registry,
        "docs": {
            "description": description,
            "info_url": docs_url,
        },
        "authors": [{"name": a[0], "email": a[1]} for a in authors],
        "base_image": dict(base_image),
        "packages": {
            "pip": split_versions(packages_pip),
            "system": split_versions(packages_system),
            "neurodocker": split_versions(packages_neurodocker),
        },
        "commands": {
            name: {
                "task": command_task,
                "row_frequency": frequency,
                "inputs": unwrap_fields(command_inputs),
                "outputs": unwrap_fields(command_outputs),
                "parameters": unwrap_fields(command_parameters),
                "configuration": dict(command_configuration),
            }
        },
        "licenses": {
            lc[0]: {"destination": lc[1], "info_url": lc[2], "description": lc[3]}
            for lc in licenses
        },
    }

    with open(output_file, "w") as f:
        yaml.dump(spec, f)


# Ensure that all sub-packages under CLI are loaded so they are added to the
# base command
extensions = list(submodules(pipeline2app, subpkg="cli"))


if __name__ == "__main__":
    make(sys.argv[1:])
