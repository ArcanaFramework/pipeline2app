from __future__ import annotations
import typing as ty
import attrs
from frametree.core.serialize import (
    ObjectConverter,
    ClassResolver,
)
from frametree.core.pipeline import (
    PipelineField,
)
from frametree.core.row import DataRow
from fileformats.core import DataType, Field
from fileformats.core.exceptions import FormatMismatchError
from frametree.core.axes import Axes
from frametree.core.utils import add_exc_note


@attrs.define
class ColumnDefaults:
    """
    Values to set the default column of a command input/output

    Parameters
    ----------
    datatype : str
        the type the data items will be stored in (e.g. file-format)
    row_frequency : Axes
        the "row-frequency" of the input column to be added
    path : str
        path to where the data will be placed in the repository
    """

    datatype: ty.Type[DataType] = attrs.field(
        default=None,
        converter=ClassResolver(  # type: ignore[misc]
            DataType,
            allow_none=True,
            alternative_types=[DataRow],
        ),
    )
    row_frequency: ty.Optional[Axes] = None
    path: ty.Optional[str] = None


@attrs.define(kw_only=True)
class CommandField(PipelineField):
    """An input/output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input, fileformats.generic.File by default
    help : str
        short description of the field to be displayed in the UI
    configuration : dict[str, Any] or bool
        Arguments that should be passed onto the the command configuration dict in
        special the ``inputs``, ``outputs`` or ``parameters`` input fields for the given
        field alongside the name and datatype. Should be set to True if just the name
        and dataset
    """

    help: str
    configuration: ty.Union[ty.Dict[str, ty.Any], bool] = attrs.field(factory=dict)

    @property
    def config_dict(self) -> ty.Dict[str, ty.Any]:
        """Returns a dictionary to be passed to the task/workflow in order to configure
        it to receive input/output

        Parameters
        ----------
        configuration : _type_
            _description_
        list_name : _type_
            _description_
        """
        if self.configuration:
            config = {
                "name": self.name,
                "datatype": self.datatype,
            }
            if isinstance(self.configuration, dict):  # Otherwise just True
                config.update(self.configuration)
        else:
            config = {}
        return config


@attrs.define(kw_only=True)
class CommandInput(CommandField):
    """Defines an input or output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help : str
        description of the input/output field
    configuration : dict
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `inputs` input field to the task/workflow
    column_defaults: ColumnDefaults, optional
        the values to use to configure a default column if the name doesn't match an
        existing column
    """

    column_defaults: ColumnDefaults = attrs.field(
        converter=ObjectConverter(
            ColumnDefaults, allow_none=True, default_if_none=ColumnDefaults
        ),
        default=None,
    )

    @column_defaults.validator
    def column_validator(self, _: ty.Any, column: ColumnDefaults) -> None:
        if (
            column.datatype is not None
            and self.datatype is not column.datatype
            and not isinstance(
                self.datatype, str
            )  # if has fallen back to string in non-build envs
        ):
            try:
                self.datatype.get_converter(column.datatype)
            except FormatMismatchError as e:
                add_exc_note(
                    e,
                    f"required to convert from the default column to the '{self.name}' input",
                )
                raise

    def __attrs_post_init__(self) -> None:
        if self.column_defaults.datatype is None:
            self.column_defaults.datatype = self.datatype


@attrs.define(kw_only=True)
class CommandOutput(CommandField):
    """Defines an input or output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help : str
        description of the input/output field
    configuration : dict
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `outputs` input field to the task/workflow
    default_columm: DefaultColumn, optional
        the values to use to configure a default column if the name doesn't match an
        existing column
    """

    column_defaults: ColumnDefaults = attrs.field(
        converter=ObjectConverter(  # type: ignore[misc]
            ColumnDefaults, allow_none=True, default_if_none=ColumnDefaults
        ),
        default=None,
    )

    @column_defaults.validator
    def column_defaults_validator(
        self, _: ty.Any, column_defaults: ColumnDefaults
    ) -> None:
        if (
            column_defaults.datatype is not None
            and self.datatype is not column_defaults.datatype
        ):
            try:
                column_defaults.datatype.get_converter(self.datatype)
            except FormatMismatchError as e:
                add_exc_note(
                    e,
                    f"required to convert to the default column from the '{self.name}' output",
                )
                raise

    def __attrs_post_init__(self) -> None:
        if self.column_defaults.datatype is None:
            self.column_defaults.datatype = self.datatype


def dtype_converter(
    dtype: ty.Union[int, float, bool, str, Field[ty.Any, ty.Any]]
) -> ty.Union[int, float, bool, str]:
    datatype = ClassResolver(ty.Union[int, float, bool, str, Field])(dtype)
    if issubclass(datatype, Field):
        datatype = datatype.primitive
    return datatype  # type: ignore[no-any-return]


@attrs.define(kw_only=True)
class CommandParameter(CommandField):
    """Defines a fixed parameter of the task/workflow/analysis to be exposed in the UI

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help : str
        description of the input/output field
    configuration : dict[str, Any]
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `parameters` input field to the task/workflow
    required : bool
        whether the parameter is required or not
    default : Any
        the default value for the parameter, must be able to be
    """

    datatype: ty.Union[int, float, bool, str] = attrs.field(converter=dtype_converter)
    required: bool = False
    default: ty.Any = None
