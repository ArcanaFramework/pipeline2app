import typing as ty
import re
from typing_extensions import Self


class Version:

    release: ty.Union[ty.Tuple[int, ...], str]
    suffix_label: str
    suffix_number: int

    def __init__(
        self,
        release: ty.Union[ty.Tuple[int, ...], str],
        suffix_label: str = "",
        suffix_number: int = 0,
    ):
        self.release = release
        self.suffix_label = suffix_label
        self.suffix_number = suffix_number

    @classmethod
    def parse(cls, version: str) -> Self:
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

    def compare(self, other: "Version") -> int:
        if (isinstance(self.release, str) and isinstance(other.release, tuple)) or (
            isinstance(self.release, tuple) and isinstance(other.release, str)
        ):
            raise ValueError("Cannot compare versions with different release types")
        if self.release < other.release:  # type: ignore[operator]
            return -1
        if self.release > other.release:  # type: ignore[operator]
            return 1
        if self.suffix_label == "post" and other.suffix_label != "post":
            return 1
        if self.suffix_label != "post" and other.suffix_label == "post":
            return -1
        if self.suffix_label and not other.suffix_label:
            return 1
        if not self.suffix_label and other.suffix_label:
            return -1
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
            r"^v?(?P<release>[a-zA-Z0-9_]+)"
            r"(?P<suffix>-(?P<suffix_l>("
            + "|".join(SUFFIX_LABELS)
            + r"))(?P<suffix_n>[0-9]+)?)?$"
        ),
        re.VERBOSE | re.IGNORECASE,
    )
