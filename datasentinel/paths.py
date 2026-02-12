import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PathResolver:
    results_home: Optional[str]
    input_home: Optional[str]

    @classmethod
    def from_env(cls) -> "PathResolver":
        results_home = os.getenv("SENTINEL_HOME")
        input_home = os.getenv("SENTINEL_INPUT_HOME")
        if results_home:
            results_home = os.path.abspath(os.path.expanduser(results_home))
        if input_home:
            input_home = os.path.abspath(os.path.expanduser(input_home))
        return cls(results_home=results_home, input_home=input_home)

    def resolve_input(self, path: Optional[str], *, allow_cwd_fallback: bool = False) -> Optional[str]:
        if not path:
            return path
        if os.path.isabs(path):
            return path
        if self.input_home:
            return os.path.join(self.input_home, path)
        if allow_cwd_fallback:
            return os.path.abspath(path)
        raise ValueError(
            "Relative input path provided but SENTINEL_INPUT_HOME is not set. "
            "Set SENTINEL_INPUT_HOME or use an absolute path."
        )

    def output_base_dir(self) -> str:
        if self.results_home:
            return self.results_home
        return os.getcwd()
