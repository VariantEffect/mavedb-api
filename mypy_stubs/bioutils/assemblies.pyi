from typing import Union

def get_assembly_names() -> list[str]: ...
def get_assembly(name: str) -> dict[str, Union[str, int, list[str]]]: ...
def get_assemblies(names: list[str] = []) -> dict[str, dict[str, Union[str, int, list[str]]]]: ...
def make_name_ac_map(assy_name: str, primary_only: bool = False) -> dict[str, str]: ...
def make_ac_name_map(assy_name: str, primary_only: bool = False) -> dict[str, str]: ...
