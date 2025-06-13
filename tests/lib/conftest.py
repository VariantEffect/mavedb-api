from pathlib import Path
from shutil import copytree


def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"
