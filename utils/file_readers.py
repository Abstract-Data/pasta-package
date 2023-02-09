import csv
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Generator, Callable
import openpyxl
import pandas as pd
from pandas import DataFrame
import numpy as np


@dataclass
class FileTypeSniffer:
    """
    Sniffs the file type
    """
    path: Path = field(default_factory=Path)
    with_pandas: bool = False

    def __repr__(self):
        return f"{self.__class__.__name__}({self.path.name})"


@dataclass
class CSVReader:
    """
    Reads CSV files
    """
    file: FileTypeSniffer
    data: Generator = field(init=False)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file.path.name})"

    def read(self):
        f = open(self.file.path, "r")
        for row in csv.DictReader(f):
            self.data = yield row
        return self.data

    def to_dataframe(self, **kwargs):
        self.data = pd.read_csv(self.file.path, kwargs)
        return self.data


@dataclass
class XLSXReader:
    """
    Reads XLSX files
    """
    file: FileTypeSniffer
    data: Dict = field(init=False)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file.path.name})"

    def read(self, **kwargs):
        excel = openpyxl.load_workbook(self.file.path)
        reader = excel.active
        columns = reader[kwargs.get("column_row_number", 1)]
        count = 0
        file_dict: Dict = dict()
        for row in reader.iter_rows(values_only=True, min_row=kwargs.get("min_row", 2)):
            r = dict()
            for c in columns:
                for v in row:
                    r[c] = v
            file_dict.update(r)
        self.data = file_dict
        return self.data

    def to_dataframe(self, **kwargs):
        self.data = pd.read_excel(self.file.path, engine="openpyxl", **kwargs)
        self.data.columns = self.data.columns.str.lower()
        self.data.columns = self.data.columns.str.strip()
        self.data.columns = self.data.columns.str.replace(" ", "_").str.replace(
            "\n", "_"
        )
        self.data.columns = self.data.columns.str.replace("__", "_")
        return self.data


@dataclass
class FileHandler:
    """
    Handles file reading
    """
    _file: Path or str
    data: Generator | pd.DataFrame | Dict = field(init=False)
    kwargs: Dict = field(default_factory=dict)

    @property
    def file(self):
        return FileTypeSniffer(self._file)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._file.name})"

    def read(self, using_pandas=False, **kwargs):
        if self.file.path.suffix == ".csv":
            data = CSVReader(self.file)
        elif self.file.path.suffix == ".xlsx":
            data = XLSXReader(self.file)
        else:
            raise ValueError("File type not supported.")

        if using_pandas:
            self.data = data.to_dataframe(**kwargs)
        else:
            self.data = data.read()
        return self.data

    def convert_nulls(self) -> pd.DataFrame:
        if not isinstance(self.data, pd.DataFrame):
            raise ValueError("Data is not a pandas DataFrame.")

        self.data: DataFrame = self.data.astype(object).where(
            pd.notnull(self.data), None
        )
        return self.data
