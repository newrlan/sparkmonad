from abc import ABC, abstractmethod
import re
from dataclasses import dataclass, field
import sys
from typing import Any, Dict, List, Optional, Self, Tuple, Callable
import os

from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame as SparkDF
import pyspark.sql.types as T

# from utils import TYPES_MAPPING


@dataclass(frozen=True, eq=True)
class TableDescription:
    """ Описание таблицы.

    Обязательные поля:
        * name - название таблицы
        * _columns - наименования колонок с их типами. Типы могут быть только
        определенных значений

    """
    name: str = field(compare=True)
    _columns: List[Tuple[str, str]] = field(hash=False, compare=True)
    description: str = field(hash=False, default_factory=str)

    def __post_init__(self):
        if len(self.columns) != len(set(self.columns)):
            raise ValueError(f"There are duplicated columns in {self.name}")

        if len(self.columns) == 0:
            raise ValueError(f"Table description {self.name} desn't have any column.")

        # Проверка, что типы указаны корректные
        for col, dtype in self._columns:
            try:
                self._type_cast(dtype)
            except KeyError:
                raise TypeError(f'You try to use inapropriete type {dtype} in column {col} for declaration {self.__class__.__name__}.')

    @staticmethod
    def _type_cast(string_type: str):

        if 'decimal' in string_type:
            v1, v2 = re.findall(r'\d+', string_type)
            return T.DecimalType(int(v1), int(v2))

        types_mapping = {
            'string': T.StringType(),
            'double': T.DoubleType(),
            'int': T.IntegerType(),
            'integer': T.IntegerType(),
            'date': T.DateType(),
            'timestamp': T.TimestampType(),
            'bigint': T.LongType(),
            'smallint': T.ShortType(),
            'boolean': T.BooleanType(),
            'tinyint': T.ByteType()
        }

        return types_mapping[string_type]

    @property
    def columns(self):
        return [col for col, _ in self._columns]

    # TODO: покрыть юнит-тестом
    def to_schema(self):
        return StructType([StructField(col, self._type_cast(dtype), True)
                           for col, dtype in self._columns])

    @property
    def types(self) -> dict[str, str]:
        return {col: ctype for col, ctype in self._columns}

    def is_projection_of(self, tab_desc: Self) -> bool:
        for col, ctype in self.types.items():
            if ctype != tab_desc.types.get(col):
                return False
        return True

    @classmethod    # TODO: доделать!
    def from_schema(cls, struct_type: StructType) -> Self:
        dump = []
        for col in struct_type.fieldNames():
            ctype = struct_type[col].dataType.typeName()
            if ctype == 'integer':
                dump.append((col, int))
            else:
                dump.append((col, ctype))

        return TableDescription('', dump)

    def projection(self, tab: SparkDF) -> SparkDF:
        """ Выбрать только колонки из описания в таблице. """
        # TODO: вернуть ошибку, если описание не соответствует таблице
        return tab.select(*self.columns)
