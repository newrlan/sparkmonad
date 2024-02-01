from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import sys
from typing import Any, Dict, List, Optional, Self, Tuple, Callable
import os


from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame as SparkDF

from ETL.modules.module_base import StepBase, SqlOnlyImportBase

from utils import TYPES_MAPPING


# на link повесить функцию генерации полного имени в БД, на вход будет
# приходить флаг in_prod=False, указывающий на путь в продовой или тесовой базе

@dataclass(frozen=True, eq=True)
class TableDescription:
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
                TYPES_MAPPING(dtype)
            except KeyError:
                raise TypeError(f'You try to use inapropriete type {dtype} in column {col} for declaration {self.__class__.__name__}.')

    @property
    def columns(self):
        return [col for col, _ in self._columns]

    # TODO: покрыть юнит-тестом
    def to_schema(self): # old: get_schema
        return StructType([StructField(col, TYPES_MAPPING(dtype), True)
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


@dataclass(frozen=True)
class Step:
    input: List[TableDescription]
    output: List[TableDescription]
    func: Callable # [[...], SparkDF] # Указать корректную типизацию

    def __call__(self, *tabs: SparkDF, **meta: Any) -> SparkDF:
        # TODO: Если есть логгер в meta залогировать, что в таблицах будут взяты проекции
        new_args = [desc.projection(tab) for desc, tab in zip(self.input, tabs)]
        res = self.func(*new_args, **meta)
        return self.output.projection(res)

    def step(self, *args, **kwargs):

        source_tables = {
            tab_desc.name: {
                'link': 'argument',
                'description': tab_desc.description,
                'columns': tab_desc._columns
            }
            for tab_desc in self.input
        }

        output_tables = {
            self.output.name: {
                'link': 'argument',     # TODO: что тут нужно поместить?
                'description': self.output.description,
                'columns': self.output._columns
            }
        }

        def _calculate(cl):
            # TODO: Это место для логирования вызова функции
            tabs = [
                cl.source_tables.get(tab_desc.name)
                for tab_desc in self.input
            ]
            res = self.func(
                *tabs, spark=cl.spark, logger=cl.logger, config=cl.config
            )
            return {self.output.name: res}

        new_class_name = 'Step_' + self.func.__name__

        args = {'source_tables': source_tables,
                'output_tables': output_tables,
                '_calculate': _calculate
                }
        return type(new_class_name, (StepBase,), args)  # It is a new class!


def signature(input: List[TableDescription], output: List[TableDescription]):
    return lambda func: Step(input, output, func)


@dataclass(frozen=True)
class Pipe:
    input: List[TableDescription]
    output: List[TableDescription]
    pipe: List[Step]    # Расширить, чтобы можно было составлять еще и Pipe от Pipe'ов

    def __post_init__(self):
        """ Проверка по сигнатурам Step'ов, что Pipe вычислим. """

        known_tabs = set(self.input)
        for step in self.pipe:
            for input in step.input:
                if input not in known_tabs:
                    raise TypeError(f"Not enough data for calc step {step.__class__.__name__}")
            known_tabs.add(step.output)

        for output in self.output:
            if output not in known_tabs:
                raise TypeError(f"Not enough data for calc output table {output.name}")

    def __call__(self, *input: SparkDF, **meta: Any) -> List[SparkDF]:
        collection = {inp.description: inp for inp in input}

        for step in self.pipe:
            local_input = [collection[tab] for tab in step.input]
            collection[step.output] = step(*local_input, **meta)

        return [collection[tab] for tab in self.output]
