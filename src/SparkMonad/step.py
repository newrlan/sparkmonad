from dataclasses import dataclass
from typing import Any, Callable, List

from pyspark.sql import DataFrame as SparkDF

# from ETL.modules.module_base import StepBase, SqlOnlyImportBase
from SparkMonad.table_description import TableDescription


@dataclass(frozen=True)
class Step:
    input: List[TableDescription]
    output: List[TableDescription]
    func: Callable # [[...], SparkDF] # Указать корректную типизацию

    def __call__(self, *tabs: SparkDF, **meta: Any) -> List[SparkDF]:
        # TODO: Если есть логгер в meta залогировать, что в таблицах будут взяты проекции
        new_args = [desc.projection(tab) for desc, tab in zip(self.input, tabs)]
        res = self.func(*new_args, **meta)
        return [desc.projection(tab) for desc, tab in zip(self.output, res)]

    # def step(self, *args, **kwargs):

    #     source_tables = {
    #         tab_desc.name: {
    #             'link': 'argument',
    #             'description': tab_desc.description,
    #             'columns': tab_desc._columns
    #         }
    #         for tab_desc in self.input
    #     }

    #     output_tables = {
    #         self.output.name: {
    #             'link': 'argument',     # TODO: что тут нужно поместить?
    #             'description': self.output.description,
    #             'columns': self.output._columns
    #         }
    #     }

    #     def _calculate(cl):
    #         # TODO: Это место для логирования вызова функции
    #         tabs = [
    #             cl.source_tables.get(tab_desc.name)
    #             for tab_desc in self.input
    #         ]
    #         res = self.func(
    #             *tabs, spark=cl.spark, logger=cl.logger, config=cl.config
    #         )
    #         return {self.output.name: res}

    #     new_class_name = 'Step_' + self.func.__name__

    #     args = {'source_tables': source_tables,
    #             'output_tables': output_tables,
    #             '_calculate': _calculate
    #             }
    #     return type(new_class_name, (StepBase,), args)  # It is a new class!


def signature(input: List[TableDescription], output: List[TableDescription]):
    return lambda func: Step(input, output, func)
