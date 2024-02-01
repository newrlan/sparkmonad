from pyspark.sql import SparkSession
from SparkMonad.step import Step
from SparkMonad.table_description import TableDescription


class Monad:
    def __init__(self, spark: SparkSession,
                 logger: Logger,
                 *meta):
        self.spark = spark
        self.logger = logger
        self.meta = meta

        # Маппинг укзаывающий из какого Step должна рассчитываться эта таблица.
        self._pipe = dict() # dict[TableDescription, Step]

        # Маппинг указывающий содержащий вычисленные таблицы
        self._dump = dict() # dict[TableDescription, Optional[SparkDF]]

    def _add_calc(self, step: Step):
        for desc in step.output:
            self._pipe[desc] = step

    def get(desc: TableDescription):
        """ Получить вычисленную таблицу из монады. """
        ...


        
