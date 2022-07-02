"""
Módulo de Orquestación
-------------------------------------------------------------------------------
Orquesta la ejecución de las funciones previamente construídas mediante la 
librería Luigi, con la cual se definen las clases con sus requerimientos y 
salidas. Las clases definidas son: ingestar_data, transformar_data, 
limpiar_data,computar_precio_diario y computar_precio_mensual

"""
import luigi
from luigi import Task, LocalTarget
class ingestar_data(Task):
    def output(self):
        return LocalTarget('data_lake/landing/arc.csv')

    def run(self):

        from ingest_data import ingest_data
        with self.output().open('w') as archivos:
            ingest_data()


class transformar_data(Task):
    def requires(self):
        return ingestar_data()

    def output(self):
        return LocalTarget('data_lake/raw/arc.txt')

    def run(self):

        from transform_data import transform_data
        with self.output().open('w') as archivos:
            transform_data()


class limpiar_data(Task):
    def requires(self):
        return transformar_data()

    def output(self):
        return LocalTarget('data_lake/cleansed/arc.txt')

    def run(self):

        from clean_data import clean_data
        with self.output().open('w') as archivos:
            clean_data()


class computar_precio_diario(Task):
    def requires(self):
        return limpiar_data()

    def output(self):
        return LocalTarget('data_lake/business/arc.txt')

    def run(self):

        from compute_daily_prices import compute_daily_prices
        with self.output().open('w') as archivos:
            compute_daily_prices()


class computar_precio_mensual(Task):
    def requires(self):
        return computar_precio_diario()

    def output(self):
        return LocalTarget('data_lake/business/arc.txt')

    def run(self):

        from compute_monthly_prices import compute_monthly_prices
        with self.output().open('w') as archivos:
            compute_monthly_prices()


if __name__ == '__main__':
    try:

        import doctest
        doctest.testmod()

        luigi.run(["computar_precio_mensual", "--local-scheduler"])

    except:
        raise NotImplementedError("Implementar el orquestador de luigi")
