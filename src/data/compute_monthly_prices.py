"""
Módulo precios promedio mensuales
-------------------------------------------------------------------------------
Del archivo data_lake/cleansed/precios-horarios.csv se transforma la columna fecha 
en formato datetime, se extrae el año y mes y se genera por cada mes del año el precio 
promedio mensual en la ruta data_lake/business/precios-mensuales.csv
Las columnas del archivo generado son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional

"""
import pandas as pd

def load_data(infile):

    file = pd.read_csv(infile, index_col=None, header=0)

    return file


def average_monthly_price(file):
    '''
    El presente caso de uso permite validar que la función compute el precio 
    promedio mensual relacionado con la fecha máxima de cada mes
    >>> list(average_monthly_price(pd.DataFrame({'fecha': ('2021-06-02', '2021-06-05', '2021-06-02', '2021-03-02'),'precio': (20, 40, 30, 80)})).precio)
    [80.0, 30.0]
    '''

    file_copy = file.copy()

    file_copy["fecha"] = pd.to_datetime(file_copy["fecha"])

    file_copy['month'] = file_copy['fecha'].dt.month
    file_copy['year'] = file_copy['fecha'].dt.year

    compute_month_prices = file_copy.groupby(
        ['year', 'month']).mean({'precio': 'precios'})
    compute_month_prices.reset_index(inplace=True)

    file_copy = file_copy.groupby(['year', 'month']).agg({'fecha': 'max'})
    file_copy.reset_index(inplace=True)

    compute_month_prices = file_copy.merge(compute_month_prices, left_on=[
                                           'year', 'month'], right_on=['year', 'month'])
    compute_month_prices = compute_month_prices[['fecha', 'precio']]

    return compute_month_prices


def save_data(compute_month_prices, outfile):
    compute_month_prices.to_csv(outfile, index=None, header=True)


def compute_monthly_prices():
    try:
        infile = "data_lake/cleansed/precios-horarios.csv"
        outfile = "data_lake/business/precios-mensuales.csv"
        file = load_data(infile)
        compute_month_prices = average_monthly_price(file)
        save_data(compute_month_prices, outfile)
    except:
        raise NotImplementedError("Implementar esta función")


def test_values_compute_daily_prices():
    df = pd.DataFrame({
        'fecha': ('2021-06-02', '2021-06-05', '2021-06-02', '2021-03-02'),
        'precio': (20, 40, 30, 80)
    })
    expect = [80.0, 30.0]
    assert list(average_monthly_price(df).precio) == expect


if __name__ == "__main__":

    import doctest

    doctest.testmod()
    compute_monthly_prices()
