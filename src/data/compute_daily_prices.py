"""
Módulo precios promedio diarios
-------------------------------------------------------------------------------
Del archivo data_lake/cleansed/precios-horarios.csv se transforma la columna fecha 
en formato datetime, y luego se genera por cada fecha el precio promedio diario en la
ruta data_lake/business/precios-diarios.csv.

Las columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional

"""
import pandas as pd

def load_data(input):

    file = pd.read_csv(input, index_col=None, header=0)
    return file


def average_daily_price(file):
    '''
    El presente caso de uso permite validar que la función computa el promedio del precio por cada fecha
    >>> list(average_daily_price(pd.DataFrame({'fecha': ('2021-06-02', '2021-03-02', '2021-06-02', '2021-03-02'),'precio': (12, 40, 12, 80)})).precio)
    [60.0, 12.0]
    '''
    subset_file = file.copy()

    subset_file["fecha"] = pd.to_datetime(subset_file["fecha"])
    compute_daily_prices = subset_file.groupby(
        "fecha").mean({"precio": "precio"})
    compute_daily_prices.reset_index(inplace=True)
    subset_file = subset_file[["fecha", "precio"]]

    return compute_daily_prices


def save_data(compute_daily_prices, output):
    compute_daily_prices.to_csv(output, index=None, header=True)


def compute_daily_prices():
    try:
        input = "data_lake/cleansed/precios-horarios.csv"
        output = "data_lake/business/precios-diarios.csv"
        file = load_data(input)
        compute_daily_prices = average_daily_price(file)
        save_data(compute_daily_prices, output)
    except:
        raise NotImplementedError("Implementar esta función")


def test_values_compute_daily_prices():
    df = pd.DataFrame({
        'fecha': ('2021-06-02', '2021-03-02', '2021-06-02', '2021-03-02'),
        'precio': (12, 40, 12, 80)
    })
    expect = [60.0, 12.0]
    assert list(average_daily_price(df).precio) == expect


if __name__ == "__main__":

    import doctest

    doctest.testmod()
    compute_daily_prices()
