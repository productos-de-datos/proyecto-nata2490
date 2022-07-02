def make_monthly_prices_plot():
    """Crea un grafico de lines que representa los precios promedios diarios.

    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.

    El archivo se debe salvar en formato PNG en data_lake/business/reports/figures/daily_prices.png.

    """
    import pandas as pd
    import matplotlib.pyplot as plt
    path_file = r'data_lake/business/precios-mensuales.csv'
    datos = pd.read_csv(path_file, index_col=None, sep=',', header=0)
    datos["fecha"] = pd.to_datetime(datos["fecha"])
    x = datos.fecha
    y = datos.precio

    plt.figure(figsize=(15, 6))
    plt.plot(x, y, 'b', label='Promedio Mensual')
    plt.title('Promedio Mensual')
    plt.xlabel('Fecha')
    plt.ylabel('Precio')
    plt.legend()
    plt.xticks(rotation="vertical")
    plt.savefig("data_lake/business/reports/figures/monthly_prices.png")

    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":

    import doctest

    doctest.testmod()
    make_monthly_prices_plot()
