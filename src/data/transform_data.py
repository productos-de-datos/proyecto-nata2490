"""
Módulo transformación de la data
-------------------------------------------------------------------------------
Función ruta: De la ruta data_lake/landing/ se toma el año y la extensión de los
archivos que se encuentran en dicha ruta

Función load_data: Carga los archivos de la ruta data_lake/landing/ diferenciando la 
columna fecha en formato YYYY-MM-DD y las horas desde H00 a H23

Función save_data: Guarda los archivos en la ruta data_lake/raw/ en formato csv

Función transform_data: Orquesta las funciones de ruta, load_data, save_data y transform_data 
con el fin de de transformar a csv los archivos de la ruta data_lake/landing/ y pegarlos a la 
ruta data_lake/raw/ con las columnas fecha en formato YYYY-MM-DD y las horas desde H00 a H23

"""
import pandas as pd


def ruta(year, extension):
    '''
    >>> print(type(ruta('2021', "xlsx")))
    <class 'str'>
    '''
    ruta = "data_lake/landing/{}.{}".format(year, extension)
    return ruta


def load_data(ruta, encabezado):
    read_file = pd.read_excel(ruta, header=encabezado)
    read_file = read_file.iloc[:, 0:25]
    read_file.columns = ['Fecha', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
                         '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']
    return read_file


def save_data(read_file, year):
    read_file.to_csv("data_lake/raw/{}.csv".format(year), index=None)


def transform_data():
    try:
        for year in range(1995, 2022):
            if year in range(1995, 2000):
                out_file = ruta(year, "xlsx")
                file = load_data(out_file, 3)
                save_data(file, year)
            elif(year in range(2000, 2016)):
                out_file = ruta(year, "xlsx")
                file = load_data(out_file, 2)
                save_data(file, year)
            elif(year in range(2016, 2018)):
                out_file = ruta(year, "xls")
                file = load_data(out_file, 2)
                save_data(file, year)
            else:
                out_file = ruta(year, "xlsx")
                file = load_data(out_file, 0)
                save_data(file, year)
    except:
        raise NotImplementedError("Implementar esta función")


def test_answer():
    assert ruta('2021', "xlsx") == "data_lake/landing/2021.xlsx"


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    transform_data()
