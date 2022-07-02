"""
Módulo limpieza de datos
-------------------------------------------------------------------------------
Función load_data: Lee los archivos cvs que se encuentran en la ruta data_lake/raw/ 
y los concatena en un solo archivo, elimina registros con nulos en la variable fecha

Función transfort_data: Reordena los precios por hora y por fecha: fecha, hora, precio

Función save_data: Guarda los datos en el archivo data_lake/cleansed/precios-horarios.csv

Función clean_data: Orquesta las anteriores funciones con el fin de concatenar 
en un solo archivo los csv de la ruta data_lake/raw/ y los guarda en el archivo 
precios-horarios.csv en la ruta data_lake/raw/ garantizando que la columna fecha 
y precio no contengan datos nulos

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

"""
import pandas as pd
import glob


def load_data(in_file):

    path_file = glob.glob(in_file)

    lista_archivos = []
    for filename in path_file:
        df = pd.read_csv(filename, index_col=None, header=0)
        lista_archivos.append(df)

    read_file = pd.concat(lista_archivos, axis=0, ignore_index=True)
    read_file = read_file[read_file["Fecha"].notnull()]

    return read_file


def transform_data(read_file):
    '''
    >>> transform_data(pd.DataFrame({'Fecha':('2021-06-02','2021-03-02','2021-01-12','2021-01-03'),'h0':(12,13,14,15),'h1':(16,17,14,15),'h2':(12,13,14,15)})).head(20)['precio'].tolist()
    [12, 13, 14, 15, 16, 17, 14, 15, 12, 13, 14, 15]
    '''
    data = pd.melt(read_file, id_vars=['Fecha'])
    data = data.rename(
        columns={'Fecha': 'fecha', 'variable': 'hora', 'value': 'precio'})
    data = data[data["precio"].notnull()]
    return data


def save_data(data, out_file):

    data.to_csv(out_file, index=None, header=True)


def clean_data():
    try:
        in_file = r'data_lake/raw/*.csv'
        out_file = "data_lake/cleansed/precios-horarios.csv"
        read_file = load_data(in_file)
        data = transform_data(read_file)
        save_data(data, out_file)

    except:
        raise NotImplementedError("Implementar esta función")


def test_columns_dataframe():
    dic = {
        'Fecha': ('2021-06-02', '2021-03-02', '2021-01-12', '2021-01-03'),
        'h0': (12, 13, 14, 15),
        'h1': (16, 17, 14, 15),
        'h2': (12, 13, 14, 15)
    }
    df = pd.DataFrame(dic)
    expect = [12, 13, 14, 15, 16, 17, 14, 15, 12, 13, 14, 15]
    assert transform_data(df).tail(20)['precio'].tolist() == expect


if __name__ == "__main__":

    import doctest

    doctest.testmod()
    clean_data()
