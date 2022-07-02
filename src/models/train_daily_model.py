"""
Módulo de entrenamiento del pronóstico de precios diarios.
-------------------------------------------------------------------------------
Entrena el modelo de los promedios de precios diarios a partir de un random forest y lo salva en models/precios-diarios.pkl. Para ello se definen las funciones:

Función load_data: se cargan los datos de entrada que usará el modelo

Función data_preparation: Se preparan los datos según el formato necesario y se definen las variables del modelo.

Función make_trein_test_split: Parte los datos de entrenamiento y prueba con un test_size de 0.25 para prueba y una semilla o random_state de 12345, retornando las variables x_train, x_test, y_train, y_test

Función train_model: Se entrena el modelo con los datos de entrenamiento

Función save_model: Guarda el modelo en la ruta src/models/precios-diarios.pickle

Función train_daily_model: Orquesta la ejecución de las funciones previamente construídas load_data, data_preparation, make_train_test_split, trein_model y save_model
"""


def load_data():

    import pandas as pd

    in_path = 'data_lake/business/features/precios_diarios.csv'
    data = pd.read_csv(in_path, sep=",")

    return data


def data_preparation(data):
    import pandas as pd
    df = data.copy()
    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')
    df['year'], df['month'], df['day'] = df['fecha'].dt.year, df['fecha'].dt.month, df['fecha'].dt.day

    y = df["precio"]
    x = df.copy()
    x.pop("precio")
    x.pop("fecha")
    return x, y


def make_train_test_split(x, y):

    from sklearn.model_selection import train_test_split

    (x_train, x_test, y_train, y_test) = train_test_split(
        x,
        y,
        test_size=0.25,
        random_state=12345,
    )
    return x_train, x_test, y_train, y_test


def trein_model(x_train, x_test):
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor

    # Se Define el algoritmo a utilizar
    scaler = StandardScaler()
    scaler.fit(x_train)
    x_train = scaler.transform(x_train)
    x_test = scaler.transform(x_test)

    # Se establece el Modelo
    model_RF = RandomForestRegressor(n_jobs=-1)

    return model_RF


def save_model(model_RF):

    import pickle

    with open("src/models/precios-diarios.pickle", "wb") as file:
        pickle.dump(model_RF, file,  pickle.HIGHEST_PROTOCOL)


def train_daily_model():
    try:
        data = load_data()
        x, y = data_preparation(data)
        x_train, x_test, y_train, y_test = make_train_test_split(x, y)
        model_RF = trein_model(x_train, x_test)
        save_model(model_RF)
    except:
        raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    train_daily_model()
