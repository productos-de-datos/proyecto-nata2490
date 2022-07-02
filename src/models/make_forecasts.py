"""
Módulo pronóstico final de precios diarios
-------------------------------------------------------------------------------
Construye los pronósticos con el modelo entrenado utilizando el archivo src/models/precios-diarios.pickle, incluyendo las columnas fecha,precio promedio real de la electricidad y pronóstico del precio promedio real y lo salva en data_lake/business/forecasts/precios-diarios.csv

Función load_pkl: se cargan el archivo src/models/precios-diarios.pickle con los datos de entrenamiento
Funcion Score: se realizan varias estimaciones para posteriormente compararlas
Función Best Score: selecciona el mejor estimador con base en la ejecución de la función Score
Función trein_model_with_best_estimator: se entrena el modelo con los estimadores sugeridos
Función prediction_test_model: se predice x_test 
Función forecasts: identifica los indices de y_test y selecciona las columnas de fecha, 
precio y pronóstico
Función save_forecasts: Guarda el pronóstico en la ruta: data_lake/business/forecasts/precios-diarios.csv
"""

from train_daily_model import load_data, data_preparation, make_train_test_split
import numpy as np
import pandas as pd
import pickle


def load_pkl(infile):

    with open(infile, "rb") as file:
        model_RF = pickle.load(file)
    return model_RF


def score(x_train, y_train, x_test, y_test, model_RF):

    estimators = np.arange(10, 200, 10)
    scores = []
    for n in estimators:
        model_RF.set_params(n_estimators=n)
        model_RF.fit(x_train, y_train)
        scores.append(model_RF.score(x_test, y_test))
    return scores


def best_score(scores):
    estimador_n = pd.DataFrame(scores)
    estimador_n.reset_index(inplace=True)
    estimador_n = estimador_n.rename(columns={0: 'scores'})
    estimador_n = estimador_n[estimador_n['scores']
                              == estimador_n['scores'].max()]
    estimador_n['index'] = (estimador_n['index']+1)*10
    estimador_n = estimador_n['index'].iloc[0]

    return estimador_n


def trein_model_with_best_estimator(estimador_n, x_train, y_train):

    from sklearn.ensemble import RandomForestRegressor

    # Entrenamiento del moodelo con los estimadores sugeridos
    model_RF = RandomForestRegressor(
        n_estimators=estimador_n, random_state=12345)
    model_RF.fit(x_train, y_train)
    return model_RF


def prediction_test_model(model_RF, x_test):
    # Prediccion de x_test
    y_pred_RF_testeo = model_RF.predict(x_test)
    return y_pred_RF_testeo


def forecasts(y_pred_RF_testeo, y_test, data):

    df_model_RF = pd.DataFrame(y_test).reset_index(drop=True)
    df_model_RF['pronostico'] = y_pred_RF_testeo

    np.random.seed(0)

    # idenfificar indices de y_test
    # ------------------------------------------
    df_series = pd.Series(y_test)
    df_series = df_series.to_frame()
    df_series.reset_index(inplace=True)
    df_series = df_series[['index']]
    # ------------------------------------------

    df_model_RF = pd.concat([df_model_RF, df_series], axis=1)

    data = data[['fecha']]
    data.reset_index(inplace=True)

    df_model_RF = pd.merge(df_model_RF, data, on='index', how='left')
    df_model_RF = df_model_RF[['fecha', 'precio', 'pronostico']]
    return df_model_RF


def save_forecasts(df_model_RF, outfile):
    df_model_RF.to_csv(outfile, index=None)


def make_forecasts():
    try:
        infile = "src/models/precios-diarios.pickle"
        outfile = 'data_lake/business/forecasts/precios-diarios.csv'
        data = load_data()
        x, y = data_preparation(data)
        x_train, x_test, y_train, y_test = make_train_test_split(x, y)
        model_RF = load_pkl(infile)
        scores = score(x_train, y_train, x_test, y_test, model_RF)
        estimador_n = best_score(scores)
        model_RF = trein_model_with_best_estimator(
            estimador_n, x_train, y_train)
        y_pred_RF_testeo = prediction_test_model(model_RF, x_test)
        df_model_RF = forecasts(y_pred_RF_testeo, y_test, data)
        save_forecasts(df_model_RF, outfile)
    except:
        raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_forecasts()
