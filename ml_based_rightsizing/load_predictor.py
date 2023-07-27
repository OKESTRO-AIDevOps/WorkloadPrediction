from time import time

from pandas import DataFrame
from pandas import Series
from pandas import concat
from pandas import read_csv
from pandas import datetime
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from sklearn.externals import joblib
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from math import sqrt
from numpy import array

import numpy as np
import keras as ks
import pandas as pd


class Predictor:
    """Predict future load given current one
    Just init this class and use predict function to predict
    """

    def __init__(self, init_load, model_path='resource/my_model_32.h5', scaler_path='resource/my_scaler.save', n_out=50):
        self.last_step = init_load
        self.model = ks.models.load_model(model_path)
        self.scaler = joblib.load(scaler_path) 
        self.n_out = n_out
        print('prediction model loaded\n')

    def forecast_lstm(self, X):
        X = X.reshape(1, 1, len(X))
        forecast = self.model.predict(X, batch_size=1)
        # return an array
        return [x for x in forecast[0, :]]