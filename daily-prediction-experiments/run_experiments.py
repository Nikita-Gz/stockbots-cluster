# 1) gets data as argument
# 2) for each machine learning model:
#   2.1) create a list of hyperparameters using grid search
#   2.2) for each hyperparameter combination:
#     2.2.1) start mlflow experiment
#     2.2.2) train model
#     2.2.3) run predictions
#     2.2.4) compute metrics
#     2.2.5) log metrics
#     2.2.6) end mlflow experiment

import pandas as pd
import numpy as np
import mlflow

from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
from xgboost import XGBRegressor
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsRegressor

# import stuff for hyperparameter tuning
from sklearn.model_selection import RandomizedSearchCV

from data_preparation import prepare_data_for_prediction

random_state = 22

def get_model(model_name):
  # create model
  if model_name == 'linear_regression':
    model = LinearRegression()
  elif model_name == 'random_forest':
    model = RandomForestRegressor(random_state=random_state)
  elif model_name == 'xgboost':
    model = XGBRegressor(random_state=random_state)
  elif model_name == 'naive_bayes':
    model = GaussianNB()
  elif model_name == 'knn':
    model = KNeighborsRegressor()
  elif model_name == 'gradient_boosting':
    model = GradientBoostingRegressor(random_state=random_state)
  else:
    raise Exception(f'Unknown model {model_name}')
  return model


def get_hyperparameter_grid(model_name):
  if model_name == 'linear_regression':
    hyperparameters = {
      'fit_intercept': [True, False],
      'normalize': [True, False],
      'copy_X': [True, False],
    }
  elif model_name == 'random_forest':
    hyperparameters = {
      'n_estimators': [int(x) for x in np.linspace(start=200, stop=2000, num=10)],
      'max_features': ['auto', 'sqrt'],
      'max_depth': [int(x) for x in np.linspace(10, 110, num=11)],
      'min_samples_split': [2, 5, 10],
      'min_samples_leaf': [1, 2, 4],
      'bootstrap': [True, False]
    }
  elif model_name == 'xgboost':
    hyperparameters = {
      'n_estimators': [int(x) for x in np.linspace(start=200, stop=2000, num=10)],
      'max_depth': [int(x) for x in np.linspace(10, 110, num=11)],
      'learning_rate': [0.05, 0.1, 0.15, 0.2, 0.25, 0.3],
      'min_child_weight': [1, 3, 5, 7],
      'gamma': [0.0, 0.1, 0.2, 0.3, 0.4],
      'colsample_bytree': [0.3, 0.4, 0.5, 0.7]
    }
  elif model_name == 'naive_bayes':
    hyperparameters = {
      'var_smoothing': [1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1]
    }
  elif model_name == 'knn':
    hyperparameters = {
      'n_neighbors': [int(x) for x in np.linspace(start=1, stop=20, num=20)],
      'weights': ['uniform', 'distance'],
      'algorithm': ['auto', 'ball_tree', 'kd_tree', 'brute'],
      'leaf_size': [int(x) for x in np.linspace(start=1, stop=50, num=50)]
    }
  elif model_name == 'gradient_boosting':
    hyperparameters = {
      'n_estimators': [int(x) for x in np.linspace(start=200, stop=2000, num=10)],
      'learning_rate': [0.05, 0.1, 0.15, 0.2],
      'max_depth': [int(x) for x in np.linspace(10, 110, num=11)],
      'min_samples_split': [2, 5, 10],
      'min_samples_leaf': [1, 2, 4],
      'max_features': ['auto', 'sqrt', 'log2']
    }
  else:
    raise Exception(f'Unknown model {model_name}')
  return hyperparameters


def run_experiment(preprocessed_prices_dataframe):
  mlflow.set_tracking_uri("http://mlflow-tracking:5000")
  mlflow.set_experiment("daily_price_prediction")

  # split data into train and test
  test_size = 0.2
  train, test = train_test_split(preprocessed_prices_dataframe, test_size=test_size, random_state=random_state)
  X_train = train.drop(columns=['c_diff'])
  y_train = train['c_diff']
  X_test = test.drop(columns=['c_diff'])
  y_test = test['c_diff']

  models_to_run = ['linear_regression', 'random_forest', 'xgboost', 'naive_bayes', 'knn', 'gradient_boosting']
  for model_name in models_to_run:
    with mlflow.start_run():
      print(f'Running model {model_name}')

      model = get_model(model_name)
      hyperparameters = get_hyperparameter_grid(model_name)

      # create random search
      random_search = RandomizedSearchCV(estimator=model, param_distributions=hyperparameters, n_iter=100, cv=5, verbose=2, random_state=random_state, n_jobs=-1)

      # run random search
      random_search.fit(X_train, y_train)

      # get best model  
      best_model = random_search.best_estimator_

      # evaluate model
      print('Evaluating model')
      y_pred = best_model.predict(X_test)
      r2 = r2_score(y_test, y_pred)
      mse = mean_squared_error(y_test, y_pred)
      mae = mean_absolute_error(y_test, y_pred)
      print(f'R2: {r2}')
      print(f'MSE: {mse}')
      print(f'MAE: {mae}')

      # log model
      mlflow.log_param('model_name', model_name)
      mlflow.log_param('test_size', test_size)
      mlflow.log_param('random_state', random_state)
      mlflow.log_metric('r2', r2)
      mlflow.log_metric('mse', mse)
      mlflow.log_metric('mae', mae)

      # log each hyperparameter
      for hyperparameter in hyperparameters:
        mlflow.log_param(hyperparameter, best_model.get_params()[hyperparameter])
