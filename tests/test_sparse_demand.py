from datetime import date,timedelta
from unittest import mock
import joblib
import numpy as np
import pandas as pd
import pytest

from serving.app.sparse_demand import (
    FEATURE_NAMES,SparseDemandRegressor,VolumeCalibratedSparseModel,
    origin_features,statistical_forecast,validate_panel,
)
from scripts.evaluate_volume_calibration import calibration_factor
from serving.app.baseline_models import MovingAverageModel
from serving.app.features import build_feature_row
from serving.app.model_loader import ModelManager,SOURCE_MLFLOW


def test_direct_origin_features_do_not_treat_forecasts_as_observed_sales():
    panel=np.array([[0,0,0,2,0,0,0,0]],float)
    x=origin_features(panel,date(2026,1,8),[1,7,14])
    assert x.shape==(3,len(FEATURE_NAMES))
    np.testing.assert_array_equal(x[:,:19],np.repeat(x[:1,:19],3,axis=0))
    assert x[0,FEATURE_NAMES.index('days_since_positive')]==4
    assert x[0,FEATURE_NAMES.index('observed_age')]==5
    assert x[0,FEATURE_NAMES.index('lifetime_positive_rate')]==pytest.approx(.2)


def test_vectorized_moving_average_matches_existing_corrected_recursive_model():
    history=[{'daily_demand':float(i)} for i in range(1,35)]
    pred=statistical_forecast(np.arange(1,35,dtype=float)[None,:],14)
    model=MovingAverageModel();expected=[]
    for k in range(14):
        row=build_feature_row(history,date(2026,2,3)+timedelta(days=k))
        value=model.predict(pd.DataFrame([row]))[0]
        expected.append(value);history.append({'daily_demand':value})
    np.testing.assert_allclose(pred[0],expected)


def test_tsb_updates_probability_on_observed_zeros_only():
    pred=statistical_forecast(np.array([[0,2,0,0.]]),14,'tsb',alpha=.1,beta=.2)
    np.testing.assert_allclose(pred,np.full((1,14),.64))
    sba=statistical_forecast(np.array([[0,2,0,0.]]),14,'sba',alpha=.1)
    np.testing.assert_allclose(sba,np.full((1,14),.95))


@pytest.mark.parametrize('method',['tsb','sba','mean28','median28','moving_average7'])
def test_all_zero_history_has_nonnegative_zero_forecast(method):
    np.testing.assert_array_equal(statistical_forecast(np.zeros((2,50)),14,method),np.zeros((2,14)))


@pytest.mark.parametrize('panel',[np.array([[np.nan]]),np.array([[-1.]]),np.zeros((0,5)),np.zeros((2,0))])
def test_invalid_panels_raise(panel):
    with pytest.raises(ValueError):validate_panel(panel)


def test_calibration_uses_previous_oof_windows_and_keeps_bounds():
    assert calibration_factor([(np.array([10.]),np.array([20.]))])==.5
    assert calibration_factor([(np.array([100.]),np.array([1.]))])==2
    assert calibration_factor([(np.array([0.]),np.array([1.]))])==1


@pytest.mark.parametrize('objective',['absolute_error','poisson','hurdle'])
def test_real_histogram_training_serialization_and_horizon_contract(objective,tmp_path):
    dates=pd.date_range('2026-01-01',periods=60)
    y=np.array([[1+(k%3) for k in range(60)],[float(k%4==0) for k in range(60)]])
    model=SparseDemandRegressor(objective=objective,n_estimators=5).fit_history(y,dates)
    expected=model.forecast_panel(y,dates[-1],14)
    assert expected.shape==(2,14) and np.isfinite(expected).all() and (expected>=0).all()
    path=tmp_path/'model.joblib';joblib.dump(model,path);loaded=joblib.load(path)
    np.testing.assert_allclose(loaded.forecast_panel(y,dates[-1],14),expected)
    history=[{'date':d.date(),'daily_demand':float(v)} for d,v in zip(dates,y[0])]
    np.testing.assert_allclose(loaded.predict_horizon_from_history(history,14),expected[0])
    with pytest.raises(ValueError):loaded.predict_horizon_from_history(history,15)
    with pytest.raises(ValueError):loaded.predict_horizon_from_history(history[::2],7)


def test_training_requires_contiguous_dates():
    with pytest.raises(ValueError,match='contiguous'):
        SparseDemandRegressor().fit_history(np.ones((1,60)),pd.date_range('2026-01-01',periods=60,freq='2D'))


def test_serving_calls_direct_model_once_and_keeps_small_rates():
    manager=object.__new__(ModelManager)
    model=mock.Mock()
    model.predict_horizon_from_history.return_value=[.01234]*7
    manager.model=model;manager.model_source=SOURCE_MLFLOW
    manager.feature_spec={'feature_columns':list(FEATURE_NAMES)}
    history=[{'date':date(2026,1,1)+timedelta(days=k),'daily_demand':1.,'order_count':1.,
              'avg_unit_price':0.,'daily_revenue':0.,'total_discount':0.} for k in range(60)]
    with mock.patch.object(manager,'_load_history',return_value=(history,history[-1]['date'])):
        result=manager.forecast('A',history[-1]['date']+timedelta(days=1),history[-1]['date']+timedelta(days=7))
    assert result['model_source']==SOURCE_MLFLOW
    assert [i['predicted_quantity'] for i in result['items']]==[.0123]*7
    model.predict_horizon_from_history.assert_called_once()
    assert len(history)==60


def test_serving_flags_invalid_direct_prediction_as_fallback():
    manager=object.__new__(ModelManager);manager.model=mock.Mock();manager.model_source=SOURCE_MLFLOW
    manager.feature_spec={'feature_columns':list(FEATURE_NAMES)}
    manager.model.predict_horizon_from_history.return_value=[float('nan')]*7
    h=[{'date':date(2026,1,1),'daily_demand':1.,'order_count':1.,'avg_unit_price':0.}]
    with mock.patch.object(manager,'_load_history',return_value=(h,date(2026,1,1))):
        result=manager.forecast('A',date(2026,1,2),date(2026,1,8))
    assert result['model_source'].startswith('heuristic')
