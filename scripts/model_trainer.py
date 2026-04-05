#!/usr/bin/env python3
"""
Simple Tabular Model Trainer

Provides a simple interface for training models on tabular datasets.
Designed to be easily extended with new model types and preprocessing.

**Who should run this:** Only when the **dataset-creation** skill optional
model-training step is requested—import or run from that workflow, not as a
default agent action. There is no separate trainer agent; keep usage tied to an
enriched dataset produced by the pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Literal
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, mean_squared_error, r2_score
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleModelTrainer:
    """
    Simple model trainer for tabular data.
    
    Supports:
    - Classification (random_forest, logistic_regression)
    - Regression (random_forest, linear_regression)
    
    Easily extendable by:
    1. Adding new model types to _get_model()
    2. Adding new preprocessing to _preprocess_data()
    3. Adding new metrics to _evaluate()
    """
    
    def __init__(
        self,
        task_type: Literal['classification', 'regression'],
        model_type: str = 'random_forest'
    ):
        """
        Initialize trainer.
        
        Args:
            task_type: 'classification' or 'regression'
            model_type: Model to use (random_forest, logistic_regression, linear_regression)
        """
        self.task_type = task_type
        self.model_type = model_type
        self.model = None
        self.label_encoders = {}
        self.feature_scaler = None
        
    def _get_model(self):
        """Get model instance based on type."""
        if self.task_type == 'classification':
            if self.model_type == 'random_forest':
                return RandomForestClassifier(n_estimators=100, random_state=42)
            elif self.model_type == 'logistic_regression':
                return LogisticRegression(max_iter=1000, random_state=42)
            else:
                raise ValueError(f"Unknown classification model: {self.model_type}")
        else:  # regression
            if self.model_type == 'random_forest':
                return RandomForestRegressor(n_estimators=100, random_state=42)
            elif self.model_type == 'linear_regression':
                return LinearRegression()
            else:
                raise ValueError(f"Unknown regression model: {self.model_type}")
    
    def _preprocess_data(
        self,
        df: pd.DataFrame,
        feature_columns: List[str],
        target_column: str,
        fit_encoders: bool = True
    ) -> tuple:
        """
        Preprocess data for training.
        
        Args:
            df: Input dataframe
            feature_columns: Column names to use as features
            target_column: Column name to predict
            fit_encoders: Whether to fit encoders (True for train, False for test)
            
        Returns:
            (X, y) arrays ready for sklearn
        """
        # Drop rows with missing target
        df_clean = df.dropna(subset=[target_column]).copy()
        
        # Extract target
        y = df_clean[target_column].values
        
        # Extract features
        X_df = df_clean[feature_columns].copy()
        
        # Handle categorical features
        for col in feature_columns:
            if X_df[col].dtype == 'object' or X_df[col].dtype.name == 'category':
                if fit_encoders:
                    self.label_encoders[col] = LabelEncoder()
                    X_df[col] = self.label_encoders[col].fit_transform(X_df[col].astype(str))
                else:
                    # Transform using fitted encoder
                    X_df[col] = self.label_encoders[col].transform(X_df[col].astype(str))
        
        # Fill remaining NaN with column mean
        X_df = X_df.fillna(X_df.mean())
        
        # Scale features
        if fit_encoders:
            self.feature_scaler = StandardScaler()
            X = self.feature_scaler.fit_transform(X_df)
        else:
            X = self.feature_scaler.transform(X_df)
        
        # Encode target for classification
        if self.task_type == 'classification':
            if fit_encoders:
                self.target_encoder = LabelEncoder()
                y = self.target_encoder.fit_transform(y.astype(str))
            else:
                y = self.target_encoder.transform(y.astype(str))
        
        return X, y
    
    def _evaluate(self, y_true, y_pred) -> Dict[str, float]:
        """
        Evaluate predictions.
        
        Args:
            y_true: True labels/values
            y_pred: Predicted labels/values
            
        Returns:
            Dictionary of metrics
        """
        if self.task_type == 'classification':
            accuracy = accuracy_score(y_true, y_pred)
            precision, recall, f1, _ = precision_recall_fscore_support(
                y_true, y_pred, average='weighted', zero_division=0
            )
            return {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1': f1
            }
        else:  # regression
            mse = mean_squared_error(y_true, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_true, y_pred)
            return {
                'mse': mse,
                'rmse': rmse,
                'r2': r2
            }
    
    def train(
        self,
        df: pd.DataFrame,
        target_column: str,
        feature_columns: List[str],
        test_size: float = 0.2
    ) -> Dict[str, Any]:
        """
        Train model on dataset.
        
        Args:
            df: Input dataframe
            target_column: Column to predict
            feature_columns: Columns to use as features
            test_size: Fraction of data to use for testing
            
        Returns:
            Dictionary with training results and metrics
        """
        logger.info(f"Training {self.model_type} for {self.task_type}")
        logger.info(f"Target: {target_column}, Features: {feature_columns}")
        
        # Preprocess data
        X, y = self._preprocess_data(df, feature_columns, target_column, fit_encoders=True)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        logger.info(f"Training samples: {len(X_train)}, Test samples: {len(X_test)}")
        
        # Train model
        self.model = self._get_model()
        self.model.fit(X_train, y_train)
        
        # Evaluate
        y_pred_train = self.model.predict(X_train)
        y_pred_test = self.model.predict(X_test)
        
        train_metrics = self._evaluate(y_train, y_pred_train)
        test_metrics = self._evaluate(y_test, y_pred_test)
        
        # Log results
        logger.info("Training metrics:")
        for k, v in train_metrics.items():
            logger.info(f"  {k}: {v:.4f}")
        
        logger.info("Test metrics:")
        for k, v in test_metrics.items():
            logger.info(f"  {k}: {v:.4f}")
        
        # Feature importance (if available)
        feature_importance = None
        if hasattr(self.model, 'feature_importances_'):
            feature_importance = dict(zip(
                feature_columns,
                self.model.feature_importances_
            ))
            logger.info("Feature importance:")
            for feat, imp in sorted(feature_importance.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {feat}: {imp:.4f}")
        
        return {
            'train_metrics': train_metrics,
            'test_metrics': test_metrics,
            'feature_importance': feature_importance,
            'n_train': len(X_train),
            'n_test': len(X_test)
        }
    
    def predict(self, df: pd.DataFrame, feature_columns: List[str]) -> np.ndarray:
        """
        Make predictions on new data.
        
        Args:
            df: Input dataframe
            feature_columns: Columns to use as features (must match training)
            
        Returns:
            Array of predictions
        """
        if self.model is None:
            raise ValueError("Model not trained yet. Call train() first.")
        
        # Preprocess using fitted encoders
        X, _ = self._preprocess_data(
            df,
            feature_columns,
            feature_columns[0],  # Dummy target
            fit_encoders=False
        )
        
        predictions = self.model.predict(X)
        
        # Decode predictions for classification
        if self.task_type == 'classification':
            predictions = self.target_encoder.inverse_transform(predictions)
        
        return predictions


def train_simple_model(
    df: pd.DataFrame,
    target_column: str,
    feature_columns: List[str],
    model_type: Literal['classification', 'regression'] = 'classification',
    algorithm: str = 'random_forest',
    test_size: float = 0.2
) -> Dict[str, Any]:
    """
    Convenience function for quick model training.
    
    Args:
        df: Input dataframe
        target_column: Column to predict
        feature_columns: Columns to use as features
        model_type: 'classification' or 'regression'
        algorithm: Model algorithm to use
        test_size: Fraction of data for testing
        
    Returns:
        Dictionary with training results
    """
    trainer = SimpleModelTrainer(task_type=model_type, model_type=algorithm)
    results = trainer.train(df, target_column, feature_columns, test_size)
    return results


