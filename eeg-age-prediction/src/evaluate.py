import os
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from src import inference, preprocess


def adjusted_r2_score(y_true, y_pred, n_features):
    """Calculate Adjusted R² score."""
    r2 = r2_score(y_true, y_pred)
    n = len(y_true)
    return 1 - (1 - r2) * (n - 1) / (n - n_features - 1)


def evaluate_all(
        eeg_dir: str,
        model_path: str,
        labels_path: str,
        file_extension: str = ".edf",
        n_features: int = 10  # estimated number of features
):
    """
    Evaluate a model on a set of EEG files and compute regression metrics.

    Parameters:
        eeg_dir (str): Path to the directory with EEG files.
        model_path (str): Path to the trained model.
        labels_path (str): CSV or JSON with 'filename' and 'age' columns.
        file_extension (str): Extension of EEG files.
        n_features (int): Number of features used in the model (for adjusted R²).

    Returns:
        dict: Dictionary of evaluation metrics.
    """
    # Load ground truth labels
    if labels_path.endswith(".csv"):
        labels_df = pd.read_csv(labels_path)
    elif labels_path.endswith(".json"):
        labels_df = pd.read_json(labels_path)
    else:
        raise ValueError("labels_path must be .csv or .json")

    assert "filename" in labels_df.columns and "age" in labels_df.columns, \
        "Labels file must contain columns: filename, age"

    y_true = []
    y_pred = []

    for _, row in labels_df.iterrows():
        filename = row["filename"]
        age = row["age"]
        file_path = os.path.join(eeg_dir, filename)

        if not os.path.exists(file_path):
            print(f"[WARNING] File not found: {file_path}")
            continue

        # Preprocess EEG and run inference
        x = preprocess.run_inline(file_path)
        pred_age = inference.run_from_array(x, model_path)

        y_true.append(age)
        y_pred.append(pred_age)

    # Calculate metrics
    r2 = r2_score(y_true, y_pred)
    adj_r2 = adjusted_r2_score(y_true, y_pred, n_features=n_features)
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)

    report = {
        "R2": r2,
        "Adjusted R2": adj_r2,
        "MAE": mae,
        "MSE": mse,
    }

    print("\nEvaluation Report:")
    for key, value in report.items():
        print(f"{key}: {value:.4f}")

    return report