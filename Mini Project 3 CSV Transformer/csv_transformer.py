import pandas as pd
import argparse
import json
import sys
import os
from pathlib import Path

# Set base path relative to where the script is located
BASE_DIR = Path(__file__).parent.resolve()


def load_csv(path, **kwargs):
    """Load CSV file with optional pandas read_csv kwargs."""
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        sys.exit(1)

def transform_df(df, **kwargs):
    """Apply transformations based on kwargs."""
    if kwargs.get("dropna"):
        df = df.dropna()

    if "drop_columns" in kwargs:
        df = df.drop(columns=kwargs["drop_columns"], errors='ignore')

    if "rename" in kwargs:
        df = df.rename(columns=kwargs["rename"])

    if "astype" in kwargs:
        try:
            df = df.astype(kwargs["astype"])
        except Exception as e:
            print(f"Error casting data types: {e}")

    if "filter" in kwargs:
        for col, val in kwargs["filter"].items():
            df = df[df[col] == val]

    if "sort_by" in kwargs:
        df = df.sort_values(by=kwargs["sort_by"])

    return df

def save_csv(df, path, **kwargs):
    """Save DataFrame to CSV."""
    try:
        os.makedirs(path.parent, exist_ok=True)
        df.to_csv(path, index=False, **kwargs)
        print(f"Saved transformed CSV to {path}")
    except Exception as e:
        print(f"Error saving CSV: {e}")



