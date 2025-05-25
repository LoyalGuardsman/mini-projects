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


def main():
    parser = argparse.ArgumentParser(description="CSV Transformer")
    parser.add_argument("--input", required=False, help="Path to input CSV")
    parser.add_argument("--output", required=False, help="Path to save transformed CSV")
    parser.add_argument("--config", required=False, help="Path to JSON config file for transformations")
    args = parser.parse_args()

    # Use script-relative defaults if not provided
    if not args.input:
        print("No input provided. Using default sample_data.csv")
        args.input = BASE_DIR / "sample_data.csv"
    else:
        args.input = Path(args.input)

    if not args.output:
        args.output = BASE_DIR / "output.csv"
    else:
        args.output = Path(args.output)

    if not args.config:
        args.config = BASE_DIR / "sample_config.json"
    else:
        args.config = Path(args.config)

    input_path = args.input.resolve()
    output_path = args.output.resolve()
    config_path = args.config.resolve()

    # Check for missing files early
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        sys.exit(1)
    if not config_path.exists():
        print(f"Config file not found: {config_path}\nProceeding with no transformations.")

    kwargs = {}
    if config_path.exists():
        with open(config_path, "r") as f:
            kwargs = json.load(f)

    df = load_csv(input_path)
    df = transform_df(df, **kwargs)

    if output_path:
        save_csv(df, output_path)
    else:
        print(df.head())

if __name__ == "__main__":
    main()
