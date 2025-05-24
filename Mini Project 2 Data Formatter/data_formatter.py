from pathlib import Path
import pandas as pd
from datetime import datetime

# Set up base paths relative to where the script is located
BASE_DIR = Path(__file__).parent.resolve()
MESSY_DIR = BASE_DIR / "messy_samples"
CLEAN_DIR = BASE_DIR / "clean_samples"
CLEAN_DIR.mkdir(parents=True, exist_ok=True) # Create the output folder if it doesn't exist

# Reads a file into a pandas DataFrame based on its extension
def read_any(path):
    ext = path.suffix.lower()
    match ext:
        case ".csv":
            return pd.read_csv(path, dtype=str, keep_default_na=False)
        case ".json":
            return pd.read_json(path)
        case ".jsonl":
            return pd.read_json(path, lines=True)
        case ".parquet":
            return pd.read_parquet(path)
        case _:
            raise ValueError(f"Unsupported file type: {ext}")

# Writes a DataFrame to a file, matching the original file format
def write_any(df, path):
    ext = path.suffix.lower()
    match ext:
        case ".csv":
            df.to_csv(path, index=False)
        case ".json":
            df.to_json(path, orient="records", indent=2)
        case ".jsonl":
            df.to_json(path, orient="records", lines=True)
        case ".parquet":
            df.to_parquet(path, index=False)
        case _:
            raise ValueError(f"Unsupported file type: {ext}")

# Cleans up the DataFrame: whitespace, casing, date formatting, removes blanks
def clean_dataframe(df):
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

    if "Name" in df.columns:
        df["Name"] = df["Name"].str.title()
    if "Email" in df.columns:
        df["Email"] = df["Email"].str.lower()
    if "Date of Birth" in df.columns:
        df["Date of Birth"] = df["Date of Birth"].apply(normalize_date)

    df = df.dropna(subset=["Name", "Email"])
    df = df[(df["Name"] != "") & (df["Email"] != "")]
    return df

# Attempts to parse various date formats into 'YYYY-MM-DD'
def normalize_date(x):
    if not isinstance(x, str): return x
    formats = ["%m-%d-%Y", "%d/%m/%y", "%Y/%m/%d", "%B %d, %Y"]
    for fmt in formats:
        try:
            return datetime.strptime(x.strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    return x

# Main entry point: loops through all files in messy_samples
def main():
    for path in MESSY_DIR.iterdir():
        out_path = CLEAN_DIR / path.name
        try:
            df = read_any(path)
            cleaned = clean_dataframe(df)
            write_any(cleaned, out_path)
            print(f"Cleaned {path.name}")
        except Exception as e:
            print(f"Skipped {path.name} — {e}")

if __name__ == "__main__":
    main()
