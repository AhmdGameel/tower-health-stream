import argparse
import logging
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OPERATORS = {
    1: "Orange",
    2: "Vodafone",
    3: "e&",
    4: "WE",
}

RAW_COLUMNS = [
    "radio", "mcc", "net", "area", "cell", "unit",
    "lon", "lat", "range", "samples", "changeable",
    "created", "updated", "averageSignal",
]


def clean_towers(input_path: str, output_path: str) -> None:
    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Raw dataset not found at {input_path}. "
            "Download Egypt (MCC=602) CSV from OpenCelliD and save it there."
        )

    log.info("Loading raw tower dataset from %s...", input_path)
    df = pd.read_csv(input_path, header=None, names=RAW_COLUMNS, low_memory=False)
    log.info("Total rows before cleaning: %d", len(df))

    # Filter to Egypt (MCC=602) and the 4 major Egyptian operators
    df = df[df["mcc"] == 602]
    df = df[df["net"].isin(OPERATORS.keys())]

    # Remove missing or invalid coordinates and zero range
    df = df.dropna(subset=["lat", "lon"])
    df = df[(df["lat"] != 0) & (df["lon"] != 0)]
    df = df[df["range"] > 0]

    df["operator"] = df["net"].map(OPERATORS)

    df = df[
        ["cell", "radio", "operator", "net", "lat", "lon", "range", "area"]
    ].rename(columns={"cell": "tower_id"})

    df = df.drop_duplicates(subset=["tower_id"])

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    df.to_csv(output_path, index=False)

    log.info("Saved %d clean towers to %s", len(df), output_path)
    log.info("Operator distribution:\n%s", df["operator"].value_counts().to_string())


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw OpenCelliD Egypt tower CSV.")
    parser.add_argument("--input", default="data/towers_raw.csv", help="Path to raw OpenCelliD CSV")
    parser.add_argument("--output", default="data/towers_clean.csv", help="Path to cleaned output CSV")
    args = parser.parse_args()
    clean_towers(args.input, args.output)


if __name__ == "__main__":
    main()
