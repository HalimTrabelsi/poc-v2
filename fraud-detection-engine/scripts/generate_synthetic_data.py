"""CLI: generate synthetic beneficiary data for ML training.

Usage:
    python scripts/generate_synthetic_data.py --n 5000 --output ml/data/synthetic/dataset_ml.csv
"""
import argparse
import logging
import sys
from pathlib import Path

# Allow running from the project root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from synthetic_data.main import SyntheticDataGenerator

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic fraud detection dataset")
    parser.add_argument("--n", type=int, default=5000, help="Number of beneficiaries to generate")
    parser.add_argument("--fraud-rate", type=float, default=0.05, help="Fraction of fraudulent beneficiaries")
    parser.add_argument(
        "--output",
        type=str,
        default="ml/data/synthetic/dataset_ml.csv",
        help="Output CSV path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generator = SyntheticDataGenerator()
    df = generator.generate_dataset(
        n_beneficiaries=args.n,
        fraud_rate=args.fraud_rate,
        output_path=args.output,
    )
    fraud_count = int(df["is_fraud"].sum())
    logger.info("Done. %d rows, %d fraud (%.1f%%)", len(df), fraud_count, 100 * fraud_count / len(df))


if __name__ == "__main__":
    main()
