"""CLI: train XGBoost + Isolation Forest and save artifacts.

Usage:
    python scripts/train_ml_models.py --data ml/data/synthetic/dataset_ml.csv --output app/models_saved/
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ml.train import MLTrainingPipeline

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train fraud detection ML models")
    parser.add_argument(
        "--data",
        type=str,
        default="ml/data/synthetic/dataset_ml.csv",
        help="Path to training CSV",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="app/models_saved",
        help="Directory to save model artifacts",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline = MLTrainingPipeline(models_dir=Path(args.output))
    metrics = pipeline.train(data_path=args.data)
    logger.info("Training complete. Metrics:\n%s", json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
