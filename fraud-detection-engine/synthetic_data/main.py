"""Entry point for synthetic dataset generation."""
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from synthetic_data.generators.beneficiary_generator import BeneficiaryGenerator

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT = Path("ml/data/synthetic/dataset_ml.csv")


class SyntheticDataGenerator:
    """Orchestrate beneficiary + fraud generation and persist to CSV."""

    def generate_dataset(
        self,
        n_beneficiaries: int = 5000,
        fraud_rate: float = 0.05,
        output_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """Generate a complete synthetic dataset with injected fraud scenarios.

        Args:
            n_beneficiaries: Total rows to generate.
            fraud_rate: Fraction of rows that are fraudulent.
            output_path: CSV destination. Defaults to ml/data/synthetic/dataset_ml.csv.

        Returns:
            The generated DataFrame (also written to disk).
        """
        generator = BeneficiaryGenerator()
        df = generator.generate(n=n_beneficiaries, fraud_rate=fraud_rate)

        dest = Path(output_path) if output_path else DEFAULT_OUTPUT
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(dest, index=False)

        fraud_count = int(df["is_fraud"].sum())
        logger.info(
            "Dataset saved to %s (%d rows, %d fraud, %.1f%%)",
            dest, len(df), fraud_count, 100 * fraud_count / len(df),
        )
        return df
