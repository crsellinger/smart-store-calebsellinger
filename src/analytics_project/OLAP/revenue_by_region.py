import pathlib
import sys

import matplotlib.pyplot as plt
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent.parent))

from utils.logger import logger

# Global constants for paths and key directories

THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
DW_DIR: pathlib.Path = THIS_DIR  # src/analytics_project/olap/
PACKAGE_DIR: pathlib.Path = DW_DIR.parent  # src/analytics_project/
SRC_DIR: pathlib.Path = PACKAGE_DIR.parent  # src/
PROJECT_ROOT_DIR: pathlib.Path = SRC_DIR.parent  # project_root/

# Data directories
DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "warehouse"

# Warehouse database location (SQLite)
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "smart_sales.db"

# OLAP output directory
OLAP_OUTPUT_DIR: pathlib.Path = DATA_DIR / "olap_cubing_outputs"

# CUBED File path
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR / "multidimensional_olap_cube.csv"

# Results output directory
RESULTS_OUTPUT_DIR: pathlib.Path = DATA_DIR / "results"

logger.info(f"THIS_DIR:            {THIS_DIR}")
logger.info(f"DW_DIR:              {DW_DIR}")
logger.info(f"PACKAGE_DIR:         {PACKAGE_DIR}")
logger.info(f"SRC_DIR:             {SRC_DIR}")
logger.info(f"PROJECT_ROOT_DIR:    {PROJECT_ROOT_DIR}")

logger.info(f"DATA_DIR:            {DATA_DIR}")
logger.info(f"WAREHOUSE_DIR:       {WAREHOUSE_DIR}")
logger.info(f"DB_PATH:             {DB_PATH}")
logger.info(f"OLAP_OUTPUT_DIR:     {OLAP_OUTPUT_DIR}")
logger.info(f"RESULTS_OUTPUT_DIR:  {RESULTS_OUTPUT_DIR}")

# Create output directory if it does not exist
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Create output directory for results if it doesn't exist
RESULTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_olap_cube(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the precomputed OLAP cube data."""
    try:
        cube_df = pd.read_csv(file_path)
        logger.info(f"OLAP cube data successfully loaded from {file_path}.")
        return cube_df
    except Exception as e:
        logger.error(f"Error loading OLAP cube data: {e}")
        raise


def analyze_top_region(cube_df: pd.DataFrame) -> pd.DataFrame:
    """Identify the product with the highest revenue for each day of the week."""
    try:
        # Group by region and product_id, sum the sales
        grouped = cube_df.groupby(["region"])["sale_amount_sum"].sum().reset_index()
        grouped.rename(columns={"sale_amount_sum": "TotalSales"}, inplace=True)

        # Sort within each day to find the top product
        top_regions = (
            grouped.sort_values(["region", "TotalSales"], ascending=[True, False])
            .groupby("region")
            .head(1)
        )
        logger.info("Top products identified for each day of the week.")
        return top_regions
    except Exception as e:
        logger.error(f"Error analyzing top product by DayOfWeek: {e}")
        raise


def visualize_sales_by_region(cube_df: pd.DataFrame) -> None:
    try:
        # Plot the stacked bar chart
        cube_df.plot(x="region", kind="bar", stacked=True, figsize=(12, 8), colormap="tab10")

        plt.title("Total Sales by Region", fontsize=16)
        plt.xlabel("Region", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("revenue_by_region.png")
        plt.savefig(output_path)
        logger.info(f"Stacked bar chart saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing sales by day and product: {e}")
        raise


def main():
    """Analyze and visualize top product sales by day of the week."""
    logger.info("Starting SALES_TOP_PRODUCT_BY_WEEKDAY analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    top_regions = analyze_top_region(cube_df)
    print(top_regions.head())

    visualize_sales_by_region(top_regions)
    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
