"""
Script to load data for week 2 bootcamp
"""

import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.io import load_data


def main():
    """Main function to run data loading"""
    print("Loading data...")
    # Add your data loading logic here
    pass


if __name__ == "__main__":
    main()
