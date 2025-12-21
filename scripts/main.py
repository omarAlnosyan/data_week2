"""Main script to display project paths"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.config import make_paths

def main():
    """Display paths"""
    
    # Get paths
    root = Path(__file__).parent.parent
    paths = make_paths(root)
    
    print("=" * 50)
    print("Project Paths")
    print("=" * 50)
    print(f"Root: {paths.root}")
    print(f"Raw data: {paths.raw}")
    print(f"Cache: {paths.cache}")
    print(f"Processed: {paths.processed}")
    print(f"External: {paths.external}")
    print("=" * 50)

if __name__ == "__main__":
    main()