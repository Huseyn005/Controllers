import CaspianPetro
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--data-dir",
    required=True,
    help="Exact data directory passed from bash"
)

args = parser.parse_args()

DATA_DIR = args.data_dir

CaspianPetro.main(DATA_DIR)