# BlobShuffle Evaluation Results

Raw Theodolite benchmark results and the analysis notebook that reproduces all evaluation plots.

## Contents

| Path | Description |
|------|-------------|
| `analysis.ipynb` | Jupyter notebook — run this to reproduce all evaluation figures |
| `requirements.txt` | Python dependencies |
| `plots/` | Output directory for generated PDF figures (git-ignored) |
| `results-*/` | Raw Theodolite CSVs for all experiments |

CSV file naming: `exp{id}_{load_bytes}_{resources}_{metric}_{repetition}.csv`

## Running the Notebook

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook analysis.ipynb
```

Run all cells top-to-bottom. Figures are saved to `plots/`.
