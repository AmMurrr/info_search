from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

base = Path(__file__).parent
zipf_path = base / 'data' / 'zipf.tsv'

if not zipf_path.exists():
	raise SystemExit(f"zipf.tsv not found at {zipf_path}. Run labs_app to generate it.")

df = pd.read_csv(zipf_path, sep='\t', encoding='utf-8', encoding_errors='replace')
plt.figure(figsize=(8,5))
plt.plot(df['log_rank'], df['log_freq'], label='Корпус', lw=1)
plt.plot(df['log_rank'], df['log_zipf_expected'], label='Zipf f_max/rank', lw=1)
plt.xlabel('log10(rank)')
plt.ylabel('log10(freq)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig('zipf.png', dpi=150)
plt.show()