"""Collect activity, fees and issuance for Criterion 1A, 1C and 1D."""
import subprocess
import sys
from criterion1_data import ROOT


def main():
    for script in ('criterion1_daily.py', 'inflation_overlay.py'):
        subprocess.run([sys.executable, str(ROOT / script)], cwd=ROOT, check=True)


if __name__ == '__main__':
    main()
