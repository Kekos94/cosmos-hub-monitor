"""Fail-closed Criterion 1 pipeline, independent of the working directory."""
import subprocess
import sys
from criterion1_data import ROOT


def main():
    for script in ('criterion1_daily.py', 'inflation_overlay.py'):
        subprocess.run([sys.executable, str(ROOT / script)], cwd=ROOT, check=True)


if __name__ == '__main__':
    main()
