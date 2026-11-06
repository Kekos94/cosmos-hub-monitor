"""Criterion 2 entrypoint; exports validated progress even if one chain fails."""
import subprocess
import sys
from criterion2_pipeline import ROOT


def main():
    collected=subprocess.run([sys.executable,str(ROOT/'criterion2_pipeline.py')],cwd=ROOT)
    subprocess.run([sys.executable,str(ROOT/'criterion2_export.py'),'--fetch-prices'],cwd=ROOT,check=True)
    subprocess.run([sys.executable,str(ROOT/'publish_criterion2.py')],cwd=ROOT,check=True)
    if collected.returncode:raise SystemExit(collected.returncode)


if __name__=='__main__':main()
