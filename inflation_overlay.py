"""Offline validation of daily checkpoints and immutable legacy windows."""
from criterion1_data import WINDOWS, read_rows, validate
from criterion1_daily import load_days, progress, export_daily


def main():
    windows = read_rows(WINDOWS)
    validate(windows)
    states = load_days()
    export_daily()
    complete = sum(progress(s)['status'] == 'complete' for s in states)
    print(f'Validated {complete} complete UTC days, {len(states)-complete} in progress, {len(windows)} archived windows.')


if __name__ == '__main__':
    main()
