import argparse

from scheduled_tasks.get_fx_up_to_date import fetch_fx_timeframe, store_fx_and_backup


def main(from_, to):
    response = fetch_fx_timeframe(start_date=from_, end_date=to)
    store_fx_and_backup(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Example CLI with from/to arguments")

    parser.add_argument(
        "--from",
        dest="from_",
        required=True,
        help="Source value",
    )

    parser.add_argument(
        "--to",
        required=True,
        help="Destination value",
    )

    args = parser.parse_args()

    main(args.from_, args.to)