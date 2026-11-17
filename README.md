# Order-book collector (Kalshi and Polymarket)

Records the public order books of Kalshi and Polymarket markets once a minute, for a university
research project on prediction markets. It only reads public market data. It places no orders and
uses no account or key on either venue.

- `collect_books.py`: the collector (Python standard library only).
- `.github/workflows/collect.yml`: runs the collector on GitHub's servers and saves the data.
- `.github/workflows/watchdog.yml`: restarts the collector if it is not running.

Data is under **Releases**: one release per UTC day (`data-YYYY-MM-DD`) with the order-book files,
and the release `state` with the list of tracked markets, the market descriptions, the last status
and `index.tsv`, a list of every data file.

To stop collecting: Actions tab, open `collect` and `watchdog`, "..." menu, "Disable workflow".
