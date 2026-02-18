from multiprocessing import Process
from pathlib import Path
from typing import cast, final

from .parser import ParserSessionStats
from .visualizer import DashApp


def target(
    parser: ParserSessionStats,
    debug: bool,
    host: str,
    port: int,
    explorer_url: str | None = None,
    show_validator_set_bin: str | None = None,
):
    DashApp(
        parser,
        explorer_url=explorer_url,
        show_validator_set_bin=show_validator_set_bin,
    ).run(debug, host, port)


@final
class ConsensusExplorer:
    def __init__(
        self,
        logs_path: list[Path],
        host: str = "127.0.0.1",
        port: int = 8050,
        explorer_url: str | None = None,
        show_validator_set_bin: str | None = None,
    ):
        self._logs_path = logs_path
        self._host = host
        self._port = port
        self._explorer_url = explorer_url
        self._show_validator_set_bin = show_validator_set_bin
        self.__process: Process | None = None

    def run(self):
        self.__process = Process(
            target=target,
            kwargs={
                "parser": ParserSessionStats(self._logs_path),
                "debug": False,
                "host": self._host,
                "port": self._port,
                "explorer_url": self._explorer_url,
                "show_validator_set_bin": self._show_validator_set_bin,
            },
        )
        self.__process.start()

    def kill(self):
        if self.__process is not None:
            self.__process.terminate()


def _main():
    import argparse
    import os

    parser = argparse.ArgumentParser()
    _ = parser.add_argument(
        "--logs", nargs="+", required=True, help="Paths to log files or directory"
    )
    _ = parser.add_argument(
        "--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)"
    )
    _ = parser.add_argument(
        "--port", type=int, default=8050, help="Port to bind to (default: 8050)"
    )
    _ = parser.add_argument(
        "--block-explorer-url",
        default=os.getenv("CONSENSUS_EXPLORER_URL", ""),
        help=(
            "Block explorer base url (for validator set lookup), "
            "e.g. http://127.0.0.1:8081"
        ),
    )
    _ = parser.add_argument(
        "--show-validator-set-bin",
        default=os.getenv("CONSENSUS_EXPLORER_SHOW_VALIDATOR_SET_BIN", ""),
        help=(
            "Path to show-validator-set binary "
            "(default: build/utils/show-validator-set)"
        ),
    )

    args = parser.parse_args()
    logs = cast(list[str], args.logs)
    host = cast(str, args.host)
    port = cast(int, args.port)
    block_explorer_url = cast(str, args.block_explorer_url) or None
    show_validator_set_bin = cast(str, args.show_validator_set_bin) or None

    log_paths = [Path(log) for log in logs]
    if len(log_paths) == 1 and log_paths[0].is_dir():
        log_paths = [p for p in log_paths[0].iterdir()]
    app = DashApp(
        ParserSessionStats(log_paths),
        explorer_url=block_explorer_url,
        show_validator_set_bin=show_validator_set_bin,
    )

    app.run(debug=True, host=host, port=port)


if __name__ == "__main__":
    _main()
