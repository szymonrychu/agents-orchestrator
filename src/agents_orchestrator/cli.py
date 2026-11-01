"""argparse front end with a command registry."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence

from agents_orchestrator import __version__

CommandFunc = Callable[[argparse.Namespace], int | None]

ConfigureFunc = Callable[[argparse.ArgumentParser], None]

_COMMANDS: dict[str, tuple[CommandFunc, str, ConfigureFunc | None]] = {}


def register(
    name: str,
    func: CommandFunc,
    help: str,  # noqa: A002
    configure: ConfigureFunc | None = None,
) -> None:
    """Register a subcommand; func receives the parsed args and returns the exit code.

    `configure` adds the subcommand's own arguments to its parser.
    """
    _COMMANDS[name] = (func, help, configure)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orch", description="agents-orchestrator")
    parser.add_argument("--version", action="version", version=f"orch {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for name, (func, help_text, configure) in _COMMANDS.items():
        cmd = sub.add_parser(name, help=help_text)
        cmd.set_defaults(func=func)
        if configure is not None:
            configure(cmd)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.command is None:
        parser.print_help(sys.stderr)
        raise SystemExit(2)
    return int(args.func(args) or 0)


# ---------------------------------------------------------------------- service commands
# Imports are lazy so `orch --help` and `orch migrate` load only what they run.


def _settings():
    from agents_orchestrator.runtime.run import configure_logging
    from agents_orchestrator.settings import Settings, SettingsError

    try:
        settings = Settings()
    except SettingsError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(2) from None
    configure_logging(settings.log_level)
    return settings


def bind_sockets(settings) -> list:
    """The main, agent and service listening sockets, bound here so one server serves all."""
    import socket

    family = socket.AF_INET6 if ":" in settings.http_host else socket.AF_INET
    socks = []
    try:
        for port in (settings.http_port, settings.agent_port, settings.service_port):
            sock = socket.socket(family, socket.SOCK_STREAM)
            socks.append(sock)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((settings.http_host, port))
            sock.listen(2048)
            sock.set_inheritable(True)
    except BaseException:
        for sock in socks:
            sock.close()
        raise
    return socks


def build_uvicorn_config(settings):
    """uvicorn config for `orch api`.

    Proxy headers are off and no forwarded-allow-ips is set, so the direct TCP peer is what
    the source allowlist sees and X-Forwarded-For cannot spoof it (D22). WebSocket frames are
    capped at the wrapper's terminal frame size so a client cannot make the api buffer
    uvicorn's 16 MiB default before the relay forwards them.
    """
    import uvicorn

    from agents_orchestrator.api.app import create_app

    return uvicorn.Config(
        create_app(settings),
        host=settings.http_host,
        port=settings.http_port,
        proxy_headers=False,
        forwarded_allow_ips=None,
        log_config=None,
        ws_max_size=65536,
        timeout_graceful_shutdown=int(settings.shutdown_grace_seconds),
    )


def _cmd_migrate(_args: argparse.Namespace) -> int:
    import logging

    from agents_orchestrator.db.migrate import run_migrations

    settings = _settings()
    applied = run_migrations(settings.database_url)
    logging.getLogger("agents_orchestrator").info("migrations applied: %s", applied or "none")
    return 0


def _cmd_api(_args: argparse.Namespace) -> int:
    import asyncio
    import contextlib

    import uvicorn

    from agents_orchestrator.runtime.run import install_signal_handlers

    class _Server(uvicorn.Server):
        # uvicorn re-raises the captured signal after a graceful stop, which would turn a clean
        # SIGTERM into a signal death; our handlers set should_exit and the exit code stays 0.
        def capture_signals(self):
            return contextlib.nullcontext()

    async def main() -> None:
        server = _Server(build_uvicorn_config(settings))
        socks = bind_sockets(settings)
        stop = asyncio.Event()
        install_signal_handlers(stop)

        async def watch() -> None:
            await stop.wait()
            server.should_exit = True

        watcher = asyncio.create_task(watch())
        try:
            await server.serve(sockets=socks)
        finally:
            watcher.cancel()
            for sock in socks:
                sock.close()

    settings = _settings()
    asyncio.run(main())
    return 0


def _run(factory_builder, singleton: str | None) -> int:
    import asyncio

    from agents_orchestrator.runtime.run import run_service

    settings = _settings()
    return asyncio.run(run_service(settings, factory_builder(settings), singleton=singleton))


def _cmd_dispatcher(_args: argparse.Namespace) -> int:
    from agents_orchestrator.dispatcher.loop import Dispatcher

    return _run(lambda s: lambda pool: Dispatcher(pool, s), None)


def _cmd_ingester(_args: argparse.Namespace) -> int:
    from agents_orchestrator.ingester.loop import Ingester

    return _run(lambda s: lambda pool: Ingester(pool, s), "ingester")


def _cmd_reconciler(_args: argparse.Namespace) -> int:
    from agents_orchestrator.k8s.kube import LightkubeApi
    from agents_orchestrator.reconciler.loop import Reconciler
    from agents_orchestrator.wrapper.client import WrapperClient

    def build(settings):
        def factory(pool):
            return Reconciler(
                pool,
                LightkubeApi(),
                settings,
                lambda url, token: WrapperClient(
                    url, token, send_timeout=settings.wrapper_send_timeout_seconds
                ),
            )

        return factory

    return _run(build, "reconciler")


def _configure_bench(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("spec", help="bench spec YAML")
    parser.add_argument("--api", required=True, help="orchestrator base URL")
    parser.add_argument("--out", required=True, help="JSONL results file")
    parser.add_argument("--keep", action="store_true", help="do not discard the bench sessions")


def _cmd_bench(args: argparse.Namespace) -> int:
    from agents_orchestrator.bench.report import format_table, summarize
    from agents_orchestrator.bench.runner import BenchError, run_bench
    from agents_orchestrator.bench.spec import BenchSpecError, load_spec

    try:
        spec = load_spec(args.spec)
    except BenchSpecError as exc:
        for err in exc.errors:
            print(f"bench spec error: {err}", file=sys.stderr)
        raise SystemExit(2) from None
    try:
        records = run_bench(spec, args.api, args.out, keep=args.keep)
    except BenchError as exc:
        print(f"bench failed: {exc}", file=sys.stderr)
        return 1
    print(format_table(summarize(records)))
    return 0


def _configure_cache(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("action", choices=["restore", "save"], help="direction of the transfer")


def _cmd_cache(args: argparse.Namespace) -> int:
    """Job-side workspace transfer; reads only CACHE_* and never constructs Settings."""
    import os

    from agents_orchestrator.cache import transfer

    run = transfer.restore if args.action == "restore" else transfer.save
    return run(os.environ)


def _cmd_presets_check(_args: argparse.Namespace) -> int:
    from agents_orchestrator import presets_check

    settings = _settings()
    return presets_check.run(settings.database_url, settings, sys.stdout)


register("migrate", _cmd_migrate, "apply database migrations and exit")
register(
    "presets-check",
    _cmd_presets_check,
    "list the stored presets a session create would refuse (run before a deploy; prints no values)",
)
register("api", _cmd_api, "serve the HTTP API and UI")
register("dispatcher", _cmd_dispatcher, "dispatch queued messages to agents")
register("ingester", _cmd_ingester, "store every wrapper event (singleton)")
register("reconciler", _cmd_reconciler, "reconcile session objects in Kubernetes (singleton)")
register(
    "bench",
    _cmd_bench,
    "run the experiment arms against the API (talks only to /api/v1)",
    _configure_bench,
)
register(
    "cache",
    _cmd_cache,
    "restore or save the session workspace through CACHE_URL (cache Job)",
    _configure_cache,
)


if __name__ == "__main__":
    raise SystemExit(main())
