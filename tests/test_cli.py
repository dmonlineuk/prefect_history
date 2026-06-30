"""Tests for prefect_history.__main__ (CLI)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from prefect_history.__main__ import _build_parser, main


class TestParser:
    def test_backfill_defaults(self):
        parser = _build_parser()
        args = parser.parse_args(["backfill"])
        assert args.command == "backfill"
        assert args.months is None

    def test_backfill_with_months(self):
        parser = _build_parser()
        args = parser.parse_args(["backfill", "-m", "6"])
        assert args.months == 6

    def test_sync_command(self):
        parser = _build_parser()
        args = parser.parse_args(["sync"])
        assert args.command == "sync"

    def test_status_command(self):
        parser = _build_parser()
        args = parser.parse_args(["status"])
        assert args.command == "status"

    def test_verbose_flag(self):
        parser = _build_parser()
        args = parser.parse_args(["-v", "sync"])
        assert args.verbose is True

    def test_env_file_override(self):
        parser = _build_parser()
        args = parser.parse_args(["--env-file", "/tmp/custom.env", "sync"])
        assert args.env_file == "/tmp/custom.env"

    def test_db_override(self):
        parser = _build_parser()
        args = parser.parse_args(["--db", "/tmp/custom.db", "status"])
        assert args.db == "/tmp/custom.db"


class TestMainBackfill:
    def test_backfill_calls_sync(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        db_path = str(tmp_path / "cli.db")
        with patch("prefect_history.__main__.backfill", return_value=10) as mock_bf:
            main(["--db", db_path, "backfill", "-m", "3"])

        mock_bf.assert_called_once()
        _, kwargs = mock_bf.call_args
        assert kwargs["months"] == 3


class TestMainSync:
    def test_sync_calls_incremental(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        db_path = str(tmp_path / "cli.db")
        with patch("prefect_history.__main__.incremental", return_value=5) as mock_inc:
            main(["--db", db_path, "sync"])

        mock_inc.assert_called_once()


class TestMainStatus:
    def test_status_runs(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        db_path = str(tmp_path / "cli.db")
        main(["--db", db_path, "status"])

        output = capsys.readouterr().out
        assert "Total runs" in output
        assert "Last sync" in output


class TestParserList:
    def test_list_defaults(self):
        parser = _build_parser()
        args = parser.parse_args(["list"])
        assert args.command == "list"
        assert args.limit == 20
        assert args.offset == 0
        assert args.state is None
        assert args.flow is None

    def test_list_with_options(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["list", "-n", "50", "--offset", "10", "--state", "FAILED", "--flow", "etl"]
        )
        assert args.limit == 50
        assert args.offset == 10
        assert args.state == "FAILED"
        assert args.flow == "etl"


class TestParserServe:
    def test_serve_defaults(self):
        parser = _build_parser()
        args = parser.parse_args(["serve"])
        assert args.command == "serve"
        assert args.host == "127.0.0.1"
        assert args.port == 8000

    def test_serve_custom(self):
        parser = _build_parser()
        args = parser.parse_args(["serve", "--host", "0.0.0.0", "--port", "9000"])
        assert args.host == "0.0.0.0"
        assert args.port == 9000


class TestMainList:
    def test_list_displays_table(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        from prefect_history.db import FlowRunDB
        from tests.conftest import make_flow_run_row

        db_path = str(tmp_path / "cli_list.db")
        db = FlowRunDB(db_path)
        db.upsert_flow_runs([make_flow_run_row(run_id="list-1", flow_name="my-flow")])

        # _cmd_list uses rich Console which writes to stdout
        main(["--db", db_path, "list"])


class TestParserSummary:
    def test_summary_defaults(self):
        parser = _build_parser()
        args = parser.parse_args(["summary"])
        assert args.command == "summary"
        assert args.since is None
        assert args.flow is None

    def test_summary_with_options(self):
        parser = _build_parser()
        args = parser.parse_args(["summary", "--since", "2026-01-01", "--flow", "etl"])
        assert args.since == "2026-01-01"
        assert args.flow == "etl"


class TestMainSummary:
    def test_summary_displays_table(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        from prefect_history.db import FlowRunDB
        from tests.conftest import make_flow_run_row

        db_path = str(tmp_path / "cli_summary.db")
        db = FlowRunDB(db_path)
        db.upsert_flow_runs(
            [
                make_flow_run_row(
                    run_id="sm1", flow_name="etl", state_type="COMPLETED"
                ),
                make_flow_run_row(run_id="sm2", flow_name="etl", state_type="FAILED"),
            ]
        )

        main(["--db", db_path, "summary"])

    def test_summary_empty_db(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        db_path = str(tmp_path / "empty.db")
        main(["--db", db_path, "summary"])
        output = capsys.readouterr().out
        assert "No flow runs found" in output

    def test_summary_with_flow_filter(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        from prefect_history.db import FlowRunDB
        from tests.conftest import make_flow_run_row

        db_path = str(tmp_path / "cli_summary_filter.db")
        db = FlowRunDB(db_path)
        db.upsert_flow_runs(
            [
                make_flow_run_row(run_id="f1", flow_name="etl", state_type="COMPLETED"),
                make_flow_run_row(
                    run_id="f2", flow_name="other", state_type="COMPLETED"
                ),
            ]
        )

        main(["--db", db_path, "summary", "--flow", "etl"])


class TestParserShow:
    def test_show_parses_run_id(self):
        parser = _build_parser()
        args = parser.parse_args(["show", "abc-123"])
        assert args.command == "show"
        assert args.run_id == "abc-123"


class TestMainShow:
    def test_show_displays_detail(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        from prefect_history.db import FlowRunDB
        from tests.conftest import make_flow_run_row

        db_path = str(tmp_path / "cli_show.db")
        db = FlowRunDB(db_path)
        db.upsert_flow_runs(
            [
                make_flow_run_row(
                    run_id="show-1",
                    flow_name="etl",
                    deployment_name="daily-etl",
                    entrypoint="flows/etl.py:run",
                )
            ]
        )

        main(["--db", db_path, "show", "show-1"])

    def test_show_not_found(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

        db_path = str(tmp_path / "cli_show_empty.db")
        main(["--db", db_path, "show", "nonexistent"])
        output = capsys.readouterr().out
        assert "Flow run not found" in output


class TestMainNoCommand:
    def test_no_command_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code == 1
