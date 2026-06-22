"""Tests for prefect_history.config."""

from __future__ import annotations

import pytest

from prefect_history.config import Settings, load_settings


class TestSettings:
    def test_frozen(self):
        s = Settings(
            prefect_api_url="https://example.com",
            prefect_api_key="key",
        )
        with pytest.raises(AttributeError):
            s.prefect_api_url = "changed"  # type: ignore[misc]

    def test_defaults(self):
        s = Settings(
            prefect_api_url="https://example.com",
            prefect_api_key="key",
        )
        assert s.db_path == "prefect_history.db"
        assert s.backfill_months == 2
        assert s.page_size == 200


class TestLoadSettings:
    def test_loads_from_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_secret")
        s = load_settings(env_file=None)
        assert s.prefect_api_url == "https://api.test.com"
        assert s.prefect_api_key == "pnu_secret"

    def test_missing_api_url_raises(self, monkeypatch):
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        monkeypatch.delenv("PREFECT_API_KEY", raising=False)
        with pytest.raises(ValueError, match="PREFECT_API_URL"):
            load_settings(env_file=None)

    def test_missing_api_key_raises(self, monkeypatch):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.delenv("PREFECT_API_KEY", raising=False)
        with pytest.raises(ValueError, match="PREFECT_API_KEY"):
            load_settings(env_file=None)

    def test_kwarg_overrides(self, monkeypatch):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_secret")
        s = load_settings(
            env_file=None,
            db_path="/custom/path.db",
            backfill_months=6,
            page_size=500,
        )
        assert s.db_path == "/custom/path.db"
        assert s.backfill_months == 6
        assert s.page_size == 500

    def test_env_var_overrides(self, monkeypatch):
        monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
        monkeypatch.setenv("PREFECT_API_KEY", "pnu_secret")
        monkeypatch.setenv("PH_DB_PATH", "/env/path.db")
        monkeypatch.setenv("PH_BACKFILL_MONTHS", "4")
        monkeypatch.setenv("PH_PAGE_SIZE", "100")
        s = load_settings(env_file=None)
        assert s.db_path == "/env/path.db"
        assert s.backfill_months == 4
        assert s.page_size == 100

    def test_loads_dot_env_file(self, tmp_path, monkeypatch):
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        monkeypatch.delenv("PREFECT_API_KEY", raising=False)
        env_file = tmp_path / ".env"
        env_file.write_text(
            "PREFECT_API_URL=https://from-file.com\n" "PREFECT_API_KEY=pnu_from_file\n"
        )
        s = load_settings(env_file=str(env_file))
        assert s.prefect_api_url == "https://from-file.com"
        assert s.prefect_api_key == "pnu_from_file"
