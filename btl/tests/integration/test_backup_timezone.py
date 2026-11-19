"""Restore fingerprints must describe rows, not a connection's display timezone."""

from contextlib import closing
import uuid
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def fingerprint_database(monkeypatch):
    import psycopg2
    from psycopg2 import sql
    from config.settings import settings

    params = dict(host=settings.database.host, port=settings.database.port,
                  user=settings.database.user, password=settings.database.password,
                  dbname=settings.database.database)
    name = 'fingerprint_tz_' + uuid.uuid4().hex
    admin = psycopg2.connect(**params)
    admin.autocommit = True
    admin.cursor().execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    try:
        for key, value in dict(HOST=params['host'],PORT=params['port'],USER=params['user'],
                               PASSWORD=params['password'],DB=name).items():
            monkeypatch.setenv('FPTZ_'+key, str(value))
        monkeypatch.setenv('PGOPTIONS','-c search_path=public')
        with closing(psycopg2.connect(**dict(params, dbname=name))) as connection, connection:
            with connection.cursor() as cursor:
                cursor.execute('CREATE TABLE proof (id bigint, clock timestamptz, local_clock timestamp, clocks timestamptz[])')
                cursor.execute("INSERT INTO proof VALUES (1,'2026-11-19T07:00:00Z','2026-11-19 14:00:00',ARRAY['2026-11-19T07:00:00Z'::timestamptz])")
        yield {'env_prefix':'FPTZ_'}
    finally:
        admin.cursor().execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
        admin.close()


def test_business_fingerprint_is_portable_across_connection_timezones(fingerprint_database, monkeypatch):
    from operations.backup import _pg_fingerprint

    monkeypatch.setenv('PGTZ','Asia/Ho_Chi_Minh')
    source = _pg_fingerprint(fingerprint_database)
    monkeypatch.setenv('PGTZ','UTC')
    restored = _pg_fingerprint(fingerprint_database)
    assert source == restored


def test_legacy_fingerprint_requires_its_explicit_source_timezone(fingerprint_database, monkeypatch):
    from operations.backup import _pg_fingerprint

    legacy = dict(fingerprint_database, sql_fingerprint_version=1)
    monkeypatch.setenv('PGTZ','Asia/Ho_Chi_Minh')
    source = _pg_fingerprint(legacy)
    monkeypatch.setenv('PGTZ','UTC')
    assert _pg_fingerprint(legacy) != source
    assert _pg_fingerprint(dict(legacy, sql_fingerprint_timezone='Asia/Ho_Chi_Minh')) == source


def test_unknown_fingerprint_format_is_refused(fingerprint_database):
    from operations.backup import _pg_fingerprint

    with pytest.raises(ValueError, match='fingerprint version'):
        _pg_fingerprint(dict(fingerprint_database, sql_fingerprint_version=999))
