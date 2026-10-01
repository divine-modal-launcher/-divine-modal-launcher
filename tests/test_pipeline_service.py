import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def service(tmp_path, monkeypatch):
    monkeypatch.setenv('BOT_DB', str(tmp_path / 'signals.db'))
    monkeypatch.setenv('WEBHOOK_PATH_TOKEN', 'test-token')
    monkeypatch.setenv('WEBHOOK_AUTH_KEY', 'test-key')
    monkeypatch.setenv('PIPELINE_ALERT_THRESHOLD', '1.2')
    monkeypatch.setenv('TELEGRAM_TOKEN', '')
    monkeypatch.setenv('TELEGRAM_CHAT_ID', '')
    spec = importlib.util.spec_from_file_location(
        'pipeline_service_test', Path(__file__).resolve().parents[1] / 'pipeline_service.py'
    )
    module = importlib.util.module_from_spec(spec)
    with patch('threading.Thread.start'):
        spec.loader.exec_module(module)
    module.app.config['TESTING'] = True
    module.send_telegram_async = Mock()
    return module


def stored(service):
    conn = service.db()
    try:
        return conn.execute(
            'SELECT decision, should_execute, reason, payload_json FROM signals'
        ).fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize('value', [
    'N/A', ' ', '1.2x', [], {}, [1], {'value': 1}, True, False,
    'NaN', 'Infinity', '-Infinity', '1e999', float('nan'), float('inf'), 10**400,
])
def test_invalid_pipeline_is_persisted_and_denied(service, value):
    payload = {'pair1': 'btc/usdt', 'pipeline1': value}
    response = service.app.test_client().post('/webhook/test-token', json=payload)
    assert response.status_code == 400
    body = response.get_json()
    assert body['decision'] == 'REJECTED'
    assert body['should_execute'] is False
    assert body['reason'] == 'INVALID_PIPELINE1'
    assert body['saved'] is True
    assert body['error'] == 'pipeline1 must be a finite number'
    rows = stored(service)
    assert len(rows) == 1
    assert rows[0][:3] == ('REJECTED', 0, 'INVALID_PIPELINE1')
    assert json.loads(rows[0][3])['pair1'] == payload['pair1']
    message = service.send_telegram_async.call_args.args[0]
    assert 'INVALID_PIPELINE1' in message
    assert 'HIGH PROBABILITY ALERT' not in message


@pytest.mark.parametrize('fields,expected', [
    ({'pipeline1': 1.5}, 1.5), ({'pipeline1': '1.5'}, 1.5),
    ({'pipeline1': 2}, 2), ({'pipeline1': ' 1.2 '}, 1.2),
    ({'pipeline1': '1e-2'}, .01), ({'pipeline1': -1}, -1),
    ({'pipeline1': 0}, 0), ({'pipeline1': '0'}, 0),
    ({}, 0), ({'pipeline1': None}, 0), ({'pipeline1': ''}, 0),
])
def test_valid_pipeline_preserves_webhook_behavior(service, fields, expected):
    payload = {'pair1': 'btc/usdt', 'bias1': 'LONG', **fields}
    response = service.app.test_client().post('/webhook/test-token', json=payload)
    assert response.status_code == 200
    assert response.get_json() == {
        'status': 'received', 'saved': True, 'hash': service.stable_hash(payload),
        'pair1': 'BTCUSDT', 'decision': 'APPROVED', 'should_execute': True, 'reason': '',
    }
    row = stored(service)[0]
    assert row[:3] == ('APPROVED', 1, '')
    assert json.loads(row[3]) == payload
    message = service.send_telegram_async.call_args.args[0]
    assert f'Pipeline: `{expected:.2f}`' in message
    assert ('HIGH PROBABILITY ALERT' in message) == (expected >= 1.2)


@pytest.mark.parametrize('value,status', [('1.5', 200), ('N/A', 400)])
def test_duplicate_stays_non_executable(service, value, status):
    client = service.app.test_client()
    payload = {'pair1': 'BTCUSDT', 'pipeline1': value}
    assert client.post('/webhook/test-token', json=payload).status_code == status
    response = client.post('/webhook/test-token', json=payload)
    assert response.status_code == status
    assert response.get_json()['saved'] is False
    assert response.get_json()['should_execute'] is False
    assert response.get_json()['reason'] == 'DUPLICATE_SIGNAL'
    assert len(stored(service)) == 1


def test_valid_pipeline_with_open_trade_stays_rejected(service):
    conn = service.db()
    conn.execute("INSERT INTO open_trades(pair,status) VALUES ('BTCUSDT','OPEN')")
    conn.commit()
    conn.close()
    response = service.app.test_client().post(
        '/webhook/test-token', json={'pair1': 'BTCUSDT', 'pipeline1': '1.5'}
    )
    assert response.status_code == 200
    assert response.get_json()['reason'] == 'OPEN_TRADE_EXISTS'
    assert response.get_json()['should_execute'] is False
    assert stored(service)[0][:3] == ('REJECTED', 0, 'OPEN_TRADE_EXISTS')
