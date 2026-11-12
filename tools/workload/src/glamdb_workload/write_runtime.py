"""Shared exact-request benchmark write outcomes and Locust loading runtime."""

import hmac
import json
import os

from glamdb_sdk import HTTPError, MutationRequest, OutcomeUnknownError, ProtocolError, TransportError

from .runtime import error_code


def _receipt_matches_rows(request, result):
    rows = len(request.to_dict()['rows'])
    try:
        if not isinstance(result.get('snapshot_id'), str):
            return False
        if isinstance(request, MutationRequest):
            # Source primary keys are unique and the target starts empty. COW
            # may rewrite existing rows, but the live-row delta must equal input.
            return (result.get('receipt_version') == 3 and result.get('mutation_kind') == 'upsert'
                    and result.get('snapshot_created') is True
                    and int(result.get('input_rows', -1)) == rows
                    and int(result.get('rows_affected', -1)) == rows
                    and int(result['rows_written']) - int(result['rows_removed']) == rows)
        return int(result.get('rows_appended', -1)) == rows
    except (KeyError, TypeError, ValueError):
        return False


def record_outcome(log, request, outcome, *, authoritative=False):
    state = outcome.outcome
    code = outcome.error.code if outcome.error else None
    result = outcome.result
    if outcome.operation_id != request.operation_id:
        state, code = 'protocol_error', 'RECEIPT_IDENTITY_MISMATCH'
    elif state == 'committed':
        if not isinstance(result, dict) or not hmac.compare_digest(result.get('payload_hash', ''), request.payload_hash):
            state, code = 'conflict', 'OPERATION_CONFLICT'
        elif not _receipt_matches_rows(request, result):
            state, code = 'protocol_error', 'RECEIPT_ROW_COUNT_MISMATCH'
    elif state == 'definitely_not_committed' and not authoritative:
        # Admission can reject before binding an ID. A failed POST proves that
        # attempt failed, but only an outcome lookup proves a permanent receipt.
        state = 'failure_lookup_required'
    hint = outcome.error.retry_hint if outcome.error else None
    if not authoritative:
        log.db.execute('''CREATE TABLE IF NOT EXISTS attempt_outcomes (
            operation_id TEXT NOT NULL,attempt INTEGER NOT NULL,server_outcome TEXT NOT NULL,
            phase TEXT,error_code TEXT,retry_hint TEXT,result TEXT,
            PRIMARY KEY(operation_id,attempt))''')
        log.db.execute('INSERT INTO attempt_outcomes VALUES (?,(SELECT attempts FROM operations '
                       'WHERE operation_id=?),?,?,?,?,?) ON CONFLICT DO NOTHING',
                       (request.operation_id, request.operation_id, outcome.outcome, outcome.phase,
                        code, hint, None if result is None else json.dumps(result)))
    log.mark(request.operation_id, state, error_code=code,
             outcome={'server_outcome': outcome.outcome, 'phase': outcome.phase, 'result': result,
                      'error_code': code, 'retry_hint': hint})
    return state


def _send_once(client, log, request, *, deadline):
    log.attempted(request.operation_id)
    try:
        send = client.upsert if isinstance(request, MutationRequest) else client.append
        return record_outcome(log, request, send(request, deadline=deadline))
    except (TransportError, ProtocolError, HTTPError) as error:
        state = ('commit_unknown' if isinstance(error, OutcomeUnknownError) else
                 'transport_error' if isinstance(error, TransportError) else 'protocol_error')
        log.mark(request.operation_id, state, error_code=error_code(error))
        return state
    except BaseException:
        log.mark(request.operation_id, 'transport_error', error_code='INTERRUPTED')
        raise


def _locust_runtime():
    # This writer uses synchronous I/O under one publication gate. Locust owns
    # scheduling/statistics, but must not patch a caller's existing TLS objects.
    previous = os.environ.get('LOCUST_SKIP_MONKEY_PATCH')
    os.environ['LOCUST_SKIP_MONKEY_PATCH'] = '1'
    try:
        from locust import User, task
        from locust.env import Environment
        from locust.exception import StopUser
        return User, task, Environment, StopUser
    finally:
        if previous is None:
            os.environ.pop('LOCUST_SKIP_MONKEY_PATCH', None)
        else:
            os.environ['LOCUST_SKIP_MONKEY_PATCH'] = previous
