import zipfile

from tools.acceptance.sanitize_trace import sanitize


def test_trace_credentials_are_removed_from_actions_and_network_members(tmp_path):
    token = 'private-bearer-canary'
    source, target = tmp_path / 'raw.zip', tmp_path / 'trace.zip'
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('trace.trace', '{"value":"' + token + '"}')
        archive.writestr('trace.network', 'Authorization: Bearer ' + token)
        archive.writestr('resources/screenshot.png', b'unchanged-binary')
    sanitize(source, target, token)
    with zipfile.ZipFile(target) as archive:
        assert all(token.encode() not in archive.read(name) for name in archive.namelist())
        assert b'Bearer [redacted]' in archive.read('trace.network')
        assert archive.read('resources/screenshot.png') == b'unchanged-binary'
