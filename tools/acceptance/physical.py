"""Independent, read-only catalog → Iceberg → Parquet inspection.

Call only while the owning server is stopped and quiesce-store holds the lease.
This verification tool is not a transport metadata implementation.
"""
import hashlib
import json
from pathlib import Path
import sqlite3
import stat
import struct
from urllib.parse import quote, urlsplit
import uuid


def resolve_store_uri(root, store_id, uri):
    root = Path(root).resolve(strict=True)
    if str(uuid.UUID(store_id)) != store_id or not isinstance(uri, str):
        raise ValueError('invalid store identity/location')
    location = urlsplit(uri)
    parts = location.path.split('/')
    if (location.scheme != 'store' or location.netloc != store_id or location.query or location.fragment
            or not location.path.startswith('/warehouse/') or '%' in uri or '\\' in uri or '\0' in uri
            or any(part in ('', '.', '..') for part in parts[1:])):
        raise ValueError('location must be an exact owned warehouse URI')
    path = root
    for part in parts[1:]:
        path = path / part
        try:
            mode = path.lstat().st_mode
        except OSError as error:
            raise ValueError('referenced store file is absent') from error
        if stat.S_ISLNK(mode):
            raise ValueError('symlink in store location')
    if not path.is_file() or not path.resolve(strict=True).is_relative_to(root):
        raise ValueError('referenced store location is not an owned regular file')
    return path


def _avro(path):
    from fastavro import reader
    if path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError('metadata file exceeds inspection budget')
    with path.open('rb') as stream:
        records = list(reader(stream))
    if len(records) > 100000:
        raise ValueError('metadata entries exceed inspection budget')
    return records


def _parquet(root, store_id, table_uuid, entry):
    import pyarrow.parquet as parquet
    uri = entry['file_path']
    path = resolve_store_uri(root, store_id, uri)
    source = parquet.ParquetFile(path)
    metadata = source.metadata
    if metadata.num_rows != entry['record_count'] or path.stat().st_size != entry['file_size_in_bytes']:
        raise ValueError('manifest and physical Parquet disagree')
    fields = []
    for field in source.schema_arrow:
        field_id = (field.metadata or {}).get(b'PARQUET:field_id')
        if field_id is None:
            raise ValueError('Parquet field ID is missing')
        fields.append({'field_id': int(field_id), 'name': field.name, 'type': str(field.type)})
    groups = []
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        columns = []
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            statistics = column.statistics
            columns.append({'field_id': fields[column_index]['field_id'], 'name': column.path_in_schema,
                'compressed_bytes': str(column.total_compressed_size),
                'null_count': None if statistics is None else statistics.null_count,
                'has_min_max': bool(statistics and statistics.has_min_max)})
        groups.append({'ordinal': index, 'row_count': str(group.num_rows),
            'total_byte_size': str(group.total_byte_size), 'columns': columns})
    identity = hashlib.sha256()
    for value in ('glamdb-file-v1', table_uuid, uri):
        encoded = value.encode('utf-8')
        identity.update(struct.pack('<I', len(encoded)))
        identity.update(encoded)
    return {'file_id': identity.hexdigest(), 'location': uri,
        'path': str(path), 'record_count': str(metadata.num_rows),
        'compressed_bytes': str(path.stat().st_size), 'footer_bytes': str(metadata.serialized_size),
        'fields': fields, 'row_groups': groups}


def inspect_store(root, store_id, collection_id, selectors=('latest', 'initial')):
    """Read the authoritative catalog pointer, then only manifest membership."""
    root = Path(root).resolve(strict=True)
    catalog = root / 'catalog.sqlite'
    if catalog.is_symlink() or not catalog.is_file():
        raise ValueError('catalog must be an existing regular file')
    with sqlite3.connect('file:' + quote(str(catalog)) + '?mode=ro&immutable=1', uri=True) as connection:
        row = connection.execute("SELECT t.metadata_location,c.table_uuid,c.current_snapshot_id FROM collections c "
            "JOIN iceberg_tables t ON t.namespace='columnar' AND t.table_name=c.name WHERE c.collection_id=?",
            (collection_id,)).fetchone()
        header = connection.execute('SELECT store_id FROM store_header WHERE singleton=1').fetchone()
        all_roots = connection.execute("SELECT t.metadata_location,c.table_uuid FROM collections c "
            "JOIN iceberg_tables t ON t.namespace='columnar' AND t.table_name=c.name").fetchall()
        if not row or not header or header[0] != store_id:
            raise ValueError('catalog identity/collection mismatch')
    location, table_uuid, catalog_snapshot = row
    root_path = resolve_store_uri(root, store_id, location)
    if root_path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError('metadata root exceeds inspection budget')
    metadata = json.loads(root_path.read_text())
    if metadata['table-uuid'] != table_uuid or metadata.get('current-snapshot-id') != catalog_snapshot:
        raise ValueError('catalog and Iceberg root disagree')
    snapshots = {str(item['snapshot-id']): item for item in metadata.get('snapshots', [])}
    reachable = {root_path}
    inventories = {}
    for snapshot_id, snapshot in snapshots.items():
        manifest_list = resolve_store_uri(root, store_id, snapshot['manifest-list'])
        reachable.add(manifest_list)
        files = {}
        manifests = []
        for descriptor in _avro(manifest_list):
            manifest_path = resolve_store_uri(root, store_id, descriptor['manifest_path'])
            if descriptor['manifest_length'] != manifest_path.stat().st_size:
                raise ValueError('manifest list length differs from disk')
            reachable.add(manifest_path)
            manifests.append(str(manifest_path))
            for entry in _avro(manifest_path):
                if entry['status'] == 2:
                    continue
                if entry['status'] not in (0, 1) or entry['data_file'].get('content', 0) != 0:
                    raise ValueError('unsupported manifest entry')
                data = entry['data_file']
                if data['file_path'] in files:
                    raise ValueError('duplicate live file membership')
                file = _parquet(root, store_id, table_uuid, data)
                files[data['file_path']] = file
                reachable.add(Path(file['path']))
        inventories[snapshot_id] = {'snapshot_id': snapshot_id,
            'manifest_list': str(manifest_list), 'manifests': manifests, 'files': list(files.values()),
            'rows': str(sum(int(item['record_count']) for item in files.values())),
            'data_files': str(len(files)),
            'compressed_bytes': str(sum(int(item['compressed_bytes']) for item in files.values()))}
    views = {}
    for selector in selectors:
        selected = str(catalog_snapshot) if selector == 'latest' and catalog_snapshot is not None else selector
        if selected == 'initial' or (selector == 'latest' and catalog_snapshot is None):
            views[selector] = {'snapshot_id': 'initial', 'rows': '0', 'data_files': '0',
                'compressed_bytes': '0', 'files': [], 'manifests': [], 'manifest_list': None}
        elif selected in inventories:
            views[selector] = inventories[selected]
        else:
            raise ValueError('snapshot is not retained by the confirmed root')
    orphans = []
    # Other collections are not orphans of the selected collection. Account for
    # every retained root before identifying truly unreferenced warehouse files.
    for other_location, other_uuid in all_roots:
        if other_location == location:
            continue
        other_path = resolve_store_uri(root, store_id, other_location)
        if other_path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError('metadata root exceeds inspection budget')
        other_root = json.loads(other_path.read_text())
        if other_root['table-uuid'] != other_uuid:
            raise ValueError('other collection root identity differs from catalog')
        reachable.add(other_path)
        for snapshot in other_root.get('snapshots', []):
            manifest_list = resolve_store_uri(root, store_id, snapshot['manifest-list'])
            reachable.add(manifest_list)
            for descriptor in _avro(manifest_list):
                manifest = resolve_store_uri(root, store_id, descriptor['manifest_path'])
                reachable.add(manifest)
                for entry in _avro(manifest):
                    if entry['status'] in (0, 1):
                        reachable.add(resolve_store_uri(root, store_id, entry['data_file']['file_path']))
    for path in (root / 'warehouse').rglob('*'):
        if path.is_symlink():
            raise ValueError('symlink in warehouse')
        if path.is_file() and path not in reachable:
            orphans.append(str(path))
    return {'store_id': store_id, 'collection_id': collection_id, 'table_uuid': table_uuid,
        'catalog': str(catalog), 'metadata_location': location, 'metadata_path': str(root_path),
        'views': views, 'unreferenced_warehouse_files': sorted(orphans)}
