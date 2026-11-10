"""Fail-closed checks for P-IO receipts and the bounded success strace."""

from collections import Counter
import errno
import json
from pathlib import Path, PurePosixPath
import posixpath
import re
from urllib.parse import urlsplit


KINDS = {"Data", "Manifest", "ManifestList", "Metadata"}
OPERATIONS = {"OpenDir", "MakeDir", "CreateExclusive", "OpenRead", "Write", "PRead", "Stat", "StatFs",
              "MeasureTree", "SyncFile", "SyncDir", "Close", "Publish"}
APPEND2 = "33333333-3333-7333-8333-333333333335"
FOOTERS = {"data-footer": "Data", "manifest-trailer": "Manifest", "manifest-list-trailer": "ManifestList"}
MUTATION_DESCRIPTORS = {"fallocate": 0, "copy_file_range": 2, "splice": 2,
                        "sendfile": 0, "sendfile64": 0, "vmsplice": 0, "tee": 1,
                        "ioctl": 0, "mmap": 4, "mmap2": 4}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path, lines=False):
    with Path(path).open("rb") as stream:
        raw = stream.read(16 * 1024 * 1024 + 1)
    require(len(raw) <= 16 * 1024 * 1024, f"artifact exceeds bound: {path}")
    try:
        def decode(value):
            if isinstance(value, str):
                return value.encode("latin1").decode("utf-8", errors="surrogateescape")
            if isinstance(value, list):
                return [decode(item) for item in value]
            if isinstance(value, dict):
                return {key: decode(item) for key, item in value.items()}
            return value

        def document(value):
            return decode(value) if isinstance(value, dict) and value.get("string_encoding") == "latin1-bytes" else value

        return [document(json.loads(line)) for line in raw.splitlines()] if lines else document(json.loads(raw))
    except (ValueError, RecursionError) as error:
        raise ValueError(f"invalid JSON artifact: {path}") from error


def inside(path, root):
    parsed, root = PurePosixPath(path), PurePosixPath(root)
    return parsed == root or root in parsed.parents


def normal_path(path):
    require(isinstance(path, str) and path.startswith("/") and "\0" not in path and
            posixpath.normpath(path) == path, "expected an absolute normalized physical path")
    return path


def physical_path(uri, root):
    require(isinstance(uri, str), "invalid store URI")
    parsed = urlsplit(uri)
    require(parsed.scheme == "store" and parsed.netloc and not parsed.query and not parsed.fragment and
            "%" not in uri and parsed.path.startswith("/"), "invalid store URI")
    path = normal_path(str(root) + parsed.path)
    require(inside(path, root) and path != str(root), "store URI escapes root")
    return path


def load_events(path):
    events = read_json(path, lines=True)
    require(bool(events), "empty IO event trace")
    for sequence, event in enumerate(events, 1):
        require(isinstance(event, dict) and event.get("string_encoding") == "latin1-bytes", "invalid event encoding")
        require(type(event.get("seq")) is int and event["seq"] == sequence, "IO event sequence is not contiguous")
        require(isinstance(event.get("op"), str) and event["op"] in OPERATIONS and
                isinstance(event.get("kind"), str) and event["kind"] in KINDS | {"Other"}, "unknown IO event operation/kind")
        require(isinstance(event.get("path"), str) and isinstance(event.get("attempt_id"), str), "invalid event path/attempt")
        require(all(type(event.get(key)) is int and event[key] >= 0 for key in ("bytes", "result")), "invalid IO event result")
        require(isinstance(event.get("references"), list) and all(isinstance(path, str) for path in event["references"]),
                "invalid publication references")
    return events


def check_durability(events, append_start):
    files = {}
    for event in events:
        op, path, sequence, error = event["op"], event["path"], event["seq"], event["result"]
        if op == "MakeDir" and append_start is not None and sequence > append_start:
            raise ValueError("directory creation attempted during append")
        if op == "CreateExclusive" and error == 0:
            require(path not in files, "duplicate exclusive create")
            files[path] = {"kind": event["kind"], "attempt_id": event["attempt_id"],
                           "write": 0, "sync": 0, "close": 0, "directory": 0, "failed": False}
        record = files.get(path)
        if record is not None and event["attempt_id"] == record["attempt_id"]:
            if ((error and not (op == "Write" and error == errno.EINTR)) and
                    (op in {"Write", "SyncFile"} or (op == "Close" and not record["close"]))):
                record["failed"] = True
            if op == "Write" and not error and event["bytes"] == 0:
                record["failed"] = True
            if not error:
                if op == "Write" and event["bytes"]:
                    record["write"] = sequence
                elif op == "SyncFile":
                    record["sync"] = sequence
                elif op == "Close" and not record["close"]:
                    record["close"] = sequence
        if op == "SyncDir":
            for file_path, record in files.items():
                if str(PurePosixPath(file_path).parent) == path and record["close"]:
                    if error and not record["directory"]:
                        record["failed"] = True
                    else:
                        record["directory"] = sequence
        if op == "Publish":
            for reference in event["references"]:
                require(reference in files and is_durable(files[reference]), "publication references an unfinalized file")
    return files


def is_durable(record):
    return not record["failed"] and 0 < record["write"] < record["sync"] < record["close"] < record["directory"]


def verify_receipt(directory, root, case):
    root = normal_path(str(root))
    directory = Path(directory)
    result = read_json(directory / "result.json")
    inventory = read_json(directory / "inventory.json")
    events = load_events(directory / "io-events.jsonl")
    require(isinstance(result, dict) and result.get("schema_version") == 1 and result.get("case") == case,
            "missing or mismatched case receipt")
    require(result.get("status") == "PASS" and isinstance(result.get("message"), str), "native assertions failed")
    expected = "published" if case == "success" or case.endswith("-short") or case == "write-eintr" else "rejected"
    require(result.get("expected_outcome") == result.get("actual_outcome") == expected, "unexpected operation outcome")
    hits = result.get("injection_hits")
    require(type(hits) is int and (hits == 0 if case == "success" else hits > 0), "fault injection was not matched")
    first_error = result.get("first_error")
    require(type(first_error) is int and (first_error == 0 if expected == "published" or case == "outside-attempt" else first_error > 0),
            "first-error evidence does not match outcome")
    for key in ("first_close", "second_close"):
        require(key in result and (result[key] is None or isinstance(result[key], str)), "missing close outcome")
    if case.endswith("-repeat-close"):
        require(result["first_close"] not in (None, "OK") and result["second_close"] == "OK",
                "repeated Close outcomes are not evidenced")
    phases = result.get("phases")
    require(isinstance(phases, list) and all(isinstance(phase, dict) and isinstance(phase.get("name"), str) and
            type(phase.get("seq")) is int and 0 <= phase["seq"] <= len(events) for phase in phases), "invalid phase markers")
    append = [phase["seq"] for phase in phases if phase["name"] == "append1-begin"]
    require(len(append) <= 1, "duplicate append phase")
    require(case == "create-dir-sync" or len(append) == 1, "missing append phase")
    files = check_durability(events, append[0] if append else None)
    require(isinstance(inventory, dict) and inventory.get("schema_version") == 1 and
            isinstance(inventory.get("published_roots"), list) and isinstance(inventory.get("files"), list) and
            isinstance(inventory.get("orphans"), list), "invalid file inventory")
    roots = inventory["published_roots"]
    require(roots == result.get("published_roots") and all(isinstance(path, str) for path in roots) and
            len(roots) == len(set(roots)), "published roots disagree with inventory")
    require("latest_root" in inventory and inventory["latest_root"] == (roots[-1] if roots else None),
            "authoritative latest root disagrees with publication history")
    expected_roots = 3 if expected == "published" else (0 if case == "create-dir-sync" else 2)
    require(len(roots) == expected_roots, "unexpected number of published roots")
    logical_paths, physical_paths = set(), set()
    for entry in inventory["files"]:
        require(isinstance(entry, dict), "invalid file inventory entry")
        path = normal_path(entry.get("physical_path"))
        logical = entry.get("path")
        require(inside(path, PurePosixPath(root)) and path != root, "inventory file escapes store")
        require(isinstance(logical, str) and logical.startswith("store://") and logical not in logical_paths and
                path not in physical_paths, "duplicate or invalid inventory path")
        logical_paths.add(logical)
        physical_paths.add(path)
        require(physical_path(logical, root) == path, "logical and physical inventory paths disagree")
        require(entry.get("format_verified") is True and entry.get("kind") in KINDS and
                isinstance(entry.get("attempt_id"), str) and entry["attempt_id"], "unverified file receipt")
        require(type(entry.get("length")) is int and entry["length"] > 0, "invalid file length")
        require(path in files and is_durable(files[path]) and files[path]["kind"] == entry["kind"] and
                files[path]["attempt_id"] == entry["attempt_id"], "inventory has no matching durable stream")
        actual = Path(path)
        require(actual.resolve(strict=True) == actual and actual.is_file() and actual.stat().st_size == entry["length"],
                "inventory length/file differs from disk")
    require(inventory["latest_root"] is None or inventory["latest_root"] in logical_paths,
            "latest root is missing from reachable inventory")
    publications = [event for event in events if event["op"] == "Publish"]
    require(len(publications) == len(roots), "missing or extra publication evidence")
    for publication, uri in zip(publications, roots):
        references = publication["references"]
        require(publication["result"] == 0 and len(references) == len(set(references)) and
                physical_path(uri, root) in references, "publication does not identify its metadata root")
    require((set(publications[-1]["references"]) if publications else set()) == physical_paths,
            "final publication references differ from reachable inventory")
    for orphan in inventory["orphans"]:
        normal_path(orphan)
        require(inside(orphan, PurePosixPath(root)) and orphan not in physical_paths, "invalid orphan classification")
    return result, inventory, events


def footer_ordinals(events):
    counts = Counter(event["kind"] for event in events if event["op"] == "Write" and event["attempt_id"] == APPEND2)
    require(all(counts[kind] > 0 for kind in FOOTERS.values()), "success does not calibrate all footer writes")
    return {case: counts[kind] for case, kind in FOOTERS.items()}


def split_arguments(text):
    arguments, start, depth, quoted, escaped = [], 0, 0, False, False
    for index, char in enumerate(text):
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "([{<":
            depth += 1
        elif char in ")]}>":
            depth -= 1
        elif char == "," and depth == 0:
            arguments.append(text[start:index].strip())
            start = index + 1
    require(not quoted and depth == 0, "unbalanced syscall arguments")
    return [*arguments, text[start:].strip()]


def c_string(text):
    require(len(text) >= 2 and text[0] == text[-1] == '"', "truncated or nonliteral syscall path")
    raw = bytearray()
    index = 1
    while index < len(text) - 1:
        char = text[index]
        if char != "\\":
            raw.extend(char.encode("utf-8"))
        else:
            index += 1
            require(index < len(text) - 1, "invalid path escape")
            char = text[index]
            if char in '"\\':
                raw.append(ord(char))
            elif char in "01234567":
                match = re.match(r"[0-7]{1,3}", text[index:])
                raw.append(int(match[0], 8))
                index += len(match[0]) - 1
            elif char == "x" and re.match(r"[0-9a-fA-F]{2}", text[index + 1:index + 3]):
                raw.append(int(text[index + 1:index + 3], 16))
                index += 2
            else:
                raise ValueError("unsupported syscall path escape")
        index += 1
    return raw.decode("utf-8", errors="surrogateescape")


def descriptor_path(text):
    match = re.fullmatch(r"(?:\d+|AT_FDCWD)<(/[^>]*)>", text)
    return posixpath.normpath(c_string('"' + match[1] + '"')) if match else None


def syscall_path(argument, directory=None):
    path = c_string(argument)
    if not path.startswith("/"):
        base = descriptor_path(directory or "")
        require(base is not None, "unresolved relative syscall path")
        path = posixpath.join(base, path)
    return normal_path(posixpath.normpath(path))


def verify_strace(paths, root, events):
    root = PurePosixPath(normal_path(str(root)))
    expected = Counter((event["op"], event["path"], event["result"]) for event in events
                       if event["op"] in {"CreateExclusive", "OpenRead", "MakeDir"})
    writes = Counter((event["path"], event["bytes"], event["result"]) for event in events if event["op"] == "Write")
    syncs = Counter((event["path"], event["result"]) for event in events
                    if event["op"] in {"SyncFile", "SyncDir"})
    sync_paths = {path for path, _ in syncs}
    registered = {event["path"] for event in events if event["op"] == "CreateExclusive" and event["result"] == 0}
    append_files = {event["path"] for event in events if event["op"] == "CreateExclusive" and event["kind"] == "Data"}
    records = []
    require(bool(paths), "missing strace files")
    for path in paths:
        with Path(path).open("r", encoding="utf-8", errors="surrogateescape") as stream:
            text = stream.read(32 * 1024 * 1024 + 1)
        require(len(text) <= 32 * 1024 * 1024, "strace exceeds size bound")
        terminated = False
        for line in text.splitlines():
            match = re.fullmatch(r"(\d+\.\d+)\s+(.+)", line)
            require(match is not None, "strace requires complete -ttt records")
            body = match[2]
            if body == "+++ exited with 0 +++":
                terminated = True
                continue
            if re.fullmatch(r"(?:exit|exit_group)\(0\)\s+=\s+\?", body):
                continue
            require(not body.startswith(("+++", "---")), "unexpected process/signal status in strace")
            call = re.fullmatch(r"(\w+)\((.*)\)\s+=\s+(-?\d+)(.*)", body)
            require(call is not None, "incomplete or unrecognized syscall record")
            returned = int(call[3])
            error = 0
            if returned < 0:
                error_name = call[4].strip().split(" ", 1)[0]
                require(hasattr(errno, error_name), "unknown syscall errno")
                error = getattr(errno, error_name)
            records.append((float(match[1]), call[1], split_arguments(call[2]), returned, error, call[4]))
        require(terminated, "strace lacks successful process termination")
    append_started = False
    finalized = {path: {"write": 0, "sync": 0, "close": 0, "directory": 0, "failed": False} for path in registered}
    for sequence, (_, operation, args, returned, error, suffix) in enumerate(sorted(records, key=lambda record: record[0]), 1):
        require(operation not in {"io_submit", "io_uring_setup", "io_uring_register", "io_uring_enter"},
                "unsupported asynchronous native IO")
        if operation in {"open", "openat", "openat2", "creat"}:
            at = operation in {"openat", "openat2"}
            require(len(args) >= (3 if at else 2), "invalid open arguments")
            path = syscall_path(args[1] if at else args[0], args[0] if at else None)
            actual = descriptor_path(str(returned) + suffix)
            if not inside(path, root) and not (actual and inside(actual, root)):
                continue
            require(actual is None or actual == path, "open resolves to a different physical path")
            flags = args[2] if at else args[1]
            require(any(mode in flags for mode in ("O_RDONLY", "O_WRONLY", "O_RDWR")) and "0x" not in flags,
                    "unrecognized native open flags")
            require(operation != "creat" and "O_TRUNC" not in flags and "O_TMPFILE" not in flags and "O_APPEND" not in flags,
                    "unsafe native create/truncate in store")
            if "O_DIRECTORY" in flags:
                require("O_CREAT" not in flags and "O_WRONLY" not in flags and "O_RDWR" not in flags, "writable directory open")
                continue
            creation = "O_CREAT" in flags
            if creation:
                require("O_EXCL" in flags, "new path lacks O_EXCL")
            else:
                require("O_WRONLY" not in flags and "O_RDWR" not in flags, "native writable open bypasses exclusive create")
            key = ("CreateExclusive" if creation else "OpenRead", path, error)
            require(expected[key] > 0, "unregistered native file open/create: " + path)
            expected[key] -= 1
            append_started |= path in append_files
        elif operation in {"mkdir", "mkdirat"}:
            path = syscall_path(args[1] if operation == "mkdirat" else args[0], args[0] if operation == "mkdirat" else None)
            if inside(path, root):
                require(not append_started, "native mkdir during append")
                key = ("MakeDir", path, error)
                if expected[key] > 0:
                    expected[key] -= 1
                else:
                    require(path == str(root), "unregistered native directory creation")
        elif operation in {"write", "pwrite64", "writev", "pwritev", "pwritev2"}:
            path = descriptor_path(args[0])
            require(error or ("<" in args[0] and args[0].endswith(">")), "write descriptor lacks -yy identity")
            if path and inside(path, root):
                require(operation == "write" and path in registered, "unregistered native write")
                key = (path, max(returned, 0), error)
                require(writes[key] > 0, "native write bypasses tracked stream")
                writes[key] -= 1
                require(not error and returned > 0, "failed native write in success trace")
                finalized[path]["write"] = sequence
        elif operation in {"fsync", "fdatasync"}:
            path = descriptor_path(args[0])
            require(error or path is not None, "sync descriptor lacks -yy identity")
            if path and (inside(path, root) or path in sync_paths):
                require(operation == "fsync" and syncs[(path, error)] > 0 and not error, "unregistered native sync")
                syncs[(path, error)] -= 1
                if path in finalized:
                    finalized[path]["sync"] = sequence
                else:
                    for file_path, state in finalized.items():
                        if str(PurePosixPath(file_path).parent) == path and state["close"]:
                            state["directory"] = sequence
        elif operation == "close":
            path = descriptor_path(args[0])
            if path in finalized and not finalized[path]["close"]:
                require(not error, "failed native writer close")
                finalized[path]["close"] = sequence
        elif operation in MUTATION_DESCRIPTORS:
            index = MUTATION_DESCRIPTORS[operation]
            require(len(args) > index, "invalid native mutation arguments")
            target = args[index]
            if operation in {"mmap", "mmap2"} and target == "-1" and "MAP_ANONYMOUS" in args[3]:
                continue
            path = descriptor_path(target)
            require(error or re.fullmatch(r"\d+<[^>]+>", target) is not None,
                    "mutation descriptor lacks -yy identity")
            require(path is None or not inside(path, root), "unsupported native store mutation")
        elif operation.startswith(("unlink", "rename", "link", "symlink", "truncate", "ftruncate", "rmdir")):
            if operation.startswith("ftruncate"):
                mutation_paths = [descriptor_path(args[0])]
                require(mutation_paths[0] is not None, "unresolved native mutation descriptor")
            elif operation in {"renameat", "renameat2", "linkat"}:
                mutation_paths = [syscall_path(args[1], args[0]), syscall_path(args[3], args[2])]
            elif operation == "symlinkat":
                mutation_paths = [syscall_path(args[2], args[1])]
            elif operation == "unlinkat":
                mutation_paths = [syscall_path(args[1], args[0])]
            else:
                count = 2 if operation in {"rename", "link", "symlink"} else 1
                mutation_paths = [syscall_path(argument) for argument in args[:count]]
            require(not any(inside(path, root) for path in mutation_paths), "unexpected native store mutation")
        elif operation.startswith(("open", "creat")):
            require(str(root) not in str(args) + suffix, "unsupported native store open")
    require(not any(expected.values()), "strace is missing tracked file opens/directory creates")
    require(not any(writes.values()), "strace is missing tracked writes")
    require(not any(syncs.values()), "strace is missing tracked syncs")
    require(all(is_durable(state) for state in finalized.values()), "native stream lacks write/sync/close/directory ordering")
    return {"status": "PASS", "trace_files": len(paths), "syscalls": len(records)}
