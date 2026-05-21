from __future__ import annotations

import argparse
import json
import os
import msvcrt
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_OUTPUTS_ROOT = Path(r"D:\CompPjts\crawlFilesDev\outputs")
DEFAULT_STATE_FILE = Path(r"D:\CompPjts\insertFiles\workflow_records_state.json")
DEFAULT_LOCK_FILE = Path(r"D:\CompPjts\insertFiles\workflow_records_tracker.lock")
DEFAULT_ERROR_LOG_FILE = Path(r"D:\CompPjts\insertFiles\workflow_records_tracker.log")
DEFAULT_CHECKPOINT_LOG_FILE = Path(r"D:\CompPjts\insertFiles\workflow_records_checkpoint_history.log")


class WorkflowRecordsError(RuntimeError):
    pass


class CheckpointMissingError(WorkflowRecordsError):
    pass


class InvalidWorkflowRecordsFileError(WorkflowRecordsError):
    pass


class LockUnavailableError(WorkflowRecordsError):
    pass


@dataclass(frozen=True)
class FolderSnapshot:
    folder: str
    file_path: Path
    records: List[Dict[str, Any]]
    top_record_key: Optional[str]


KST = timezone(timedelta(hours=9))


def kst_now_iso() -> str:
    return datetime.now(KST).isoformat()


def read_json_with_retry(path: Path, retries: int = 3, delay_seconds: float = 0.2) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            text = path.read_text(encoding="utf-8")
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise InvalidWorkflowRecordsFileError(f"{path} does not contain a JSON object.")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < retries - 1:
                time.sleep(delay_seconds)
    raise InvalidWorkflowRecordsFileError(f"Failed to read JSON from {path}: {last_error}") from last_error


def stable_record_identity(record: Dict[str, Any]) -> str:
    return record_checkpoint_value(record)


def record_checkpoint_value(record: Dict[str, Any]) -> str:
    final_url = record.get("final_url")
    if isinstance(final_url, str) and final_url:
        return final_url

    record_key = record.get("record_key")
    if isinstance(record_key, str) and record_key:
        return record_key

    normalized = json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return sha256(normalized.encode("utf-8")).hexdigest()


def record_identity_candidates(record: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []

    record_key = record.get("record_key")
    if isinstance(record_key, str) and record_key:
        candidates.append(record_key)

    final_url = record.get("final_url")
    if isinstance(final_url, str) and final_url:
        candidates.append(final_url)

    normalized = json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    candidates.append(sha256(normalized.encode("utf-8")).hexdigest())
    return candidates


def extract_records(payload: Dict[str, Any], file_path: Path) -> List[Dict[str, Any]]:
    records = payload.get("records")
    if records is None:
        raise InvalidWorkflowRecordsFileError(f"{file_path} is missing a 'records' field.")
    if not isinstance(records, list):
        raise InvalidWorkflowRecordsFileError(f"{file_path} has a non-list 'records' field.")
    normalized_records: List[Dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise InvalidWorkflowRecordsFileError(
                f"{file_path} has a non-object record at index {index}."
            )
        normalized_records.append(record)
    return normalized_records


def discover_snapshots(outputs_root: Path) -> List[FolderSnapshot]:
    snapshots: List[FolderSnapshot] = []
    for folder_path in sorted(p for p in outputs_root.iterdir() if p.is_dir()):
        file_path = folder_path / "filter" / "workflow_records.json"
        if not file_path.is_file():
            continue
        payload = read_json_with_retry(file_path)
        records = extract_records(payload, file_path)
        top_record_key = record_checkpoint_value(records[0]) if records else None
        snapshots.append(
            FolderSnapshot(
                folder=folder_path.name,
                file_path=file_path,
                records=records,
                top_record_key=top_record_key,
            )
        )
    return snapshots


def load_state(state_file: Path) -> Dict[str, Any]:
    if not state_file.exists():
        return {"version": 1, "folders": {}}
    raw_text = state_file.read_text(encoding="utf-8").strip()
    if not raw_text:
        return {"version": 1, "folders": {}}
    payload = json.loads(raw_text)
    if not isinstance(payload, dict):
        raise WorkflowRecordsError(f"State file {state_file} is invalid.")
    payload.setdefault("version", 1)
    payload.setdefault("folders", {})
    if not isinstance(payload["folders"], dict):
        raise WorkflowRecordsError(f"State file {state_file} has an invalid 'folders' object.")
    return payload


def save_state(state_file: Path, state: Dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def append_error_log(log_file: Path, payload: Dict[str, Any]) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    entry = dict(payload)
    entry.setdefault("logged_at", kst_now_iso())
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False))
        handle.write("\n")


def append_checkpoint_history_log(log_file: Path, payload: Dict[str, Any]) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    entry = dict(payload)
    entry.setdefault("logged_at", kst_now_iso())
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False))
        handle.write("\n")


def acquire_lock(lock_file: Path) -> Any:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_file.open("a+b")
    try:
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            handle.close()
            raise LockUnavailableError(f"Another run is already active: {lock_file}") from exc

        handle.seek(0)
        handle.truncate()
        handle.write(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "created_at": kst_now_iso(),
                    "state": "active",
                },
                ensure_ascii=False,
            ).encode("utf-8")
        )
        handle.flush()
        os.fsync(handle.fileno())
        handle.seek(0)
        return handle
    except Exception:
        try:
            handle.close()
        except Exception:
            pass
        raise


def release_lock(handle: Any) -> None:
    try:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    except Exception:
        pass
    finally:
        try:
            handle.close()
        except Exception:
            pass


def slice_new_records(records: List[Dict[str, Any]], checkpoint_key: Optional[str]) -> List[Dict[str, Any]]:
    if not records:
        return []
    current_top_key = record_checkpoint_value(records[0])
    if checkpoint_key is None:
        return records
    if current_top_key == checkpoint_key:
        return []

    identities = [record_identity_candidates(record) for record in records]
    try:
        checkpoint_index = next(index for index, candidates in enumerate(identities) if checkpoint_key in candidates)
    except StopIteration as exc:
        raise CheckpointMissingError(
            "The previous checkpoint was not found in the current workflow_records.json."
        ) from exc
    return records[:checkpoint_index]


class WorkflowRecordTracker:
    def __init__(
        self,
        outputs_root: Path = DEFAULT_OUTPUTS_ROOT,
        state_file: Path = DEFAULT_STATE_FILE,
        checkpoint_log_file: Path = DEFAULT_CHECKPOINT_LOG_FILE,
    ):
        self.outputs_root = outputs_root
        self.state_file = state_file
        self.checkpoint_log_file = checkpoint_log_file

    def discover(self) -> List[FolderSnapshot]:
        return discover_snapshots(self.outputs_root)

    def run(self, initialize_missing: bool = True) -> Dict[str, Any]:
        state = load_state(self.state_file)
        folders_state: Dict[str, Any] = state["folders"]
        snapshots = self.discover()
        current_folders = {snapshot.folder for snapshot in snapshots}
        pruned_folders_state: Dict[str, Any] = {
            folder: folder_state
            for folder, folder_state in folders_state.items()
            if folder in current_folders
        }
        state["folders"] = pruned_folders_state
        folders_state = pruned_folders_state
        result: Dict[str, Any] = {
            "generated_at": kst_now_iso(),
            "outputs_root": str(self.outputs_root),
            "state_file": str(self.state_file),
            "folders": {},
        }

        for snapshot in snapshots:
            folder_state = folders_state.get(snapshot.folder)
            if folder_state is None:
                folder_state = {}
            checkpoint_key = folder_state.get("checkpoint_key")
            previous_checkpoint_key = checkpoint_key

            if not folder_state and initialize_missing:
                new_records = snapshot.records
                folders_state[snapshot.folder] = {
                    "checkpoint_key": snapshot.top_record_key,
                    "initialized": True,
                    "initialized_at": kst_now_iso(),
                    "updated_at": kst_now_iso(),
                }
                append_checkpoint_history_log(
                    self.checkpoint_log_file,
                    {
                        "event": "checkpoint_initialized",
                        "folder": snapshot.folder,
                        "file_path": str(snapshot.file_path),
                        "previous_checkpoint_key": previous_checkpoint_key,
                        "current_checkpoint_key": snapshot.top_record_key,
                        "record_count": len(snapshot.records),
                    },
                )
                result["folders"][snapshot.folder] = {
                    "new_records": new_records,
                    "new_count": len(new_records),
                    "checkpoint_key": snapshot.top_record_key,
                    "initialized": True,
                }
                continue

            changed = previous_checkpoint_key != snapshot.top_record_key
            if checkpoint_key is None:
                new_records = snapshot.records
            else:
                try:
                    new_records = slice_new_records(snapshot.records, checkpoint_key)
                except CheckpointMissingError:
                    new_records = snapshot.records
            result["folders"][snapshot.folder] = {
                "new_records": new_records,
                "new_count": len(new_records),
                "checkpoint_key": snapshot.top_record_key,
                "initialized": checkpoint_key is not None,
            }

            append_checkpoint_history_log(
                self.checkpoint_log_file,
                {
                    "event": "checkpoint_changed" if changed else "checkpoint_unchanged",
                    "folder": snapshot.folder,
                    "file_path": str(snapshot.file_path),
                    "previous_checkpoint_key": previous_checkpoint_key,
                    "current_checkpoint_key": snapshot.top_record_key,
                    "checkpoint_changed": changed,
                    "record_count": len(snapshot.records),
                    "new_record_count": len(new_records),
                },
            )

            folders_state[snapshot.folder] = {
                "checkpoint_key": snapshot.top_record_key,
                "initialized": True,
                "updated_at": kst_now_iso(),
            }
            append_checkpoint_history_log(
                self.checkpoint_log_file,
                {
                    "event": "state_persisted",
                    "folder": snapshot.folder,
                    "file_path": str(snapshot.file_path),
                    "persisted_checkpoint_key": folders_state[snapshot.folder]["checkpoint_key"],
                    "persisted_updated_at": folders_state[snapshot.folder]["updated_at"],
                    "record_count": len(snapshot.records),
                },
            )

        save_state(self.state_file, state)
        return result


def run_once(
    tracker: WorkflowRecordTracker,
    lock_file: Path,
    initialize_missing: bool,
) -> Dict[str, Any]:
    lock_handle = acquire_lock(lock_file)
    try:
        return tracker.run(initialize_missing=initialize_missing)
    finally:
        release_lock(lock_handle)


def extract_non_empty_new_records(output: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    folders = output.get("folders", {})
    if not isinstance(folders, dict):
        return {}

    new_records_by_folder: Dict[str, List[Dict[str, Any]]] = {}
    for folder, folder_output in folders.items():
        if not isinstance(folder_output, dict):
            continue
        new_records = folder_output.get("new_records", [])
        if isinstance(new_records, list) and new_records:
            new_records_by_folder[folder] = new_records
    return new_records_by_folder


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track incremental workflow_records.json updates.")
    parser.add_argument(
        "--outputs-root",
        type=Path,
        default=DEFAULT_OUTPUTS_ROOT,
        help="Root folder containing per-source output directories.",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_FILE,
        help="JSON file used to persist per-folder checkpoints.",
    )
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=DEFAULT_LOCK_FILE,
        help="File used to prevent overlapping runs.",
    )
    parser.add_argument(
        "--error-log-file",
        type=Path,
        default=DEFAULT_ERROR_LOG_FILE,
        help="File used to append loop errors and skipped runs.",
    )
    parser.add_argument(
        "--checkpoint-log-file",
        type=Path,
        default=DEFAULT_CHECKPOINT_LOG_FILE,
        help="File used to append checkpoint history events.",
    )
    parser.add_argument(
        "--no-initialize-missing",
        action="store_true",
        help="Do not baseline folders that have no checkpoint yet.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously with a fixed sleep interval between runs.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Sleep duration between loop iterations when --loop is enabled.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Maximum loop iterations when --loop is enabled. Use 0 for unlimited.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    tracker = WorkflowRecordTracker(
        outputs_root=args.outputs_root,
        state_file=args.state_file,
        checkpoint_log_file=args.checkpoint_log_file,
    )

    try:
        if not args.loop:
            output = run_once(
                tracker=tracker,
                lock_file=args.lock_file,
                initialize_missing=not args.no_initialize_missing,
            )
            new_records_by_folder = extract_non_empty_new_records(output)
            if new_records_by_folder:
                print(json.dumps(new_records_by_folder, ensure_ascii=False, indent=2))
        else:
            iteration = 0
            while True:
                iteration += 1
                try:
                    output = run_once(
                        tracker=tracker,
                        lock_file=args.lock_file,
                        initialize_missing=not args.no_initialize_missing,
                    )
                    new_records_by_folder = extract_non_empty_new_records(output)
                    if new_records_by_folder:
                        print(json.dumps(new_records_by_folder, ensure_ascii=False, indent=2))
                except LockUnavailableError as exc:
                    append_error_log(
                        args.error_log_file,
                        {
                            "level": "warning",
                            "type": "LockUnavailableError",
                            "message": str(exc),
                            "iteration": iteration,
                            "lock_file": str(args.lock_file),
                        },
                    )
                except WorkflowRecordsError as exc:
                    append_error_log(
                        args.error_log_file,
                        {
                            "level": "error",
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "iteration": iteration,
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    append_error_log(
                        args.error_log_file,
                        {
                            "level": "error",
                            "type": type(exc).__name__,
                            "message": f"Unexpected error: {exc}",
                            "iteration": iteration,
                        },
                    )
                if args.iterations > 0 and iteration >= args.iterations:
                    break
                time.sleep(max(0, args.interval_seconds))
            return 0
    except LockUnavailableError as exc:
        print(
            json.dumps(
                {
                    "skipped": True,
                    "reason": str(exc),
                    "lock_file": str(args.lock_file),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except WorkflowRecordsError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
