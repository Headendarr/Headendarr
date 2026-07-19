import time

from backend.utils import clean_text

from .common import cso_session_manager


def vod_hls_output_session_key(source_id: int, profile: str, start_seconds: int = 0) -> str:
    """Return the identity shared by every client of one finite VOD HLS output."""
    source_value = int(source_id or 0)
    profile_value = clean_text(profile).lower()
    start_value = max(0, int(start_seconds or 0))
    return f"cso-vod-hls-output-{source_value}-{profile_value}-start{start_value}"


async def reusable_vod_hls_output_snapshot(output_session_key: str) -> dict[str, object] | None:
    """Return bounded lifecycle details only when an output is safe to serve or join."""
    session = await cso_session_manager.get_output_session(output_session_key)
    if session is None:
        return None
    snapshot_hook = getattr(session, "vod_hls_reuse_snapshot", None)
    if not callable(snapshot_hook):
        return None
    snapshot = await snapshot_hook()
    if not bool(snapshot.get("reusable")):
        return None
    return {
        "output_key": str(output_session_key),
        "completion_state": clean_text(snapshot.get("completion_state")) or "unknown",
        "completed": bool(snapshot.get("completed")),
        "client_count": max(0, int(snapshot.get("client_count") or 0)),
        "reservation_owner": clean_text(snapshot.get("reservation_owner")) or None,
        "reservation_slot_id": clean_text(snapshot.get("reservation_slot_id")) or None,
        "input_kind": "upstream" if bool(snapshot.get("input_is_upstream")) else "completed_local_cache",
        "observed_at": int(time.time()),
    }
