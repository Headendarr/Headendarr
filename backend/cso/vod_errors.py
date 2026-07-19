UPSTREAM_CAPACITY_ERROR_CODE = "upstream_capacity_reached"
UPSTREAM_CAPACITY_MESSAGE = (
    "The VOD provider has reached its connection limit. Stop another stream or try again shortly."
)
UPSTREAM_INVALID_MEDIA_ERROR_CODE = "upstream_invalid_media_response"
UPSTREAM_INVALID_MEDIA_MESSAGE = (
    "The VOD provider returned an error page instead of video. Try again later or choose another source."
)


def vod_error_payload(error_code: str, message: str, retryable: bool = True) -> dict[str, object]:
    return {
        "success": False,
        "error_code": error_code,
        "message": message,
        "retryable": bool(retryable),
    }
