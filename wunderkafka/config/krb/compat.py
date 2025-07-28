try:
    from requests.auth import AuthBase
except ImportError:

    class AuthBase: ...  # type: ignore[no-redef]
