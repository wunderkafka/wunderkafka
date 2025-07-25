try:
    from requests.auth import AuthBase
except ImportError:
    from httpx import Client as Session, Response

    class AuthBase:
        ...