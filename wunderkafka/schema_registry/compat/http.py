try:
    from requests import Session, Response
except ImportError:
    from httpx import Client as Session
    from httpx import Response
