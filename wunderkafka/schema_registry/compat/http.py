try:
    from requests import Session, Response
except ImportError:
    # confluent-kafka>=2.16 on Python>=3.10 ships httpx2 instead of httpx
    try:
        from httpx2 import Client as Session
        from httpx2 import Response
    except ImportError:
        from httpx import Client as Session
        from httpx import Response
