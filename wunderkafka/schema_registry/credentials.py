import urllib.parse
from typing import Optional

from pydantic import BaseModel


class BasicCredentials(BaseModel):
    username: str
    password: str


class SplitURL(BaseModel):
    url: str
    credentials: Optional[BasicCredentials] = None


def split_url_credentials(url: str) -> SplitURL:
    """
    Separate ``user:password@`` from a URL, so the URL can be logged and raised in exceptions safely.

    Percent-encoded credentials are decoded, the way ``requests`` and ``httpx`` decode them.

    Args:
        url: URL which may carry credentials in its authority part.

    Returns:
        URL without credentials (scheme, host, port, path and query are untouched) and the extracted
        credentials, or ``None`` if the URL carried none.
    """
    parts = urllib.parse.urlsplit(url)
    if parts.username is None and parts.password is None:
        return SplitURL(url=url)
    credentials = BasicCredentials(
        username=urllib.parse.unquote(parts.username or ""),
        password=urllib.parse.unquote(parts.password or ""),
    )
    clean_netloc = parts.netloc.rpartition("@")[2]
    return SplitURL(url=parts._replace(netloc=clean_netloc).geturl(), credentials=credentials)
