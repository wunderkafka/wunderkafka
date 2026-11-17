import pytest

from wunderkafka.schema_registry.credentials import BasicCredentials, split_url_credentials


@pytest.mark.parametrize(
    ("url", "username", "password"),
    [
        pytest.param("http://ml:secret@host", "ml", "secret", id="plain"),
        pytest.param("http://ml:p%40ss%3Aword@host", "ml", "p@ss:word", id="percent_encoded_is_decoded"),
        pytest.param("http://ml:a@b@host", "ml", "a@b", id="raw_at_sign_in_password"),
        pytest.param("http://ml:a:b@host", "ml", "a:b", id="raw_colon_in_password"),
        pytest.param("http://ml@host", "ml", "", id="username_only"),
        pytest.param("http://:secret@host", "", "secret", id="password_only"),
    ],
)
def test_credentials_are_extracted_from_url(url: str, username: str, password: str) -> None:
    result = split_url_credentials(url)

    assert result.credentials == BasicCredentials(username=username, password=password)


@pytest.mark.parametrize(
    ("url", "expected_url"),
    [
        pytest.param(
            "https://ml:secret@registry.example:8081/api?x=1#top",
            "https://registry.example:8081/api?x=1#top",
            id="port_path_query_and_fragment_are_kept",
        ),
        pytest.param("http://ml:a@b@host", "http://host", id="raw_at_sign_in_password"),
        pytest.param("http://ml@host/api", "http://host/api", id="username_only"),
    ],
)
def test_url_is_returned_without_credentials(url: str, expected_url: str) -> None:
    result = split_url_credentials(url)

    assert result.url == expected_url


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("http://host:8081/api", id="plain"),
        pytest.param("http://host/api?", id="trailing_question_mark_is_not_normalized"),
        pytest.param("HTTP://Host/api", id="case_is_not_normalized"),
    ],
)
def test_url_without_credentials_is_returned_untouched(url: str) -> None:
    result = split_url_credentials(url)

    assert result.url == url
    assert result.credentials is None
