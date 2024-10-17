import pytest

from dagfactory import settings


@pytest.mark.parametrize(
    "value,expected_response",
    [
        ("f", False),
        ("false", False),
        ("0", False),
        ("", False),
        ("none", False),
        ("True", True),
        ("true", True),
        ("1", True),
    ],
)
def test_convert_to_boolean(value, expected_response):
    assert settings.convert_to_boolean(value) == expected_response
