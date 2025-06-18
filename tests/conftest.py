# tests/conftest.py
import pytest


@pytest.fixture
def sample_acs_json():
    """Provides a sample JSON response from a Census ACS API call."""
    return [
        ["NAME", "B01001_001E", "B01001_001M", "state"],
        ["Alabama", "5030053", "null", "01"],
        ["Alaska", "736081", "null", "02"],
        ["Arizona", "7158923", "null", "04"],
    ]
