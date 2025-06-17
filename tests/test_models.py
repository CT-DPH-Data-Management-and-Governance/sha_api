import pytest
from pydantic import ValidationError

# Adjust this import based on your library's structure.
# It assumes your classes are in a file at `src/census_helpers/models.py`
from dataops.models import CensusAPIEndpoint


# Test for the CensusAPIEndpoint class
class TestCensusAPIEndpoint:
    def test_initialization_success(self):
        """Tests successful creation with valid parameters."""
        endpoint = CensusAPIEndpoint(
            year=2021,
            dataset="acs/acs1",
            variables=["NAME", "B01001_001E"],
            geography="state:*",
        )
        assert endpoint.year == 2021
        assert endpoint.dataset == "acs/acs1"

    def test_initialization_failure_invalid_year(self):
        """Tests that Pydantic's validation catches bad input."""
        with pytest.raises(ValidationError):
            CensusAPIEndpoint(
                year=1980,  # Invalid year, should be > 1989
                dataset="acs/acs1",
                variables=["NAME"],
                geography="state:*",
            )

    def test_from_url_success(self):
        """Tests that the from_url classmethod correctly parses a URL."""
        url = (
            "https://api.census.gov/data/2021/acs/acs1?get=NAME,B01001_001E&for=state:*"
        )
        endpoint = CensusAPIEndpoint.from_url(url)
        assert endpoint.year == 2021
        assert endpoint.dataset == "acs/acs1"
        assert endpoint.variables == ["NAME", "B01001_001E"]
        assert endpoint.geography == "for:state:*"

    def test_from_url_failure_bad_url(self):
        """Tests that from_url raises an error for a malformed URL."""
        url = "https://not-a-census-url.com"
        with pytest.raises(ValueError):
            CensusAPIEndpoint.from_url(url)

    def test_api_key_logic(self, monkeypatch):
        """Tests that api_key is handled correctly (user-provided vs. environment)."""
        # 1. Test with user-provided key
        endpoint_user_key = CensusAPIEndpoint(
            year=2021,
            dataset="acs/acs1",
            variables=["NAME"],
            geography="us:1",
            api_key="USER_KEY",
        )
        assert "key=USER_KEY" in endpoint_user_key.full_url

        # 2. Test with environment variable
        monkeypatch.setenv("CENSUS_API_KEY", "ENV_KEY")
        endpoint_env_key = CensusAPIEndpoint(
            year=2021, dataset="acs/acs1", variables=["NAME"], geography="us:1"
        )
        assert "key=ENV_KEY" in endpoint_env_key.full_url
