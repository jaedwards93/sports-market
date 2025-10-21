from util.request import ApiCall


def test_github_api_call_returns_json():
    # GitHub API is free and returns JSON every time
    url = "https://api.github.com/repos/psf/requests"

    api = ApiCall(
        key_name="DUMMY_KEY",  # won't be used
        url=url,
        additional_params=None
    )

    response = api.run()

    # The .run() should automatically call .json() for this endpoint
    assert isinstance(response, dict)
    assert response["name"] == "requests"
    assert "stargazers_count" in response
