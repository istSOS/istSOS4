import pytest


@pytest.mark.parametrize("collection", ["Things", "Observations"])
def test_as_of_and_from_to_together_returns_422(client, collection):
    response = client.get(
        f"{collection}?$as_of=2026-10-01T00:00:00Z&$from_to=2026-10-01T00:00:00Z/2026-10-02T00:00:00Z"
    )

    assert response.status_code == 422, (
        f"Expected 422, got {response.status_code}. " f"Response: {response.text}"
    )


@pytest.mark.parametrize("collection", ["Things", "Observations"])
def test_invalid_as_of_returns_422(client, collection):
    response = client.get(f"{collection}?$as_of=banana")

    assert response.status_code == 422, (
        f"Expected 422, got {response.status_code}. " f"Response: {response.text}"
    )


@pytest.mark.parametrize("collection", ["Things", "Observations"])
def test_future_as_of_returns_422(client, collection):
    response = client.get(f"{collection}?$as_of=2099-01-01T00:00:00Z")

    assert response.status_code == 422, (
        f"Expected 422, got {response.status_code}. " f"Response: {response.text}"
    )

    assert (
        response.json()["detail"]
        == "$as_of query parameter time can not be more than current time!"
    )


@pytest.mark.parametrize("collection", ["Things", "Observations"])
@pytest.mark.parametrize(
    "from_to",
    [
        "",
        "2026-10-01T00:00:00Z",
        "a/b/c",
    ],
    ids=["empty", "missing-separator", "extra-part"],
)
def test_invalid_from_to_structure_returns_422(client, from_to, collection):
    response = client.get(f"{collection}?$from_to={from_to}")

    assert response.status_code == 422, (
        f"Expected 422, got {response.status_code}. " f"Response: {response.text}"
    )

    assert response.json()["detail"] == (
        "Query parameter $from_to must contain two date-times separated by '/'."
    )


@pytest.mark.parametrize("collection", ["Things", "Observations"])
@pytest.mark.parametrize(
    "from_to",
    [
        "/2026-10-02T00:00:00Z",
        "2026-10-01T00:00:00Z/",
        "/",
    ],
    ids=["empty-start", "empty-end", "empty-both"],
)
def test_empty_from_to_boundary_returns_422(client, from_to, collection):
    response = client.get(f"{collection}?$from_to={from_to}")

    assert response.status_code == 422, (
        f"Expected 422, got {response.status_code}. " f"Response: {response.text}"
    )

    assert response.json()["detail"] == (
        "Query parameter $from_to cannot contain empty boundaries."
    )


@pytest.mark.parametrize("collection", ["Things", "Observations"])
@pytest.mark.parametrize(
    "from_to",
    [
        "banana/2026-10-02T00:00:00Z",
        "2026-10-01T00:00:00Z/banana",
        "2026-02-30T00:00:00Z/2026-03-01T00:00:00Z",
        "2020-01-01/2020-01-02T00:00:00Z",
        "2020-01-01T00:00:00Z/2020-01-02",
    ],
    ids=[
        "invalid-start",
        "invalid-end",
        "non-existent-date",
        "start-date-only",
        "end-date-only",
    ],
)
def test_invalid_from_to_datetime_returns_422(client, from_to, collection):
    response = client.get(f"{collection}?$from_to={from_to}")

    assert response.status_code == 422, (
        f"Expected 422, got {response.status_code}. " f"Response: {response.text}"
    )

    assert response.json()["detail"] == (
        "Query parameter $from_to must contain valid date-times."
    )


@pytest.mark.parametrize("collection", ["Things", "Observations"])
@pytest.mark.parametrize(
    "from_to",
    [
        "2026-10-02T00:00:00Z/2026-10-01T00:00:00Z",
        "2026-10-01T09:00:00Z/2026-10-01T10:00:00%2B02:00",
    ],
)
def test_reversed_from_to_returns_422(client, from_to, collection):
    response = client.get(f"{collection}?$from_to={from_to}")

    assert response.status_code == 422, (
        f"Expected 422, got {response.status_code}. " f"Response: {response.text}"
    )

    assert response.json()["detail"] == (
        "First time interval in quary parameter $from_to can not be more than second time interval!"
    )
