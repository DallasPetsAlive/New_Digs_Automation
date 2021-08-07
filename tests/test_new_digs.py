import logging
from datetime import date
from new_digs_automation.config import base
from new_digs_automation.automation import (
    get_available_pets_to_update,
    update_available_pets,
)

logging.basicConfig(filename="log.log", level=logging.DEBUG)
today = date.today()


def test_get_pets_to_set_available_date():
    test_pets = [
        {
            "id": "1",
            "fields": {
                "Pet Name": "Test Doggo",
                "Status": "Published - Available for Adoption",
            },
        },
        {
            "id": "2",
            "fields": {
                "Pet Name": "Test Cat",
                "Made Available for Adoption Date": "some date",
                "Status": "Adoption Pending",
            },
        },
        {
            "id": "3",
            "fields": {
                "Pet Name": "Test Birdie",
                "Made Available for Adoption Date": "",
                "Status": "Adopted",
            },
        },
        {
            "id": "4",
            "fields": {
                "Pet Name": "Spot",
                "Made Available for Adoption Date": "",
                "Status": "Accepted, Not Yet Published",
            },
        },
    ]
    pet_ids_to_update = get_available_pets_to_update(test_pets)
    assert pet_ids_to_update == ["1", "3"]


def test_check_for_invalid_status(caplog):
    test_pets = [
        {
            "id": "1",
            "fields": {
                "Pet Name": "Test Doggo",
                "Status": "Something Random",
            },
        },
    ]
    get_available_pets_to_update(test_pets)
    assert "Unknown pet status: Something Random id: 1" in caplog.text


def test_check_for_empty_status(caplog):
    test_pets = [
        {
            "id": "1",
            "fields": {
                "Pet Name": "Test Doggo",
                "Status": "",
            },
        },
    ]
    get_available_pets_to_update(test_pets)
    assert "Empty/missing pet status id: 1" in caplog.text


def test_check_for_missing_status(caplog):
    test_pets = [
        {
            "id": "1",
            "fields": {
                "Pet Name": "Test Doggo",
            },
        },
    ]
    get_available_pets_to_update(test_pets)
    assert "Empty/missing pet status id: 1" in caplog.text


def test_request_pet_available_update(requests_mock):
    input = ["1"]

    url = "https://api.airtable.com/v0/" + base + "/Pets"
    successful_output = {
        "records": [
            {
                "id": "1",
                "fields": {
                    "Made Available for Adoption Date": str(today)
                }
            }
        ]
    }

    requests_mock.patch(url, json=successful_output)

    assert update_available_pets(input)


def test_request_pet_available_update_bad_response(requests_mock, caplog):
    input = ["1"]

    url = "https://api.airtable.com/v0/" + base + "/Pets"
    error_output = {
        "status_code": 412
    }

    requests_mock.patch(
        url,
        [
            error_output
        ]
    )

    assert not update_available_pets(input)
    assert "Patch failed." in caplog.text


def test_request_pet_available_update_wrong_length(requests_mock, caplog):
    input = ["1"]

    url = "https://api.airtable.com/v0/" + base + "/Pets"
    error_output = {
        "records": []
    }

    requests_mock.patch(url, json=error_output)

    assert not update_available_pets(input)
    assert "Patch returned the wrong number of records." in caplog.text


def test_request_pet_available_update_wrong_date(requests_mock, caplog):
    input = ["1"]

    url = "https://api.airtable.com/v0/" + base + "/Pets"
    error_output = {
        "records": [
            {
                "id": "1",
                "fields": {
                    "Made Available for Adoption Date": "2020-12-12"
                }
            }
        ]
    }

    requests_mock.patch(url, json=error_output)

    assert not update_available_pets(input)
    assert "Patch returned the wrong date." in caplog.text
