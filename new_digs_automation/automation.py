import json
import logging
import requests

from .config import api_key, base
from datetime import date

logger = logging.getLogger()
logger.setLevel(logging.INFO)

possible_pet_statuses = [
    "Accepted, Not Yet Published",
    "Published - Available for Adoption",
    "Adoption Pending",
    "Adopted",
    "Removed from Program"
]
base_url = "https://api.airtable.com/v0/" + base
headers = {"Authorization": "Bearer " + api_key}


def automations():
    url = base_url + "/Pets"

    response = requests.get(url, headers=headers)
    logger.info(json.dumps(json.loads(response.text)))
    if(response.status_code != requests.codes.ok):
        logger.error("Airtable response: ")
        logger.error(response)
        logger.error("URL: " + url)
        logger.error("Headers: " + str(headers))
        return

    airtable_response = response.json()

    # first get pets that are available but don't have an available date
    available_pets_to_update = get_available_pets_to_update(
        airtable_response["records"]
    )
    available_pets_updated = 0
    if available_pets_to_update:
        available_pets_updated = update_available_pets(
            available_pets_to_update
        )
        if not available_pets_updated:
            logger.error("Updating pets failed.")

    return {
        "available_pets_updated": available_pets_updated
    }


def get_available_pets_to_update(pets):
    pets_to_update = []
    for pet in pets:
        pet_fields = pet["fields"]

        # make sure there's no funny business
        if (
            "Status" in pet_fields
            and pet_fields["Status"] not in possible_pet_statuses
            and len(pet_fields["Status"]) > 0
        ):
            error_status = pet_fields["Status"]
            id = pet["id"]
            logger.warning(f"Unknown pet status: {error_status} id: {id}")
            continue
        if (
            "Status" not in pet_fields
            or pet_fields["Status"] == ""
        ):
            id = pet["id"]
            logger.warning(f"Empty/missing pet status id: {id}")
            continue

        # check if pet is available and available date has not been set
        if (
            "Status" in pet_fields
            and pet_fields["Status"] in [
                "Published - Available for Adoption",
                "Adoption Pending",
                "Adopted",
                "Removed from Program"
            ]
            and (
                "Made Available for Adoption Date" not in pet_fields
                or not pet_fields["Made Available for Adoption Date"]
            )
        ):
            pets_to_update.append(pet["id"])
    return pets_to_update


def update_available_pets(pet_ids):
    today = date.today()
    update_records = []
    for id in pet_ids:
        record = {
            "id": id,
            "fields": {
                "Made Available for Adoption Date": today,
            }
        }
        update_records.append(record)

    if len(update_records) > 0:
        payload = {
            "records": update_records
        }
        payload = json.dumps(payload, indent=4, default=str)
        logger.info(payload)
        url = base_url + "/Pets"
        patch_headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + api_key
        }

        response = requests.patch(url, headers=patch_headers, data=payload)
        logger.info(response.text)
        if(response.status_code != requests.codes.ok):
            logger.error("Patch failed.")
            logger.error(response.content)
            return False

        airtable_response = response.json()
        records = airtable_response["records"]
        if len(records) != len(update_records):
            logger.error("Patch returned the wrong number of records.")
            logger.error(response.content)
            return False
        for record in records:
            if (
                record["fields"]["Made Available for Adoption Date"]
                != str(today)
            ):
                logger.error("Patch returned the wrong date.")
                logger.error(response.content)
                return False
    return len(update_records)


# logging.basicConfig(filename="log.log", level=logging.DEBUG)
# update_available_pets(["recOkHgRR68MnYz2k"])
