import json
import logging
import requests
import urllib.parse

from .config import api_key, base, rebrandly_domain_key, rebrandly_api_key
from .google_sheets import google_sheets_synchronization
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
        logger.error("Airtable response Pets: ")
        logger.error(response)
        logger.error("URL: " + url)
        logger.error("Headers: " + str(headers))
        return

    airtable_pet_response = response.json()

    # first get pets that are available but don't have an available date
    available_pets_to_update = get_available_pets_to_update(
        airtable_pet_response["records"]
    )
    available_pets_updated = 0
    if available_pets_to_update:
        available_pets_updated = update_available_pets(
            available_pets_to_update
        )
        if not available_pets_updated:
            logger.error("Updating pets failed.")

    # get pets that are adopted without an adopted date
    adopted_pets_to_update = get_adopted_pets_to_update(
        airtable_pet_response["records"]
    )
    adopted_pets_updated = 0
    if adopted_pets_to_update:
        adopted_pets_updated = update_adopted_pets(
            adopted_pets_to_update
        )
        if not adopted_pets_updated:
            logger.error("Updating pets failed.")

    url = base_url + "/Adoption%20Applicants"
    response = requests.get(url, headers=headers)
    logger.info(json.dumps(json.loads(response.text)))
    if(response.status_code != requests.codes.ok):
        logger.error("Airtable response Adopt apps: ")
        logger.error(response)
        logger.error("URL: " + url)
        logger.error("Headers: " + str(headers))
        return

    airtable_adopt_response = response.json()

    url = base_url + "/Original%20Owners"
    response = requests.get(url, headers=headers)
    logger.info(json.dumps(json.loads(response.text)))
    if(response.status_code != requests.codes.ok):
        logger.error("Airtable response Adopt apps: ")
        logger.error(response)
        logger.error("URL: " + url)
        logger.error("Headers: " + str(headers))
        return

    airtable_owners_response = response.json()

    contracts_added = add_adoption_contracts(
        airtable_adopt_response["records"],
        airtable_pet_response["records"],
        airtable_owners_response["records"],
    )

    sheets_rows = google_sheets_synchronization()

    return {
        "available_pets_updated": available_pets_updated,
        "adoption_contracts_added": contracts_added,
        "google_sheets_rows_written": sheets_rows,
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


def get_adopted_pets_to_update(pets):
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

        # check if pet is adopted and adopted date has not been set
        if (
            "Status" in pet_fields
            and pet_fields["Status"] == "Adopted"
            and (
                "Adopted Date" not in pet_fields
                or not pet_fields["Adopted Date"]
            )
        ):
            pets_to_update.append(pet["id"])
    return pets_to_update


def update_adopted_pets(pet_ids):
    today = date.today()
    update_records = []
    for id in pet_ids:
        record = {
            "id": id,
            "fields": {
                "Adopted Date": today,
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
                record["fields"]["Adopted Date"]
                != str(today)
            ):
                logger.error("Patch returned the wrong date.")
                logger.error(response.content)
                return False
    return len(update_records)


def add_adoption_contracts(records, pets, owners):
    update_records = []
    for app in records:
        app_fields = app["fields"]
        if (
            "Contract Link" not in app_fields or
            (
                "Contract Link" in app_fields
                and not app_fields["Contract Link"]
            )
        ):
            pet_name = None
            pet_id = None
            is_dog = False
            current_owner_name = None
            current_owner_id = None
            current_owner_email = None
            if (
                "Applied For" in app_fields
                and app_fields["Applied For"]
            ):
                pet_record_id = app_fields["Applied For"][0]
                for pet in pets:
                    if pet_record_id == pet["id"]:
                        pet_fields = pet["fields"]
                        if (
                            "Pet Name" in pet_fields
                            and pet_fields["Pet Name"]
                        ):
                            pet_name = pet_fields["Pet Name"]
                        if (
                            "Pet ID - do not edit" in pet_fields
                            and pet_fields["Pet ID - do not edit"]
                        ):
                            pet_id = pet_fields["Pet ID - do not edit"]
                        if (
                            "Pet Species" in pet_fields
                            and pet_fields["Pet Species"]
                        ):
                            is_dog = pet_fields["Pet Species"] == "Dog"
                        if (
                            "Original Owner" in pet_fields
                            and pet_fields["Original Owner"]
                        ):
                            current_owner_id = pet_fields["Original Owner"][0]
                        break

                for owner in owners:
                    if current_owner_id == owner["id"]:
                        owner_fields = owner["fields"]
                        if (
                            "Name" in owner_fields
                            and owner_fields["Name"]
                        ):
                            current_owner_name = owner_fields["Name"]
                        if (
                            "Email Address" in owner_fields
                            and owner_fields["Email Address"]
                        ):
                            current_owner_email = owner_fields["Email Address"]

            contract_link = get_adoption_app_link(
                app,
                pet_name,
                pet_id,
                current_owner_name,
                current_owner_email,
                is_dog,
            )
            record = {
                "id": app["id"],
                "fields": {
                    "Contract Link": contract_link,
                }
            }
            update_records.append(record)

    if len(update_records) > 0:
        payload = {
            "records": update_records
        }
        payload = json.dumps(payload, indent=4, default=str)
        logger.info(payload)
        url = base_url + "/Adoption%20Applicants"
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

    return len(update_records)


def get_adoption_app_link(app, pet_name, pet_id, owner_name, owner_email, dog):
    link = "https://dallaspetsalive.org/new-digs-canine-adoption-contract/?"
    if not dog:
        link = "https://dallaspetsalive.org/new-digs-feline-adoption-contract/?"
    params = {}
    if pet_name:
        params["petName"] = pet_name
    if pet_id:
        params["petId"] = pet_id
    if owner_name:
        space = owner_name.find(" ")
        owner_first_name = owner_name[:space]
        owner_last_name = owner_name[space:]
        params["input6[firstname-3]"] = owner_first_name
        params["input6[lastname-3]"] = owner_last_name
    if owner_email:
        params["ownersEmail"] = owner_email

    app_fields = app["fields"]
    if (
        "Name" in app_fields
        and app_fields["Name"]
    ):
        app_name = app_fields["Name"]
        space = app_name.find(" ")
        app_first_name = app_name[:space]
        app_last_name = app_name[space:]
        params["input6[firstname-4]"] = app_first_name
        params["input6[lastname-4]"] = app_last_name

    link += urllib.parse.urlencode(params)

    linkRequest = {
        "destination": link,
        "domain": {
            "id": rebrandly_domain_key
        },
    }

    requestHeaders = {
        "Content-type": "application/json",
        "apikey": rebrandly_api_key,
    }

    r = requests.post(
        "https://api.rebrandly.com/v1/links",
        data=json.dumps(linkRequest),
        headers=requestHeaders
    )

    if (r.status_code == requests.codes.ok):
        link = r.json()
        logger.info("Long URL was %s, short URL is %s" % (link["destination"], link["shortUrl"]))
        return link["shortUrl"]
    return None


# logging.basicConfig(filename="log.log", level=logging.DEBUG)
# update_available_pets(["recOkHgRR68MnYz2k"])
