import boto3
import json
import logging
import os
import requests
import urllib.parse

from botocore.exceptions import ClientError
from .config import api_key, base, rebrandly_domain_key, rebrandly_api_key
from .google_sheets import google_sheets_synchronization
from datetime import date
from PIL import Image, ImageOps, UnidentifiedImageError

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

    quit = False
    pets = []
    offset = None

    while not quit:
        
        params = {}

        if offset:
            params = {
                "offset": offset,
            }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != requests.codes.ok:
            logger.error("Airtable response: ")
            logger.error(response)
            logger.error("URL: %s", url)
            logger.error("Headers: %s", str(headers))
            raise Exception
    
        airtable_response = response.json()
        
        if not airtable_response.get("offset"):
            quit = True
        else:
            offset = airtable_response["offset"]
        
        pets += airtable_response["records"]

    # first get pets that are available but don't have an available date
    available_pets_to_update = get_available_pets_to_update(pets)
    available_pets_updated = 0
    if available_pets_to_update:
        available_pets_updated = update_available_pets(
            available_pets_to_update
        )
        if not available_pets_updated:
            logger.error("Updating available pets failed.")

    # get pets that are adopted without an adopted date
    adopted_pets_to_update = get_adopted_pets_to_update(pets)
    adopted_pets_updated = 0
    if adopted_pets_to_update:
        adopted_pets_updated = update_adopted_pets(
            adopted_pets_to_update
        )
        if not adopted_pets_updated:
            logger.error("Updating adopted pets failed.")

    # get pets that are removed without a removed date
    removed_pets_to_update = get_removed_pets_to_update(pets)
    removed_pets_updated = 0
    if removed_pets_to_update:
        removed_pets_updated = update_removed_pets(
            removed_pets_to_update
        )
        if not removed_pets_updated:
            logger.error("Updating removed pets failed.")

    url = base_url + "/Adoption%20Applicants"

    quit = False
    adopt_apps = []
    offset = None

    while not quit:
        
        params = {}

        if offset:
            params = {
                "offset": offset,
            }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != requests.codes.ok:
            logger.error("Airtable response: ")
            logger.error(response)
            logger.error("URL: %s", url)
            logger.error("Headers: %s", str(headers))
            raise Exception
    
        airtable_response = response.json()
        
        if not airtable_response.get("offset"):
            quit = True
        else:
            offset = airtable_response["offset"]
        
        adopt_apps += airtable_response["records"]

    url = base_url + "/Original%20Owners"

    quit = False
    owners = []
    offset = None

    while not quit:
        
        params = {}

        if offset:
            params = {
                "offset": offset,
            }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != requests.codes.ok:
            logger.error("Airtable response: ")
            logger.error(response)
            logger.error("URL: %s", url)
            logger.error("Headers: %s", str(headers))
            raise Exception
    
        airtable_response = response.json()
        
        if not airtable_response.get("offset"):
            quit = True
        else:
            offset = airtable_response["offset"]
        
        owners += airtable_response["records"]

    contracts_added = add_adoption_contracts(
        adopt_apps,
        pets,
        owners,
    )

    # sheets_rows = google_sheets_synchronization()
    sheets_rows = 0
    # update thumbnails for pets that don't have one
    thumbnails_to_update = get_thumbnails_to_update(pets)
    thumbnails_updated = 0
    if thumbnails_to_update:
        thumbnails_updated = update_thumbnails(
            pets,
            thumbnails_to_update,
        )
        if not thumbnails_updated:
            logger.error("Updating thumbnails failed.")

    # move photos to s3
    photos_in_s3 = get_photos()
    photos_uploaded = upload_photos(photos_in_s3, pets)

    return {
        "available_pets_updated": available_pets_updated,
        "adopted_pets_updated": adopted_pets_updated,
        "removed_pets_updated": removed_pets_updated,
        "adoption_contracts_added": contracts_added,
        "google_sheets_rows_written": sheets_rows,
        "thumbnails_updated": thumbnails_updated,
        "photos_uploaded": photos_uploaded,
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


def get_removed_pets_to_update(pets):
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

        # check if pet is removed and removed date has not been set
        if (
            "Status" in pet_fields
            and pet_fields["Status"] == "Removed from Program"
            and (
                "Removed from Program Date" not in pet_fields
                or not pet_fields["Removed from Program Date"]
            )
        ):
            pets_to_update.append(pet["id"])
    return pets_to_update


def update_removed_pets(pet_ids):
    today = date.today()
    update_records = []
    for id in pet_ids:
        record = {
            "id": id,
            "fields": {
                "Removed from Program Date": today,
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
                record["fields"]["Removed from Program Date"]
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
            disclaimer = None
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
                        if (
                            "Disclaimers" in pet_fields
                            and pet_fields["Disclaimers"]
                        ):
                            disclaimer = pet_fields["Disclaimers"]
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
                disclaimer,
            )
            record = {
                "id": app["id"],
                "fields": {
                    "Contract Link": contract_link,
                }
            }
            update_records.append(record)

    if len(update_records) > 0:
        for i in range(0, len(update_records), 10):
            payload = {
                "records": update_records[i:i+10]
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
            if len(records) != 10:
                logger.error("Patch returned the wrong number of records.")
                logger.error(response.content)
                return False

    return len(update_records)


def get_adoption_app_link(app, pet_name, pet_id, owner_name, owner_email, dog, disclaimer):
    link = "https://form.jotform.com/212055719626154?"
    if not dog:
        link = "https://form.jotform.com/212054429850049?"
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
    if disclaimer:
        params["petSpecific"] = disclaimer

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

    link += urllib.parse.urlencode(params, quote_via=urllib.parse.quote)

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


def get_thumbnails_to_update(pets):
    pets_to_update = []
    for pet in pets:
        pet_fields = pet["fields"]

        # check if pet has images but no thumbnail
        if (
            "Pictures" in pet_fields
            and pet_fields["Pictures"]
            and (
                "ThumbnailURL" not in pet_fields
                or not pet_fields["ThumbnailURL"]
            )
        ):
            pets_to_update.append(pet["id"])
    return pets_to_update


def update_thumbnails(pets, pet_ids):
    update_records = []

    for pet in pets:
        if pet["id"] in pet_ids:
            # get the first image
            pet_fields = pet["fields"]
            if (
                "Pictures" in pet_fields
                and pet_fields["Pictures"]
            ):
                url = pet_fields["Pictures"][0]["url"]
                filename = pet_fields["Pictures"][0]["filename"]
                filename = filename.replace(" ", "_")
                filename = filename.replace("%20", "_")
                thumbnail_file = thumbnail_image(url, filename)
                if thumbnail_file:
                    thumbnail_url = upload_image(thumbnail_file, "new-digs-thumbnails/")
                    os.remove("/tmp/" + thumbnail_file)

                    record = {
                        "id": pet["id"],
                        "fields": {
                            "ThumbnailURL": thumbnail_url,
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
                not record["fields"].get("ThumbnailURL")
            ):
                logger.error("Upload seemed to fail.")
                logger.error(response.content)
                return False
    return len(update_records)


def thumbnail_image(url, filename):
    r = requests.get(url)
    logger.info(filename)
    with open('/tmp/' + filename, 'wb') as fp:
        fp.write(r.content)
    try:
        with Image.open('/tmp/' + filename) as img:
            img = ImageOps.exif_transpose(img)
            width, height = img.size

            if height < width:
                # make square by cutting off equal amounts left and right
                left = (width - height) / 2
                right = (width + height) / 2
                top = 0
                bottom = height
                img = img.crop((left, top, right, bottom))

            elif width < height:
                # make square by cutting off bottom
                left = 0
                right = width
                top = 0
                bottom = width
                img = img.crop((left, top, right, bottom))

            if width > 160 and height > 160:
                img.thumbnail((160, 160))

            img.save('/tmp/' + filename)
    except UnidentifiedImageError:
        logger.error("Could not open image " + filename)
        return None

    return filename


def upload_image(filename, path):
    logger.info(f"uploading {path}/{filename}")

    s3 = boto3.client('s3')

    # Upload the file
    try:
        s3.upload_file(
            "/tmp/" + filename,
            "dpa-media",
            path + filename,
            ExtraArgs={'ACL': 'public-read'},
        )
    except ClientError as e:
        logging.error(e)

    return "https://dpa-media.s3.us-east-2.amazonaws.com/new-digs-thumbnails/" + filename


def get_photos():
    # get the current photos

    s3 = boto3.client('s3')

    photos = []

    try:
        done = False
        while not done:
            response = s3.list_objects_v2(
                Bucket="dpa-media",
                Prefix="new-digs-photos/",
            )
            
            if not response.get("isTruncated"):
                done = True
            contents = response.get("Contents")
            for item in contents:
                photos.append(item.get("Key"))
    except ClientError as e:
        logging.error(e)

    return photos


def upload_photos(photos_in_s3, pets):
    photos_to_upload = []
    for pet in pets:
        pet_id = pet["id"]
        pet_fields = pet["fields"]
        if (
            "Pictures" in pet_fields
            and pet_fields["Pictures"]
        ):
            for photo in pet_fields["Pictures"]:
                photo_url = photo["url"]
                photo_filename = photo["filename"]
                photo_filename = photo_filename.replace(" ", "_")
                photo_filename = photo_filename.replace("%20", "_")
                photo_key = "new-digs-photos/" + pet_id + "/" + photo_filename
                if photo_key not in photos_in_s3:
                    photos_to_upload.append((photo_key, photo_url, photo_filename, pet_id))

    for photo_key, photo_url, photo_filename, pet_id in photos_to_upload:
        r = requests.get(photo_url)
        with open("/tmp/" + photo_filename, "wb") as fp:
            fp.write(r.content)
        upload_image(photo_filename, "new-digs-photos/" + pet_id + "/")
        os.remove("/tmp/" + photo_filename)

    return len(photos_to_upload)

# logging.basicConfig(filename="log.log", level=logging.DEBUG)
# update_available_pets(["recOkHgRR68MnYz2k"])
