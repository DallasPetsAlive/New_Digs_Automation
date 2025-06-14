import boto3
import copy
import datetime
import json
import logging
import os
import random
import requests
import string
import urllib.parse

from botocore.exceptions import ClientError
from .config import api_key, base, rebrandly_domain_key, rebrandly_api_key
from datetime import date
from PIL import Image, ImageOps, UnidentifiedImageError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")

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

    # check for repeat photo names
    check_photo_names(pets)

    # rename photos
    photos_renamed = rename_photos(pets)

    # get pets that are available but don't have an available date
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

    # links_cleaned_up = cleanup_links(
    #     pets,
    # )
    links_cleaned_up = 0

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
        "links_cleaned_up": links_cleaned_up,
        "photos_renamed": photos_renamed,
    }


def check_photo_names(pets):
    # only run this once a day
    current_hour = datetime.datetime.today().hour
    if current_hour != 0:
        return

    pets_with_bad_photos = []

    for pet in pets:
        try:
            pet_fields = pet["fields"]
            if (
                "Pictures" in pet_fields
                and pet_fields["Pictures"]
            ):
                pictures = pet_fields["Pictures"]

                photo_names = [picture["filename"] for picture in pictures]
                if len(photo_names) != len(set(photo_names)):
                    pets_with_bad_photos.append(pet_fields["Pet Name"])

        except Exception:
            logger.exception(f"Error checking photo names for pet {pet['id']}")

    if pets_with_bad_photos:
        post_to_slack("The following pets have duplicate photo names that must be renamed:\n{}".format("\n".join(pets_with_bad_photos)))


def rename_photos(pets):
    photos_renamed = 0
    records_to_update = []

    for pet in pets:
        try:
            pet_fields = pet["fields"]
            if (
                "Pictures" in pet_fields
                and pet_fields["Pictures"]
            ):
                photo_name_map_str = pet_fields.get("PictureMap-DoNotModify", "")
                photo_name_map = {}
                if photo_name_map_str:
                    photo_name_map = json.loads(photo_name_map_str)

                renamed = False
                fields_copy = copy.deepcopy(pet_fields)
                for photo in fields_copy["Pictures"]:
                    mapped_name = photo_name_map.get(photo["filename"], "")
                    _, photo_extension = os.path.splitext(photo["filename"])
                    if not mapped_name.startswith("nd_"):
                        new_photo_name = "nd_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
                        if not photo_extension:
                            photo_extension = ".jpg"
                        new_photo_name += photo_extension

                        logger.info(f"renaming {photo['filename']} to {new_photo_name}")
                        photo_name_map[photo["filename"]] = new_photo_name
                        photos_renamed += 1

                        renamed = True

                if renamed:
                    records_to_update.append({
                        "id": pet["id"],
                        "fields": {
                            "PictureMap-DoNotModify": json.dumps(photo_name_map),
                        },
                    })

                    pet_fields["PictureMap-DoNotModify"] = json.dumps(photo_name_map)

        except Exception:
            logger.exception(f"Error renaming photos for pet {pet['id']}")

    if records_to_update:
        send_update(records_to_update)

    return photos_renamed


def send_update(records_to_update):
    batchsize = 10
    for i in range(0, len(records_to_update), batchsize):
        batch = records_to_update[i:i+batchsize]
        try:
            payload = {
                "records": batch,
            }
            payload = json.dumps(payload, indent=4, default=str)
            url = base_url + "/Pets"
            patch_headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + api_key
            }

            response = requests.patch(url, headers=patch_headers, data=payload)
            logger.info(response.text)
            if(response.status_code != requests.codes.ok):
                logger.error(f"Patch failed status code {response.status_code}")
                logger.error(response.content)
                return False

            airtable_response = response.json()
            records = airtable_response["records"]
            if len(records) != len(batch):
                logger.error("Patch returned the wrong number of records.")
                logger.error(response.content)
                return False
        except Exception:
            logger.exception("Error updating pet records")
                


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
        try:
            if pet["id"] in pet_ids:
                # get the first image
                pet_fields = pet["fields"]
                if (
                    "Pictures" in pet_fields
                    and pet_fields["Pictures"]
                ):
                    logger.info(f"updating thumbnail for ID {pet['id']}")

                    filename_map = pet_fields.get("PictureMap-DoNotModify", "")
                    filename_map = json.loads(filename_map)

                    filename = pet_fields["Pictures"][0]["filename"]
                    if filename in filename_map:
                        filename = filename_map[filename]

                    url = pet_fields["Pictures"][0]["url"]
                    filename = filename.replace(" ", "_")
                    filename = filename.replace("%20", "_")

                    file_extension = os.path.splitext(filename)[1]
                    if "pdf" in file_extension.lower():
                        logger.warning(f"Skipping PDF image {filename}")
                        post_to_slack(f"Pet {pet['id']} has a PDF image {filename} that needs to be converted.")
                        continue

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
        except Exception:
            logger.exception(f"Error updating thumbnail for pet {pet['id']}")

    count = 0
    while len(update_records) > 0:
        batch_size = min(10, len(update_records))
        count += batch_size
        update_batch = update_records[:batch_size]
        update_records = update_records[batch_size:]
        payload = {
            "records": update_batch
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
        if len(records) != len(update_batch):
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
    return count


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

            if width > 400 and height > 400:
                img.thumbnail((400, 400))

            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
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
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket="dpa-media", Prefix="new-digs-photos/")
        for page in page_iterator:
            logger.debug("response: %s", page)

            contents = page.get("Contents")
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

                filename_map = pet_fields.get("PictureMap-DoNotModify", "")
                filename_map = json.loads(filename_map)

                photo_filename = photo["filename"]
                if photo_filename in filename_map:
                    photo_filename = filename_map[photo_filename]

                photo_filename = photo_filename.replace(" ", "_")
                photo_filename = photo_filename.replace("%20", "_")
                photo_key = "new-digs-photos/" + pet_id + "/" + photo_filename
                if photo_key not in photos_in_s3:
                    logger.info(f"going to upload {photo_key}")
                    photos_to_upload.append((photo_key, photo_url, photo_filename, pet_id))

    for photo_key, photo_url, photo_filename, pet_id in photos_to_upload:
        r = requests.get(photo_url)
        logger.info(pet_id)
        logger.info(photo_filename)
        with open("/tmp/" + photo_filename, "wb") as fp:
            fp.write(r.content)
        upload_image(photo_filename, "new-digs-photos/" + pet_id + "/")
        os.remove("/tmp/" + photo_filename)

    return len(photos_to_upload)


def cleanup_links(pets):
    active_pet_ids = []

    for pet in pets:
        if (
            pet.get("fields", {}).get("Status") not in [
                "Adopted",
                "Removed from Program"
            ]
        ):
            active_pet_ids.append(str(pet.get("fields", {}).get("Pet ID - do not edit", "")))

    current_links = []

    requestHeaders = {
        "Content-type": "application/json",
        "apikey": rebrandly_api_key,
    }

    done = False
    last_link = ""
    while not done:
        r = requests.get(
            "https://api.rebrandly.com/v1/links?limit=20&last={}".format(last_link),
            headers=requestHeaders
        )

        if (r.status_code == requests.codes.ok):
            response = r.json()
            current_links.extend(response)
            if not response:
                done = True
            else:
                last_link = response[-1]["id"]

    links_to_delete = []
    
    for link in current_links:
        destination = link.get("destination", "")
        if "jotform" not in destination:
            continue

        if "pass-form" in destination:
            logger.info("skipping pass-form link {}".format(destination)) 
            continue

        parsed = urllib.parse.urlparse(destination)
        params = urllib.parse.parse_qs(parsed.query)
        parsed_pet_id = params.get("petId")
        if parsed_pet_id:
            parsed_pet_id = parsed_pet_id.pop()
            if parsed_pet_id not in active_pet_ids:
                links_to_delete.append(link.get("id"))

    batch_to_delete = []
    for link in links_to_delete:
        batch_to_delete.append(link)
        if len(batch_to_delete) == 25:
            r = requests.delete(
                "https://api.rebrandly.com/v1/links",
                headers=requestHeaders,
                json={"links": batch_to_delete},
            )
            batch_to_delete = []

    if batch_to_delete:
        r = requests.delete(
            "https://api.rebrandly.com/v1/links",
            headers=requestHeaders,
            json={"links": batch_to_delete},
        )

    return len(links_to_delete)


def post_to_slack(message):
    message = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message,
                }
            },
        ],
    }

    webhook = json.loads(secrets_client.get_secret_value(SecretId="slack_nd_alerts_webhook")["SecretString"])
    url = webhook.get("url")

    requests.post(
        url,
        json=message,
    )
