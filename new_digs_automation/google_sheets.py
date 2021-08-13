import gspread
import json
import logging
import requests

from .config import (
    api_key,
    base,
    pets_file_key,
    adoption_app_file_key,
    participant_app_file_key,
    original_owners_file_key,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

base_url = "https://api.airtable.com/v0/" + base
headers = {"Authorization": "Bearer " + api_key}

fields_to_ignore = ["Pictures", "Medical Records"]


def google_sheets_synchronization():
    sheets = gspread.service_account(filename="new_digs_automation/service_account.json")
    total_rows = 0
    total_rows += sync_sheet(sheets, pets_file_key, "/Pets")
    total_rows += sync_sheet(sheets, adoption_app_file_key, "/Adoption%20Applicants")
    total_rows += sync_sheet(sheets, participant_app_file_key, "/Participant%20Applicants")
    total_rows += sync_sheet(sheets, original_owners_file_key, "/Original%20Owners")
    return total_rows


def sync_sheet(sheets, file_key, table_name):
    file = sheets.open_by_key(file_key)
    sheet = file.get_worksheet(0)

    url = base_url + table_name

    # get data from Airtable
    response = requests.get(url, headers=headers)
    logger.info(json.dumps(json.loads(response.text)))
    if(response.status_code != requests.codes.ok):
        logger.error("Airtable response: ")
        logger.error(response)
        logger.error("URL: " + url)
        logger.error("Headers: " + str(headers))
        return 0

    airtable_response = response.json()

    records = airtable_response.get("records")
    if not records:
        logger.info(f"No records found for table {table_name}")
        return 0
    number_of_records = len(records)

    # sort the data into a list of dicts
    table_data = []

    for record in records:
        output_record = {
            "id": record["id"],
            "createdTime": record["createdTime"],
        }
        for field in record["fields"]:
            if (
                field not in fields_to_ignore
                and type(record["fields"][field]) is not list
            ):
                output_record[field] = record["fields"][field]
        table_data.append(output_record)

    # get field list
    field_list = []
    for record in table_data:
        for field in record:
            field_list.append(field)
    field_list = sorted(list(set(field_list)))

    # the output data for google sheets should be a list of lists
    output_data = []
    # first row is headers aka fields
    output_data.append(field_list)
    # now add all the data
    for record in table_data:
        record_data = []
        for field in field_list:
            record_data.append(record.get(field, ""))
        output_data.append(record_data)

    output_rows_len = len(output_data)

    logger.info(f"writing out {output_rows_len} rows for {table_name}")
    if number_of_records + 1 != output_rows_len:
        logger.error("Number of records/rows mismatch")
        logger.error(f"Expected {number_of_records} + 1 rows")
        return 0

    # finally write it all out to sheets
    sheet.update("A1", output_data)

    return output_rows_len
