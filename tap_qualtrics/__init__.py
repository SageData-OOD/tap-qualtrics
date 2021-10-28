#!/usr/bin/env python3
import os
import io
import json
import re
import time
import pandas as pd

import backoff
import zipfile
import requests
from datetime import datetime, timedelta

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform
from tap_qualtrics.exceptions import Qualtrics429Error, Qualtrics500Error, Qualtrics503Error, Qualtrics504Error, \
    Qualtrics400Error, Qualtrics401Error, Qualtrics403Error

HOST_URL = "https://{data_center}.qualtrics.com"
REQUIRED_CONFIG_KEYS = ["start_date", "data_center", "client_id", "client_secret", "refresh_token"]
LOGGER = singer.get_logger()
END_POINTS = {
    "refresh": "/oauth2/token",
    "surveys_responses": "/API/v3/surveys",
    "export_responses": "/surveys/{survey_id}/export-responses/"
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_end_date(start_date):
    end_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ") + timedelta(days=30)
    today = datetime.today().strftime('%Y-%m-%d')

    # if end_date bigger than today's date, then end_time = current datetime
    if end_date.strftime('%Y-%m-%d') >= today:
        end_date = datetime.utcnow()
    return end_date.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_bookmark(stream_id):
    bookmark = {
        "surveys_responses": "recorded_date"
    }
    return bookmark.get(stream_id)


def get_key_properties(stream_id):
    key_properties = {
        "surveys_responses": ["survey_id", "response_id"]
    }
    return key_properties.get(stream_id, [])


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": [replication_key], "selected": True}}]
    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if replication_key is None:
        mdata[0]["metadata"]["forced-replication-method"] = "FULL_TABLE"
        mdata[0]["metadata"].pop("valid-replication-keys")

    for key in schema.properties:
        # hence when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion, "selected": True}} for prop
                 in schema.properties.get(key).properties])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion, "selected": True}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def print_metrics(config):
    creds = {
        "data_center": config["data_center"],
        "state": {"service_client_id": config["client_id"], "service_client_secret": config["client_secret"]},
        "raw_credentials": {"refresh_token": config["refresh_token"]}
    }
    metric = {"type": "secret", "value": creds, "tags": "tap-secret"}
    LOGGER.info('METRIC: %s', json.dumps(metric))


def _refresh_token(config):
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token']
    }
    url = HOST_URL.format(data_center=config["data_center"]) + END_POINTS["refresh"]
    response = requests.post(url, data=data,
                             auth=(config["client_id"], config['client_secret']))
    if response.status_code != 200:
        raise Exception(response.text)
    return response.json()


def refresh_access_token_if_expired(config):
    # if [expires_in not exist] or if [exist and less then current time] then it will update the token
    if config.get('expires_in') is None or config.get('expires_in') < datetime.utcnow():
        res = _refresh_token(config)
        config["access_token"] = res["access_token"]
        config["refresh_token"] = res["refresh_token"]
        print_metrics(config)
        config["expires_in"] = datetime.utcnow() + timedelta(seconds=int(res["expires_in"]))
        return True
    return False


def header_setup(headers, config, path=None):
    if refresh_access_token_if_expired(config) or "Authorization" not in headers:
        headers["Authorization"] = f'bearer {config["access_token"]}'
    if path:
        url = HOST_URL.format(data_center=config["data_center"]) + path
        return headers, url
    return headers


@backoff.on_exception(backoff.expo, Qualtrics429Error, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def extract_survey_page(url, config, headers):
    headers = header_setup(headers, config)

    req = requests.get(url, headers=headers)
    response = req.json()

    if response['meta']['httpStatus'] == '429 - Too Many Requests':
        raise Qualtrics429Error("429 - Too Many Requests")
    elif response['meta']['httpStatus'] == '500 - Internal Server Error':
        raise Qualtrics500Error('500 - Internal Server Error')
    elif response['meta']['httpStatus'] == '503 - Temporary Internal Server Error':
        raise Qualtrics503Error('503 - Temporary Internal Server Error')
    elif response['meta']['httpStatus'] == '504 - Gateway Timeout':
        raise Qualtrics504Error('504 - Gateway Timeout')
    elif response['meta']['httpStatus'] == '400 - Bad Request':
        raise Qualtrics400Error(
            'Qualtrics Error\n(Http Error: 400 - Bad Request): There was something invalid about the request.')
    elif response['meta']['httpStatus'] == '401 - Unauthorized':
        raise Qualtrics401Error(
            'Qualtrics Error\n(Http Error: 401 - Unauthorized): The Qualtrics API user could not be authenticated or '
            'does not have authorization to access the requested resource.')
    elif response['meta']['httpStatus'] == '403 - Forbidden':
        raise Qualtrics403Error(
            'Qualtrics Error\n(Http Error: 403 - Forbidden): The Qualtrics API user was authenticated and made a '
            'valid request, but is not authorized to access this requested resource.')
    elif response['meta']['httpStatus'] != '200 - OK':
        raise Exception(response['meta']['error']['errorMessage'])

    surveys = response['result'].get('elements')
    next_page = response['result'].get('nextPage')
    return surveys, next_page


def get_surveys(config, stream_id):
    all_surveys = []
    endpoint = END_POINTS.get(stream_id)
    url = HOST_URL.format(data_center=config["data_center"]) + endpoint
    headers = {"Content-Type": "application/json"}
    _next = url
    while _next is not None:
        surveys, _next = extract_survey_page(_next, config, headers)
        if not surveys:
            break
        all_surveys += surveys
    return all_surveys


@backoff.on_exception(backoff.expo, Qualtrics429Error, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def setup_request(survey_id, payload, config):
    """
    This method sets up the request and handles the setup of the request for the survey.
    """
    headers = {"Content-Type": "application/json"}
    headers, url = header_setup(headers, config, path=END_POINTS["export_responses"].format(survey_id=survey_id))
    request = requests.post(url, data=json.dumps(payload), headers=headers)
    response = request.json()

    if response['meta']['httpStatus'] == '429 - Too Many Requests':
        raise Qualtrics429Error("429 - Too Many Requests")
    elif response['meta']['httpStatus'] == '500 - Internal Server Error':
        raise Qualtrics500Error('500 - Internal Server Error')
    elif response['meta']['httpStatus'] == '503 - Temporary Internal Server Error':
        raise Qualtrics503Error('503 - Temporary Internal Server Error')
    elif response['meta']['httpStatus'] == '504 - Gateway Timeout':
        raise Qualtrics504Error('504 - Gateway Timeout')
    elif response['meta']['httpStatus'] == '400 - Bad Request':
        raise Qualtrics400Error(
            'Qualtrics Error\n(Http Error: 400 - Bad Request): There was something invalid about the request.')
    elif response['meta']['httpStatus'] == '401 - Unauthorized':
        raise Qualtrics401Error(
            'Qualtrics Error\n(Http Error: 401 - Unauthorized): The Qualtrics API user could not be authenticated or '
            'does not have authorization to access the requested resource.')
    elif response['meta']['httpStatus'] == '504 - Gateway Timeout':
        raise Qualtrics504Error('504 - Gateway Timeout')
    elif response['meta']['httpStatus'] != '200 - OK':
        raise Exception(response['meta']['error']['errorMessage'])

    progress_id = response['result']['progressId']
    return progress_id, url, headers


def get_survey_responses(survey_id, payload, config):
    """
    This method sends the request, and sets up the download request.
    """
    is_file = None
    check_response = None
    progress_id, url, headers = setup_request(survey_id, payload, config)
    progress_status = "in progress"
    while progress_status != "complete" and progress_status != "failed" and is_file is None:
        headers = header_setup(headers, config)
        check_url = url + progress_id
        check_request = requests.get(check_url, headers=headers)
        check_response = check_request.json()

        is_file = check_response.get("result", {}).get("fileId")
        progress_status = check_response["result"]["status"]
        time.sleep(0.25)

    if check_response['meta']['httpStatus'] == '429 - Too Many Requests':
        raise Qualtrics429Error("429 - Too Many Requests")
    elif check_response['meta']['httpStatus'] == '500 - Internal Server Error':
        raise Qualtrics500Error('500 - Internal Server Error')
    elif check_response['meta']['httpStatus'] == '503 - Temporary Internal Server Error':
        raise Qualtrics503Error('503 - Temporary Internal Server Error')
    elif check_response['meta']['httpStatus'] == '504 - Gateway Timeout':
        raise Qualtrics504Error('504 - Gateway Timeout')
    elif check_response['meta']['httpStatus'] == '400 - Bad Request':
        raise Qualtrics400Error(
            'Qualtrics Error\n(Http Error: 400 - Bad Request): There was something invalid about the request.')
    elif check_response['meta']['httpStatus'] == '401 - Unauthorized':
        raise Qualtrics401Error(
            'Qualtrics Error\n(Http Error: 401 - Unauthorized): The Qualtrics API user could not be authenticated or '
            'does not have authorization to access the requested resource.')
    elif check_response['meta']['httpStatus'] == '403 - Forbidden':
        raise Qualtrics403Error(
            'Qualtrics Error\n(Http Error: 403 - Forbidden): The Qualtrics API user was authenticated and made a '
            'valid request, but is not authorized to access this requested resource.')
    elif check_response['meta']['httpStatus'] != '200 - OK':
        raise Exception(check_response['meta']['error']['errorMessage'])

    download_url = url + is_file + '/file'
    headers = header_setup(headers, config)
    download_request = requests.get(download_url, headers=headers, stream=True)

    with zipfile.ZipFile(io.BytesIO(download_request.content)) as survey_zip:
        for s in survey_zip.infolist():
            df = pd.read_csv(survey_zip.open(s.filename))
            return df.to_dict('records')


def camel_to_snake_case(name):
    """
    AssimilatedVatBox  --> assimilated_vat_box
    """
    sn = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    return sn


def refactor_record_according_to_schema(record):
    converted_data = {camel_to_snake_case(k): v if not isinstance(v, dict) else refactor_record_according_to_schema(v)
                      for k, v in record.items()}
    return converted_data


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = get_bookmark(stream.tap_stream_id)
        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column).split(" ")[0] \
            if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"] + "T00:00:00Z"

        list_surveys = get_surveys(config, stream.tap_stream_id)
        list_surveys_id = [item["id"] for item in list_surveys]

        for survey_id in list_surveys_id:
            bookmark = start_date

            while True:
                end_date = get_end_date(bookmark)
                payload = {
                    "startDate": bookmark,
                    "endDate": end_date,
                    "compress": True
                }

                records = get_survey_responses(survey_id, payload, config)
                with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                    for row in records:
                        row["survey_id"] = survey_id
                        converted_data = refactor_record_according_to_schema(row)
                        # Type Conversation and Transformation
                        transformed_data = transform(converted_data, schema, metadata=mdata)

                        singer.write_records(stream.tap_stream_id, [transformed_data])
                        counter.increment()
                        if bookmark_column:
                            bookmark = max([bookmark, converted_data[bookmark_column]])
                    if bookmark_column:
                        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, bookmark)
                        singer.write_state(state)
                    if end_date.split("T")[0] == datetime.today().strftime('%Y-%m-%d'):
                        break

                    bookmark = end_date


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
