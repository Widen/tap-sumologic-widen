"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_sumologic.tap import TapSumoLogic

SAMPLE_CONFIG = {
    "access_id": "accessid",
    "access_key": "accesskey",
    "root_url": "https://example.com",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "end_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "time_zone": "timezone",
    "tables": [
        {
            "query": "sumologic query | count by foo",
            "query_type": "records",
            "table_name": "foo_table",
            "by_receipt_time": False,
            "auto_parsing_mode": "autoparsingmode",
        }
    ],
}

records_json = {
    "fields": [
        {"name": "_sourcecategory", "fieldType": "string", "keyField": True},
        {"name": "_count", "fieldType": "int", "keyField": False},
    ],
    "records": [{"map": {"_count": "90", "_sourcecategory": "service"}}],
}
#
# messages_json = {
#     "fields": [
#         {"name": "_sourcecategory", "fieldType": "string", "keyField": True},
#         {"name": "_count", "fieldType": "int", "keyField": False},
#     ],
#     "messages": [{"map": {"_count": "90", "_sourcecategory": "service"}}],
# }


def test_standard_tap_tests(requests_mock):
    """Run standard tap tests from the SDK."""
    requests_mock.post(
        "https://example.com/v1/search/jobs",
        json={
            "id": "123ID",
            "link": {"href": "https://example.com/v1/search/jobs/123ID", "rel": "self"},
        },
    )
    requests_mock.get(
        "https://example.com/v1/search/jobs/123ID",
        json={
            "state": "DONE GATHERING RESULTS",
            "messageCount": 90,
            "histogramBuckets": [
                {"length": 60000, "count": 1, "startTimestamp": 1359404820000},
                {"length": 60000, "count": 1, "startTimestamp": 1359405480000},
                {"length": 60000, "count": 1, "startTimestamp": 1359404340000},
            ],
            "pendingErrors": [],
            "pendingWarnings": [],
            "recordCount": 1,
        },
    )
    requests_mock.get(
        "https://example.com/v1/search/jobs/123ID/records?limit=1&offset=0",
        json=records_json,
    )
    requests_mock.get(
        "https://example.com/v1/search/jobs/123ID/records?limit=10000&offset=0",
        json=records_json,
    )
    # requests_mock.get(
    #     "https://example.com/v1/search/jobs/123ID/messages?limit=1&offset=0",
    #     json=records_json,
    # )
    # requests_mock.get(
    #     "https://example.com/v1/search/jobs/123ID/messages?limit=10000&offset=0",
    #     json=records_json,
    # )
    tests = get_standard_tap_tests(TapSumoLogic, config=SAMPLE_CONFIG)
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
