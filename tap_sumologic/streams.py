"""Stream type classes for tap-sumologic."""

import time
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from tap_sumologic.client import SumoLogicStream


class SearchJobStream(SumoLogicStream):
    """Define dynamic stream for Search Job API queries."""

    def __init__(
        self,
        tap: Any,
        name: str,
        primary_keys: list = None,
        replication_key: str = None,
        schema: dict = None,
        query: str = None,
        by_receipt_time: bool = None,
        auto_parsing_mode: str = None,
    ) -> None:
        """Class initialization.

        Args:
            tap: see tap.py
            name: see tap.py
            primary_keys: see tap.py
            replication_key: see tap.py
            schema: the json schema for the stream.
            query: see tap.py
            by_receipt_time: see tap.py
            auto_parsing_mode: see tap.py

        """
        super().__init__(tap=tap, schema=schema)

        if primary_keys is None:
            primary_keys = []

        self.name = name
        self.primary_keys = primary_keys
        self.replication_key = replication_key
        self.query = query
        self.by_receipt_time = by_receipt_time
        self.auto_parsing_mode = auto_parsing_mode

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        self.logger.info("Running query in sumologic to get records")

        records = []
        limit = 10000

        now_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        custom_columns = {
            "_SDC_EXTRACTED_AT": now_datetime,
            "_SDC_BATCHED_AT": now_datetime,
            "_SDC_DELETED_AT": None,
        }
        delay = 5
        search_job = self.conn.search_job(
            self.query,
            self.config["start_date"],
            self.config["end_date"],
            self.config["time_zone"],
            self.by_receipt_time,
            self.auto_parsing_mode,
        )
        self.logger.info(search_job)

        status = self.conn.search_job_status(search_job)
        while status["state"] != "DONE GATHERING RESULTS":
            if status["state"] == "CANCELLED":
                break
            time.sleep(delay)
            self.logger.info(":check query status")
            status = self.conn.search_job_status(search_job)
            self.logger.info(status)

        self.logger.info(status["state"])

        if status["state"] == "DONE GATHERING RESULTS":
            record_count = status["recordCount"]
            count = 0
            while count < record_count:
                self.logger.info(
                    "Get records %d of %d, limit=%d", count, record_count, limit
                )
                response = self.conn.search_job_records(
                    search_job, limit=limit, offset=count
                )
                self.logger.info("Got records %d of %d", count, record_count)

                recs = response["records"]
                # extract the result maps to put them in the list of records
                for rec in recs:
                    records.append({**rec["map"], **custom_columns})

                if len(recs) > 0:
                    count = count + len(recs)
                else:
                    break  # make sure we exit if nothing comes back

        for row in records:
            yield row
