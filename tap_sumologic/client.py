"""REST client handling, including sumologicStream base class."""

from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap

from tap_sumologic.sumologic_sdk import SumoLogic


class SumoLogicStream(Stream):
    """sumologic stream class."""

    def __init__(self, tap: Tap, schema):
        super().__init__(tap, name=tap.name, schema=schema)
        self.conn = SumoLogic(
            self.config["access_id"], self.config["access_key"], self.url_base
        )

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["root_url"]
