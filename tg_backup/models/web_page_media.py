import datetime
from typing import Optional


class WebPageMedia:
    def __init__(
            self,
            archive_datetime: Optional[datetime.datetime],
            archive_tl_schema_layer: Optional[int],
            web_page_id: int,
            media_id: int,
            media_json_path: str,
    ):
        # noinspection PyUnresolvedReferences
        current_scheme_layer = telethon.tl.alltlobjects.LAYER
        self.archive_datetime = archive_datetime or datetime.datetime.now(datetime.timezone.utc)
        self.archive_tl_schema_layer = archive_tl_schema_layer or current_scheme_layer
        self.web_page_id = web_page_id
        self.media_id = media_id
        self.media_json_path = media_json_path

    @classmethod
    def from_web_page(cls, web_page_id: int, media_id: int, media_json_path: str) -> "WebPageMedia":
        return cls(
            None,
            None,
            web_page_id,
            media_id,
            media_json_path,
        )
