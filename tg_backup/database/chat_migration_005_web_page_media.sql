/*
This migration is going to add a bunch more columns to the `chats` table, for additional information, some parsed from
ChatFull/ChannelFull objects. This should accompany a change in the UserDataFetcher, to fetch this chat data.
This will be applied to both the core DB and any chats DBs
*/
alter table messages
    add web_page_id integer GENERATED ALWAYS AS (json_extract(dict_repr, "$.media.webpage.id")) VIRTUAL;

create table web_page_media
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    web_page_id integer not null,
    media_id integer not null,
    media_json_path text  -- JSON path in the WebPage object, to the media
);
