/*
This is the schema for an individual chat's database. It stores the messages and media of a chat.
Migrations may be placed on top of this database definition.

Each resource stored starts with a table like this, and then additional columns can be parsed out for easier querying.
But these basic columns are essential.

create table if not exists main.template_resource
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text not null,
    str_repr                text not null,
    dict_repr               text
    /* Custom fields below * /
);
*/

create table if not exists main.db_migrations
(
    migration_id   integer not null
        constraint db_migrations_pk
            primary key,
    migration_name integer not null,
    start_time     text,
    end_time       text
);

create table if not exists main.messages
(
    archive_datetime        text    not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text    not null,
    str_repr                text    not null,
    dict_repr               text,
    /* Custom fields below */
    datetime                text,
    text                    text,
    media_id                integer,
    user_id                 integer,
    sticker_id              integer,
    sticker_set_id          integer,
    deleted                 boolean not null,
    edit_datetime           text
);
create index if not exists messages_id_index
    on messages (id);
create index if not exists messages_datetime_index
    on messages (datetime);

create table if not exists main.chats
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text not null,
    str_repr                text not null,
    dict_repr               text,
    /* Custom fields below */
    title                   text
);
create index if not exists chats_id_index
    on chats (id);

create table if not exists main.users
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text not null,
    str_repr                text not null,
    dict_repr               text,
    /* Custom fields below */
    bio                     text,
    birthday                text,
    is_bot                  boolean,
    is_deleted              boolean,
    first_name              text,
    last_name               text,
    phone_number            text,
    has_premium             boolean,
    username                text,
    other_usernames         text
);
create index if not exists users_id_index
    on users (id);

create table if not exists main.admin_events
(
    archive_datetime        text    not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text    not null,
    str_repr                text    not null,
    dict_repr               text,
    /* Custom fields below */
    datetime                text,
    message_id              integer
);
create index if not exists admin_events_id_index
    on admin_events (id);
create index if not exists admin_events_datetime_index
    on admin_events (datetime);
