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
    deleted                 boolean not null
);
create index messages_id_index
    on messages (id);
create index messages_datetime_index
    on messages (datetime);

create table if not exists main.chats
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text not null,
    str_repr                text not null,
    dict_repr               text
    /* Custom fields below */
);
create index chats_id_index
    on chats (id);

create table if not exists main.users
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text not null,
    str_repr                text not null,
    dict_repr               text
    /* Custom fields below */
);
create index users_id_index
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
create index admin_events_id_index
    on admin_events (id);
create index admin_events_datetime_index
    on admin_events (datetime);

create table if not exists main.media
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text not null,
    str_repr                text not null,
    dict_repr               text,
    /* Custom fields below */
    file_name               text
);
create index media_id_index
    on media (id);
