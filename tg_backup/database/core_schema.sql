/*
This is the schema for the core database, this database stores only the users and chats
# TODO: Add stickers and sticker packs to this database too
Migrations may be placed on top of this database definition

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
create index template_resource_id_index
    on template_resource (id);
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
