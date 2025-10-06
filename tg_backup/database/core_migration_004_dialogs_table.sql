/**
  This migration adds a new table for storing dialog entries.
  These are telegram objects representing chats with users or groups.
  This should help with being able to select which dialogs should be archived in future.

  This differs from other objects, in not storing a history of the object, just current state, keyed on ID
 */

create table if not exists main.dialogs
(
    archive_datetime        text not null,
    archive_tl_scheme_layer integer not null,
    id                      integer not null,
    type                    text not null,
    str_repr                text not null,
    dict_repr               text,
    /* Custom fields below */
    chat_type               text,
    name                    text,
    pinned                  boolean,
    archived_chat           boolean,
    last_msg_date           text,
    first_seen              text not null,
    last_seen               text not null
);
create unique index dialogs_id_index
    on dialogs (id);