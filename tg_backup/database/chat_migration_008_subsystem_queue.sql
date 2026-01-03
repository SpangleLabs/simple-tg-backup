/**
  This migration adds a subsystem queue table in the chat databases. This allows chat archival to resume subsystem data
  processing after improper shutdown and incomplete archival.
 */

create table subsystem_queue
(
    queue_entry_id  integer not null
        constraint subsystem_queue_pk
            primary key autoincrement,
    subsystem_name  text    not null,
    message_id      integer,
    extra_data_json text
);
