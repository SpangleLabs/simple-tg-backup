/**
  This migration adds a new table for storing archive records, this is the record of which archival runs have been done.
  This was needed because previously we were mostly relying on the list of chats, but that doesn't include archived users.
 */

create table archive_runs
(
    archive_run_id text not null -- Stores a uuidv4
        constraint table_name_pk
            primary key,
    target_type         text    not null, -- Chat or User
    target_id           integer not null,
    time_queued         text,
    history_time_start  text,
    history_time_latest text, -- Time of the latest processed message
    history_time_end    text,
    follow_time_start   text,
    follow_time_latest  text, -- Time of the latest processed message
    follow_time_end     text,
    behaviour_config    text,  -- JSON formatted copy of this archive's behaviour config
    completed           boolean,
    failure_reason      text, -- Text, explaining why the archive was stopped
    archive_stats       text -- JSON formatted dict of archive statistics. How many messages were parsed how many archived and such
);

