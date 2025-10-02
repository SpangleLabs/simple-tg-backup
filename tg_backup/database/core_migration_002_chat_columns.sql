/*
This migration is going to add a bunch more columns to the `chats` table, for additional information, some parsed from
ChatFull/ChannelFull objects. This should accompany a change in the UserDataFetcher, to fetch this chat data.
This will be applied to both the core DB and any chats DBs
*/
alter table chats
    add creation_date TEXT;

alter table chats
    add is_creator boolean;

alter table chats
    add have_left boolean;

alter table chats
    add is_broadcast_channel boolean;

alter table chats
    add participants_count integer;

alter table chats
    add about text;

alter table chats
    add username text;

alter table chats
    add other_usernames text;

alter table chats
    add migrated_to_chat_id integer;

alter table chats
    add migrated_from_chat_id integer;

alter table chats
    add linked_chat_id integer;

