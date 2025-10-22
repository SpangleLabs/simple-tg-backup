# Simple TG Backup

A simple utility to backup Telegram chats.
Unsure how simple, yet. It's a work in progress, and not very functional yet.

## Key goals of the project:
- Data is archived as complete as possible, with additional fields parsed out, rather than only storing parsed fields
- Each chat should be stored relatively self-contained, so they could be zipped and transferred
  - This will mean minor duplication of media, but that seems worth it


## TODO lists
### TODO: Would be good
- Ability to have the archiver start, and not check the whole chat history?
  - Add `msg_history_overlap_days` to behaviour config. Make it such that it'll check that far back for edited and deleted messages
  - Reset the counter if it does find an edited or deleted message
  - Default it to 3 days or something (normally messages can be edited for 2 days)
- Further work on the web UI
  - Implement form for setting up filtered configuration for dialogs
    - Also, update the "default" value for dialogs in known dialogs list, when this is done
    - And then the functionality, ofc
- Data deduplication
  - Deduplicate admin events in the same way messages are?
  - Deduplicate users in the same way messages are
  - Deduplicate chats in the same way messages are
  - Double check how sticker and sticker set deduplication is working
- Store shared locations, with history

### TODO: Later
- Further work on the web UI
  - Implement form for setting behaviour config for individual dialogs
- Behaviour setting to only download necessary stickers, rather than entire packs
- HTML view of chats?
- Archive user and chat profile photos
- Ensure archiver can correctly archive targets with multiple usernames
- Store web page previews?


## Storage layout
- `store/` All the archived data goes in here
- `store/core_db.sqlite` Core database
- `store/chats/` Each chat goes in a directory in here, named by chat ID
- `store/chats/<id>/` Each chat has its own directory
- `store/chats/<id>/media/` Each chat has its own media directory. Media might be duplicated across multiple chats, that's fine.
- `store/chats/<id>/chat_db.sqlite` Each chat has its own database, with messages and such inside
- `store/stickers/` Stickers get stored in a separate directory to chats
- `store/stickers/<pack_id>/` Each sticker pack gets a directory by pack ID
- `store/stickers/<pack_id>/<sticker_id>.webp` Each sticker is stored by sticker ID
