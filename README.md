# Simple TG Backup

A simple utility to backup Telegram chats.
Unsure how simple, yet. It's a work in progress, and not very functional yet.

## Key goals of the project:
- Data is archived as complete as possible, with additional fields parsed out, rather than only storing parsed fields
- Each chat should be stored relatively self-contained, so they could be zipped and transferred
  - This will mean minor duplication of media, but that seems worth it


## TODO lists
### TODO: Would be good
- Ability to archive groups where you are not an admin
- Ability to archive 1:1 chats, not just groups
- Ability to archive channels
- Allow specifying multiple chats to backup
  - Metrics on how many chats are being watched, what's the chat download queue? What's the total number of chats?
- Deduplicate admin events in the same way messages are?
- Deduplicate users in the same way messages are
- Deduplicate chats in the same way messages are
- Double check how sticker and sticker set deduplication is working

### TODO: full archiver
- List how many chats there are
- Store shared locations, with history

### TODO: eventually
- HTML view of chatting?
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
