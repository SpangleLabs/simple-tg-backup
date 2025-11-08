# Simple TG Backup

A simple utility to backup Telegram chats.
Unsure how simple, yet. It's a work in progress, and not very functional yet.

## Key goals of the project:
- Data is archived as complete as possible, with additional fields parsed out, rather than only storing parsed fields
- Each chat should be stored relatively self-contained, so they could be zipped and transferred
  - This will mean minor duplication of media, but that seems worth it

## Usage
The main way to use this is via the web UI, simply run the `main.py` file without arguments, and the web UI will be started up.
From the web UI you can modify the archiver settings (which are stored in `archive_settings.yaml`), and trigger the archiver to run.

You can also use the `main.py` command line arguments to specify an individual chat to archive, with `--chat-id`

## Storage layout
- `store/` All the archived data goes in here
- `store/core_db.sqlite` Core database
- `store/chats/` Each chat goes in a directory in here, named by chat ID
- `store/chats/<id>/` Each chat has its own directory
- `store/chats/<id>/media/` Each chat has its own media directory. Media might be duplicated across multiple chats, that's fine.
- `store/chats/<id>/web_page_media/` Each chat has a separate directory for web page media, which is any media in web previews and the like. This has a higher risk of duplication than `/media/`.
- `store/chats/<id>/chat_db.sqlite` Each chat has its own database, with messages and such inside
- `store/stickers/` Stickers get stored in a separate directory to chats
- `store/stickers/<pack_id>/` Each sticker pack gets a directory by pack ID
- `store/stickers/<pack_id>/<sticker_id>.webp` Each sticker is stored by sticker ID

## TODO lists
### TODO: Would be good
- Data deduplication
  - Deduplicate admin events in the same way messages are?
  - Deduplicate users in the same way messages are
  - Deduplicate chats in the same way messages are
  - Double check how sticker and sticker set deduplication is working
- Ability to specify notes on dialogs archive settings. (Like, why are you overriding defaults)

### TODO: Later
- Further work on the web UI
  - Implement form for setting behaviour config for individual dialogs
- Behaviour setting to only download necessary stickers, rather than entire packs?
- HTML view of chats?
- Archive user and chat profile photos
- Call GetPollResultsRequest and gather poll results `PollResults` after getting poll with MessageMediaPoll?

