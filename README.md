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
- Something to actually mark when a chat is totally completed, got to the end of message history, finished all subsystems, etc
  - Needs to be in the chat_db itself, not core_db, because what if it's nuked?
  - It could mark the earliest ID its seen, and have the archive history actually check both top and bottom?
    - That could be done without DB changes, but the media and sticker part??
  - Maybe the alternative is that the subsystems could serialise and reload their queues? They all know their ArchiveTarget now.
    - Queue table in each chat DB?
      - StickerDownloader could just save message ID
      - MediaDownloader might want to save media ID too? But message ID would be sufficient, I bet
      - PeerDataFetcher?.. Not certain we care, but maybe
- Don't mark archive targets end time until it's done?
- Abort button, which stops archiver at end of current target
- Data deduplication
  - Deduplicate admin events in the same way messages are?
  - Deduplicate users in the same way messages are
  - Deduplicate chats in the same way messages are
- Ability to specify notes on dialogs archive settings. (Like, why are you overriding defaults)
- Implement web UI form for setting behaviour config for individual dialogs
- Use Takeout API to get old dialogs
  - Can only use it once a day though.. But shouldn't need it any more than once
  - https://docs.telethon.dev/en/stable/modules/client.html#telethon.client.account.AccountMethods.takeout
  - https://core.telegram.org/api/takeout

### TODO: Later
- Behaviour setting to only download necessary stickers, rather than entire packs?
- HTML view of chats?
  - Will need access control
  - Web UI view for any of the archived data at all?
- Archive user and chat profile photos
- Call GetPollResultsRequest and gather poll results `PollResults` after getting poll with MessageMediaPoll?

