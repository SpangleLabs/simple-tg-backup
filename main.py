import asyncio
import logging

from telethon import TelegramClient

from tg_backup.config import load_config
from tg_backup.target import backup_target

logger = logging.getLogger(__name__)


def main() -> None:
    conf = load_config()
    client = TelegramClient('simple_backup', conf["client"]["api_id"], conf["client"]["api_hash"])
    client.start()
    loop = asyncio.get_event_loop()
    for target_conf in conf["backup_targets"]:
        loop.run_until_complete(backup_target(client, target_conf))
    logger.info("All backups complete")


if __name__ == '__main__':
    main()
