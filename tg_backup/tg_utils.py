from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types.messages import Messages


async def get_message_count(client, entity, latest_id=0):
    get_history = GetHistoryRequest(
        peer=entity,
        offset_id=0,
        offset_date=None,
        add_offset=0,
        limit=1,
        max_id=0,
        min_id=latest_id or 0,
        hash=0
    )
    history = await client(get_history)
    if isinstance(history, Messages):
        count = len(history.messages)
    else:
        count = history.count
    return count


def get_chat_name(entity):
    if hasattr(entity, "title"):
        return f"#{entity.title}"
    else:
        return get_user_name(entity) or str(entity.id)


def get_user_name(user):
    if hasattr(user, "title"):
        return f"#{user.title}"
    full_name = (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)
    if full_name == "":
        return "DELETED_ACCOUNT"
    return full_name.replace(" ", "_")


def get_from_obj_by_path(obj: object, json_path: str) -> object:
    # Find dict keys
    first_key = json_path
    remaining_path = None
    if "." in json_path:
        first_key, remaining_path = json_path.split(".", 1)
    # Find list indexes
    index = None
    if "[" in first_key and first_key.endswith("]"):
        first_key, index_str = first_key[:-1].split("[", 1)
        index = int(index_str)
    # Fetch the thing
    sub_obj = obj.__getattribute__(first_key)
    if index is not None:
        sub_obj = sub_obj[index]
    # Return
    if remaining_path:
        return get_from_obj_by_path(sub_obj, remaining_path)
    return sub_obj
