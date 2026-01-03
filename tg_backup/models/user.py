import datetime
from typing import Optional

from telethon.tl.types.users import UserFull

from tg_backup.models.abstract_resource import AbstractResource


class User(AbstractResource):
    def __init__(
            self,
            archive_datetime: datetime.datetime,
            archive_tl_schema_layer: int,
            resource_id: int,
            resource_type: str,
            str_repr: str,
            dict_repr: Optional[dict],
    ) -> None:
        super().__init__(archive_datetime, archive_tl_schema_layer, resource_id, resource_type, str_repr, dict_repr)
        self.bio: Optional[str] = None
        self.birthday: Optional[datetime.date] = None
        self.is_bot: Optional[bool] = None
        self.is_deleted: Optional[bool] = None
        self.first_name: Optional[str] = None
        self.last_name: Optional[str] = None
        self.phone_number: Optional[str] = None
        self.has_premium: Optional[bool] = None
        self.username: Optional[str] = None
        self.other_usernames: Optional[list[str]] = None

    @classmethod
    def from_full_user(cls, full: UserFull) -> "User":
        # Construct the storable user object
        user_obj = cls.from_storable_object(full)
        # Dissect the object into the two parts
        full_user = full.full_user if hasattr(full, "full_user") else None
        user = full.users[0] if hasattr(full, "users") and len(full.users) > 0 else None
        # Set the user ID properly
        if user_obj.resource_id is None and full_user is not None:
            user_obj.resource_id = full_user.id
        if user_obj.resource_id is None and user is not None:
            user_obj.resource_id = user.id
        # Parse everything from the users.UserFull object
        if hasattr(full_user, "about"):
            user_obj.bio = full_user.about
        if hasattr(full_user, "birthday") and full_user.birthday is not None:
            user_obj.birthday = datetime.date(
                year=full_user.birthday.year or 4,
                month=full_user.birthday.month,
                day=full_user.birthday.day,
            )
        # Parse everything from the User object
        if hasattr(user, "bot"):
            user_obj.is_bot = user.bot
        if hasattr(user, "deleted"):
            user_obj.is_deleted = user.deleted
        if hasattr(user, "first_name"):
            user_obj.first_name = user.first_name
        if hasattr(user, "last_name"):
            user_obj.last_name = user.last_name
        if hasattr(user, "phone"):
            user_obj.phone_number = user.phone
        if hasattr(user, "premium"):
            user_obj.has_premium = user.premium
        if hasattr(user, "username"):
            user_obj.username = user.username
        if hasattr(user, "usernames") and user.usernames is not None:
            other_usernames = []
            for username in user.usernames:
                if hasattr(username, "username"):
                    other_usernames.append(username.username)
            user_obj.other_usernames = other_usernames
        # TODO: profile photos?
        return user_obj

    @property
    def full_name(self) -> Optional[str]:
        if self.first_name is None:
            if self.last_name is None:
                return None
            return self.last_name
        if self.last_name is None:
            return self.first_name
        return self.first_name + " " + self.last_name
