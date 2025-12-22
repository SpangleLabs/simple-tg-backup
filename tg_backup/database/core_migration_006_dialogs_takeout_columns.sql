/**
  This migration adds a couple columns to the dialogs table to determine whether takeout client was used to fetch this
  dialog, and whether it's needed to fetch the dialog.
 */

alter table dialogs
    add needs_takeout boolean default FALSE;

alter table dialogs
    add last_used_takeout boolean default FALSE;
