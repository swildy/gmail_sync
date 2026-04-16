# gmail_sync
Sync job to import yahoo email into gmail. Replaces gmailify pop import, now that it's shutting down. This is a one-way sync.

Notes: 
* Python script
* oauth for gmail authentication
* User login for yahoo authentication
* Uses google api import so that emails still get categorized and spam filtered by gmail
* Uses IMAP for yahoo fetch
* Base functionality:
  - Config file for main settings
  - Syncs read/unread status
  - Configurable label to identify synced messages in gmail
  - Syncs timestamp from source email so delays in sync do not affect receipt time of messages
  - Tracks message ids in SQLite DB to prevent duplicates
  - Employs retry logic with escalating backoff in case of IMAP or google api issues
  - Configurable message limit per run
  - Moves Yahoo emails to trash folder after successful sync

To Do:
* Add folder mapping b/w yahoo and gmail
* Add periodic cleanup of synced messages DB
