#!/usr/bin/env python3
import imaplib
import time
import logging
import yaml
import sqlite3
import argparse
from logging.handlers import RotatingFileHandler
from email.parser import BytesParser
from email.policy import default as default_policy
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone
import sys, os
import re
import psutil
from gmail_importer import get_gmail_service, import_raw_message, get_or_create_label



# -----------------------------------
# INIT
# -----------------------------------
LOCKFILE = "/tmp/gmail_sync.lock"

def acquire_lock():
    if os.path.exists(LOCKFILE):
        with open(LOCKFILE) as f:
            pid = int(f.read().strip())
        if psutil.pid_exists(pid):
            print("Sync already running. Quitting")
            raise SystemExit(0) # safe exit, does NOT kill other processes
        else:
            print("Stale lockfile found - removing")
            os.remove(LOCKFILE)
    with open(LOCKFILE, "w") as f:
        f.write(str(os.getpid()))

def release_lock():
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)


# -----------------------------------
# CONFIG
# -----------------------------------
CONFIG_PATH = "config.yaml"

if not os.path.exists(CONFIG_PATH):
    print(f"Config file not found: {CONFIG_PATH}")
    sys.exit(1)
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

YAHOO  = cfg["yahoo"]
GMAIL  = cfg["gmail"]
LOGCFG = cfg["logging"]
SYNC   = cfg["sync"]
STATE  = cfg["state"]

MAX_RETRIES  = SYNC.get("max_retries", 5)
BASE_BACKOFF = SYNC.get("base_backoff_seconds", 2)
TEST_SUBJECT = SYNC.get("test_subject", "imap_sync_test")
SKIP_DAYS    = SYNC.get("skip_older_than_days", 0)
FOLDERS      = [f.strip() for f in SYNC.get("folders", "INBOX").split(",") if f.strip()]
DB_PATH      = STATE.get("db_path", "state.db")
GMAIL_LABEL  = YAHOO["username"]
YAHOO_PORT   = YAHOO.get("imap_port", 993)
GMAIL_PORT   = GMAIL.get("imap_port", 993)
MAX_PER_RUN  = SYNC.get("max_per_run", 2)

CATEGORY_TO_MAILBOX = {
    "Promotions": "[Gmail]/All Mail",
    "Social": "[Gmail]/All Mail",
    "Updates": "[Gmail]/All Mail",
    None: "INBOX"
}


# -----------------------------------
# CLI ARGUMENTS
# -----------------------------------
parser = argparse.ArgumentParser(description="Yahoo -> Gmail sync tool")
parser.add_argument("--test", action="store_true", help="Sync only test messages (subj: {TEST_SUBJECT!r})")
parser.add_argument("--prod", action="store_true", help="Sync all messages")
parser.add_argument("--dry-run", action="store_true", help="Log actions without syncing")
args = parser.parse_args()

if args.test and args.prod:
    print("Choose either --test or --prod, not both.")
    sys.exit(1)

MODE = "test" if args.test else "prod"
DRY_RUN = args.dry_run


# -----------------------------------
# LOGGING 
# -----------------------------------
logger = logging.getLogger("gmail_sync")
logger.setLevel(logging.INFO)
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

handler = RotatingFileHandler(
    LOGCFG["file"],
    maxBytes=LOGCFG["max_bytes"],
    backupCount=LOGCFG["backup_count"], 
    encoding="utf-8"
)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    "%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

error_logger = logging.getLogger("gmail_sync_errors")
error_logger.setLevel(logging.ERROR)

error_handler = RotatingFileHandler(
    LOGCFG["error_file"],
    maxBytes=LOGCFG["max_bytes"],
    backupCount=LOGCFG["backup_count"],
    encoding="utf-8"
)
error_handler.setFormatter(formatter)
error_logger.addHandler(error_handler)


# -----------------------------------
# SQLITE STATE DB
# -----------------------------------
#os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS synced_messages (
    message_id TEXT NOT NULL,
    yahoo_uid TEXT NOT NULL,
    folder TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    PRIMARY KEY (message_id, yahoo_uid)
)
""")
conn.commit()

def already_synced(message_id, yahoo_uid):
    cur.execute(
        "SELECT 1 FROM synced_messages WHERE message_id=? AND yahoo_uid=?",
        (message_id, yahoo_uid)
    )
    return cur.fetchone() is not None

def record_synced(message_id, yahoo_uid, folder):
    cur.execute(
        "INSERT OR IGNORE INTO synced_messages VALUES (?, ?, ?, ?)",
        (message_id, yahoo_uid, folder, datetime.utcnow().isoformat())
    )
    conn.commit()


# -----------------------------------
# HELPER FUNCS
# -----------------------------------
def sanitize_headers(raw_msg, yahoo_address):
    """
    Ensures Reply-To is a single, safe RFC822 header line.
    Removes CRLF injection, folding, and empty values.
    Also removes yahoo_address from reply to so Gmail falls back to From
    Returns clean raw message
    """
    msg_obj = BytesParser(policy=default_policy).parsebytes(raw_msg)
    reply_to = msg_obj.get("Reply-To")
    if not reply_to:
        return raw_msg

    # Remove CRLF injection and folding
    clean = " ".join(reply_to.splitlines()).strip()

    if clean: 
        # If Reply-To is your Yahoo address, remove it so Gmail falls back to From
        if yahoo_address.lower() in clean.lower():
            del msg_obj["Reply-To"]
        else:
            msg_obj.replace_header("Reply-To", clean)
    else: 
        # If the sanitized value is empty, remove the header entirely
        del msg_obj["Reply-To"]

    return msg_obj.as_bytes()

def internaldate_to_timestamp(date_str):
    try:
        dt = datetime.strptime(date_str, "%d-%b-%Y %H:%M:%S %z")
        return dt.timestamp()
    except Exception:
        return None

#def get_gmail_safe_internaldate(imap_conn, uid):
#    raw = get_internaldate_raw(imap_conn, uid)
#    if not raw:
#        return None
#
#    ts = internaldate_to_timestamp(raw)
#    if ts is None:
#        return None
#
#    return imaplib.Time2Internaldate(ts)

def detect_gmail_category(msg):
    """
    Returns one of: 'Promotions', 'Social', 'Updates', or None
    """

    from_addr = (msg.get("From") or "").lower()
    subject = (msg.get("Subject") or "").lower()

    # --- Social ---
    social_domains = [
        "facebook.com", "instagram.com", "twitter.com", "x.com",
        "linkedin.com", "tiktok.com", "pinterest.com", "snapchat.com"
    ]
    if any(d in from_addr for d in social_domains):
        return "Social"

    # --- Promotions ---
    if msg.get("List-Unsubscribe"):
        return "Promotions"

    promo_keywords = [
        "sale", "deal", "discount", "offer", "promo",
        "newsletter", "coupon", "limited time"
    ]
    if any(k in subject for k in promo_keywords):
        return "Promotions"

    # --- Updates ---
    update_keywords = [
        "receipt", "invoice", "statement", "alert",
        "notification", "update", "billing"
    ]
    if any(k in subject for k in update_keywords):
        return "Updates"

    # Default -> Primary (no label)
    return None


# -----------------------------------
# RETRY DECORATOR
# -----------------------------------
def retryable(func):
    def wrapper(*args, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait = BASE_BACKOFF ** attempt
                logger.warning(
                    f"{func.__name__} failed (attempt {attempt}/{MAX_RETRIES}): {e}. "
                    f"Retrying in {wait} seconds."
                )
                time.sleep(wait)
        logger.error(f"{func.__name__} failed after {MAX_RETRIES} attempts.")
        raise RuntimeError(f"{func.__name__} failed repeatedly")
    return wrapper


# -----------------------------------
# IMAP HELPERS
# -----------------------------------
@retryable
def connect_imap(host, port, user, password):
    logger.info(f"Connecting to {host} as {user}...")
    m = imaplib.IMAP4_SSL(host, port)
    m.login(user, password)
    return m

@retryable
def get_uids(imap_conn, folder="INBOX"):
    imap_conn.select(folder)
    typ, data = imap_conn.search(None, "ALL")
    if typ != "OK":
        return {}
    return data[0].split()

@retryable
def fetch_header_fields(imap_conn, uid, fields):
    """ 
    Takes list of fields to fetch from header. 
    Returns dictionary of header field values.
    """
    h_fields = dict.fromkeys(fields, None)
    # Quote the field names to avoid Yahoo IMAP parser bugs
    fields_quoted = ' '.join([f'"{field}"' for field in fields])
    try:
        typ, msg_data = imap_conn.fetch(uid, f"(BODY.PEEK[HEADER.FIELDS ({fields_quoted})])")
        if typ != "OK" or not msg_data or msg_data[0] is None:
            return h_fields
        raw = msg_data[0][1]
        if not raw:
            return h_fields
        data = BytesParser(policy=default_policy).parsebytes(raw)
        for field in fields:
            if field in data:
                h_fields[field] = data[field]
    except Exception as e:
        if "CLIENTBUG" in str(e):
            logger.error(f"Yahoo returned CLIENTBUG for UID {uid} - skipping")
            return h_fields
        raise
    return h_fields


@retryable
def fetch_header_field(imap_conn, uid, field):
    # Quote the field name to avoid Yahoo IMAP parser bugs
    field_quoted = f'"{field}"'
    typ, msg_data = imap_conn.fetch(uid, f"(BODY.PEEK[HEADER.FIELDS ({field_quoted})])")
    if typ != "OK" or not msg_data or msg_data[0] is None:
        return None

    raw = msg_data[0][1]
    if not raw:
        return None

    msg = BytesParser(policy=default_policy).parsebytes(raw)
    return msg[field]

@retryable
def fetch_headers(imap_conn, uid):
    typ, msg_data = imap_conn.fetch(uid, "(BODY.PEEK[HEADER])")
    if typ != "OK":
        return None
    return msg_data[0][1].decode(errors="replace")

@retryable
def fetch_full_message(imap_conn, uid):
    typ, msg_data = imap_conn.fetch(uid, "(BODY.PEEK[])")
    if typ != "OK":
        return None
    return msg_data[0][1]

@retryable
def append_to_gmail(gmail_conn, raw_msg, internaldate, label=None, seen=False):

    # Parse message for category detection
    msg_obj = BytesParser(policy=default_policy).parsebytes(raw_msg)
    category = detect_gmail_category(msg_obj)
    mailbox = CATEGORY_TO_MAILBOX[category]
    category_label = "\\" + category if category else category

    flags = "(" + " ".join( p for p in (["\\Seen"] if seen else []) + ([label] if label else []) +
         ([category_label] if category_label else [])) + ")"

    logger.info(f"Appending to Gmail mailbox: {mailbox}, Seen: {seen}, Label: {label}, Flags: {flags}")
    gmail_conn.append(mailbox, flags, internaldate, raw_msg)

@retryable
def delete_from_yahoo(yahoo_conn, uid):
    # first copy to trash so it shows up immediately
    logger.info("    copying msg to Trash folder")
    yahoo_conn.copy(uid, '"Trash"')
    # then delete original 
    logger.info("    deleting msg from Inbox")
    yahoo_conn.store(uid, "+FLAGS", "\\Deleted")

@retryable
def get_internaldate_raw(imap_conn, uid):
    typ, msg_data = imap_conn.fetch(uid, "(INTERNALDATE)")
    if typ != "OK" or not msg_data:
        return None

    # msg_data[0] can be:
    #   - bytes: b'1 (INTERNALDATE "12-Jul-2024 10:23:45 +0000")'
    #   - tuple: (b'1 (INTERNALDATE "12-Jul-2024 10:23:45 +0000")', b'')
    first = msg_data[0]

    if isinstance(first, tuple):
        line = first[0].decode(errors="replace")
    else:
        line = first.decode(errors="replace")

    m = re.search(r'INTERNALDATE "([^"]+)"', line)
    if not m:
        return None

    return m.group(1)  # exact string Gmail expects, e.g. '12-Jul-2024 10:23:45 +0000'

@retryable
def yahoo_is_seen(imap_conn, uid):
    typ, msg_data = imap_conn.fetch(uid, "(FLAGS)")
    if typ != "OK":
        return False

    # msg_data[0][0] might be bytes or tuple
    first = msg_data[0]
    if isinstance(first, tuple):
        line = first[0].decode(errors="replace")
    else:
        line = first.decode(errors="replace")

    return "\\Seen" in line



# -----------------------------------
# MAIN SYNC LOGIC
# -----------------------------------
def main():
    now_time = datetime.now()
    start_time = time.perf_counter()
    print("#########################################################")
    print(f"Started at: {now_time.strftime('%Y-%m-%d %H:%M:%S')}")
    acquire_lock()

    success = True
    yahoo = None
    gmail = None

    try:
        logger.info(f"\n########################################################################")
        logger.info(f"Starting Yahoo -> Gmail sync (mode={MODE}, dry_run={DRY_RUN})")
        logger.info(f"Configs:")
        logger.info(f"      MAX_RETRIES:   {MAX_RETRIES}")
        logger.info(f"      MAX_PER_RUN:   {MAX_PER_RUN}")
        logger.info(f"      BASE_BACKOFF:  {BASE_BACKOFF}")
        logger.info(f"      SKIP_DAYS:     {SKIP_DAYS}")
        logger.info(f"      TEST_SUBJECT:  {TEST_SUBJECT}")
        logger.info(f"      FOLDERS:       {FOLDERS}")
        logger.info(f"      DB_PATH:       {DB_PATH}")
        logger.info(f"      GMAIL_LABEL:   {GMAIL_LABEL}")
        logger.info(f"      YAHOO_PORT:    {YAHOO_PORT}")
        logger.info(f"      GMAIL_PORT:    {GMAIL_PORT}")

        logger.info(f"Connecting to Yahoo via IMAP")
        yahoo = connect_imap( YAHOO["imap_host"], YAHOO_PORT, YAHOO["username"], YAHOO["password"])

        logger.info(f"Connecting to Gmail via OAUTH")
        gmail = get_gmail_service()

        # Get gmail label id
        GMAIL_LABEL_ID = get_or_create_label(gmail, GMAIL_LABEL) if GMAIL_LABEL else None

        for folder in FOLDERS:
            processed_count = 0

            logger.info(f"Processing Yahoo folder: {folder}")
            uids = get_uids(yahoo, folder)
            logger.info(f"{len(uids)} messages found in Yahoo {folder}")

            for uid in uids:
                logger.info(f"Processed count: {processed_count}")
                # Stop if we've hit the per-run limit
                if processed_count >= MAX_PER_RUN:
                    logger.info(f"Reached max-per-run limit of {MAX_PER_RUN}. Stopping early.")
                    break

                uid_str = uid.decode()

                # Fetch message headers for filtering
                logger.info(f"Fetching header fields for uid {uid} ({uid_str})")
                head_fields = fetch_header_fields(yahoo, uid, ["MESSAGE-ID", "SUBJECT", "DATE"])
                message_id = head_fields["MESSAGE-ID"]
                subject    = head_fields["SUBJECT"]
                date_hdr   = head_fields["DATE"]
                if not message_id or not date_hdr:
                    continue   # skip this uid

                # Fetch Message-ID
                #message_id = fetch_header_field(yahoo, uid, "MESSAGE-ID")
                if not message_id:
                    logger.info(f"Skipping UID {uid_str} (no Message-ID)")
                    continue

                # Check state DB
                if already_synced(message_id, uid_str):
                    logger.info(f"Skipping UID {uid_str} (Already synced)")
                    continue

                # Fetch subject
                #subject = fetch_header_field(yahoo, uid, "SUBJECT")

                # Test mode filter
                if MODE == "test" and subject != TEST_SUBJECT:
                    logger.info(f"[TEST MODE] Skipping non-test msg: {subject} (folder='{folder}')")
                    continue

                # Age filter
                #date_hdr = fetch_header_field(yahoo, uid, "DATE")
                if date_hdr:
                    try:
                        dt = parsedate_to_datetime(date_hdr)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        cutoff = datetime.now(timezone.utc) - timedelta(days=SKIP_DAYS)
                        if dt < cutoff:
                            logger.info(f"Skipping msg older than {SKIP_DAYS} days: {subject}")
                            continue
                    except Exception:
                        pass

                # DRY RUN: log only, no side effects
                if DRY_RUN:
                    logger.info(f"    [DRY RUN] Would sync {message_id} (subj='{subject}')")
                    processed_count += 1
                    continue

                logger.info(f"    Syncing msg: {message_id}:\n    subject='{subject}')")

                try:
                    raw_msg = fetch_full_message(yahoo, uid)
                    if not raw_msg:
                        raise RuntimeError("Failed to fetch full msgs")

                    # Strip Reply-To pointing Yahoo address and ensure valid RFC822 reply-to
                    raw_msg_clean = sanitize_headers(raw_msg, YAHOO["username"])

                    # Get internal date from Yahoo and format for Gmail
                    internaldate = get_internaldate_raw(yahoo, uid)
                    #internaldate = get_gmail_safe_internaldate(yahoo, uid)
                    if not internaldate:
                        # Fallback: current time in Gmail-friendly format
                        internaldate = imaplib.Time2Internaldate(time.time()).replace('"', '')
                    logger.info(f"    InternalDate: {internaldate}")

                    # get 'seen' status for message from yahoo
                    seen = yahoo_is_seen(yahoo, uid)

                    logger.info(f"    Importing msg to Gmail (subj='{subject}')")
                    if raw_msg_clean is None:
                        logger.info(f"    ERROR: raw_msg is None for UID: {uid}")
                        return None
                    if not isinstance(raw_msg_clean, (bytes, bytearray)):
                        logger.info(f"    ERROR: raw_msg is not bytes: {type(raw_msg_clean)}")
                        return None
                    #logger.info(f"raw_msg appears to be good, type: {type(raw_msg_clean)}")

                    import_raw_message(gmail, raw_msg_clean, internaldate, GMAIL_LABEL_ID, seen)
                    logger.info(f"    Deleting msg from Yahoo (subj='{subject}')")
                    delete_from_yahoo(yahoo, uid)
                    logger.info(f"    Recording synced msg to DB (subj='{subject}')")
                    record_synced(message_id, uid_str, folder)

                    logger.info(f"Synced and deleted {message_id}")
                    processed_count += 1

                except Exception as e:
                    logger.error(f"ERROR: Message sync failed: {e}\n    uid={message_id}  subj={subject}")
                    headers = fetch_headers(yahoo, uid)
                    error_logger.error(
                        f"Message-ID: {message_id}\n"
                        f"Subject: {subject}\n"
                        f"Error: {e}\n"
                        f"Headers:\n{headers}\n"
                        f"{'-'*60}"
                    )
                    continue

        if not DRY_RUN:
            yahoo.expunge()
            logger.info("Yahoo expunge completed")

    except Exception as e:
        success = False
        logger.error(f"System-level failure: {e}")

    finally:
        try:
            yahoo.logout()
        except:
            pass
        try:
            gmail.logout()
        except:
            pass

        release_lock()
        end_time = time.perf_counter()
        now_time = datetime.now()
        print(f"Finished at: {now_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Run time: {(end_time - start_time):.2f} seconds")
        if success:
            logger.info(f"Sync complete. Run time: {(end_time - start_time):.2f} seconds")
        else:
            logger.info(f"Sync failed. Run time: {(end_time - start_time):.2f} seconds")

if __name__ == "__main__":
    main()

