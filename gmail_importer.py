# gmail_importer.py
import base64
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

def internaldate_to_epoch_ms(date_str):
    # Parse IMAP INTERNALDATE format
    dt = datetime.strptime(date_str, "%d-%b-%Y %H:%M:%S %z")
    # Convert to epoch milliseconds
    return int(dt.timestamp() * 1000)


def get_or_create_label(service, label_name):
    labels = service.users().labels().list(userId="me").execute().get("labels", [])

    # Look for existing label
    for label in labels:
        if label["name"] == label_name:
            return label["id"]

    # Create label if not found
    label_body = {
        "name": label_name,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show"
    }

    new_label = service.users().labels().create(
        userId="me",
        body=label_body
    ).execute()

    return new_label["id"]


def get_gmail_service():
    """
    Loads OAuth2 credentials from token.json or runs the OAuth flow.
    Returns an authenticated Gmail API service object.
    """
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        #print("##################################")
        #print("Debug credentials from token.json:")
        #print("Loaded creds: ", creds)
        #print("Token:        ", creds.token)
        #print("Refresh token:", creds.refresh_token)
        #print("Valid:        ", creds.valid)
        #print("Expired:      ", creds.expired)
        #print("##################################")


    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("GOOGLE AUTH: Refreshing oauth token")
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0, open_browser=False)
            print(f"GOOGLE AUTH: Manually authenticated google app, now saving creds")

            with open("token.json", "w") as token:
                token.write(creds.to_json())
                #token.write(json.dumps({
                #    "token": creds.token,
                #    "refresh_token": creds.refresh_token,
                #    "token_uri": creds.token_uri,
                #    "client_id": creds.client_id,
                #    "client_secret": creds.client_secret,
                #    "scopes": creds.scopes
                #}))
    else: 
        print("GOOGLE AUTH: Loaded google api oauth token is still valid")

    return build("gmail", "v1", credentials=creds)


def import_raw_message(service, raw_bytes, internaldate, label_id, yahoo_seen):
    """
    Uploads a raw RFC822 message to Gmail using users.messages.import.
    Gmail will:
      - preserve timestamp
      - auto-categorize (Promotions/Social/Updates)
      - auto-thread
      - spam-filter
    internaldate_ts must be a UNIX timestamp (seconds).
    """
    try:
        label = ["INBOX"] + [label_id] if label_id else []
        # by default import sets message to read, so set to unread if not seen in yahoo
        if not yahoo_seen:
            label.append("UNREAD")

        encoded = base64.urlsafe_b64encode(raw_bytes).decode("utf-8")
        epoch_ms = internaldate_to_epoch_ms(internaldate)
        body = {
            "raw": encoded,
            "internalDate": epoch_ms,
            "labelIds": label
        }
        return service.users().messages().import_(userId="me", body=body).execute()

    except Exception as e:
        print("Gmail import failed:", repr(e))
        raise

