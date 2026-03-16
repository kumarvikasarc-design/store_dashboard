import json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/business.manage"]

CREDENTIALS_PATH = r"C:\Users\ACER\store_dashboard\feedback\credentials.json"

def main():
    flow = InstalledAppFlow.from_client_secrets_file(
        CREDENTIALS_PATH, SCOPES
    )
    creds = flow.run_local_server(port=0)

    token_data = {
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri
    }

    with open("token.json", "w") as f:
        json.dump(token_data, f, indent=4)

    print("✅ token.json created successfully!")

if __name__ == "__main__":
    main()