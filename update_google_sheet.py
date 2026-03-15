import os
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Adjust this path if your merged file lives somewhere else
MERGED_PATH = os.path.join("data", "merged", "HAYSA_SSSL_Merged_Spring_2026.xlsx")
SHEET_NAME = "HAYSA SSSL Mismatches"   # Google Sheet name
TAB_NAME = "Mismatches"                # Tab name in that sheet


def get_credentials_from_env():
    key_json = os.environ["GOOGLE_SHEETS_KEY"]
    key_dict = json.loads(key_json)

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    return creds


def main():
    # 1. Load mismatches from merged Excel
    df = pd.read_excel(MERGED_PATH, sheet_name="Mismatches")

    # 2. Auth to Google Sheets
    creds = get_credentials_from_env()
    client = gspread.authorize(creds)

    # 3. Open sheet and tab
    sh = client.open(SHEET_NAME)
    try:
        worksheet = sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=TAB_NAME, rows="100", cols="20")

    # 4. Clear existing content
    worksheet.clear()

    # 5. Write header + data
    values = [df.columns.tolist()] + df.astype(str).fillna("").values.tolist()
    worksheet.update("A1", values)

    print("Updated Google Sheet with mismatches.")


if __name__ == "__main__":
    main()
