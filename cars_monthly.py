#!/usr/bin/python
import os
import gspread
import pandas as pd

from datetime import datetime
from environs import Env
from gspread_formatting import set_frozen

from s3_api import S3Client
from app_logger import get_logger


logger = get_logger(__name__)

env = Env()
env.read_env()


BASE_DIR = os.path.abspath(".")

S3_CONFIG = {
    "aws_access_key_id": env.str("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": env.str("AWS_SECRET_ACCESS_KEY"),
    "endpoint_url": env.str("S3_ENDPOINT_URL"),
    "region_name": env.str("S3_REGION_NAME")
}
S3_BUCKET = env.str("S3_BUCKET_NAME", default="my-bucket")

GSHEET_CONFIG = {
    "credentials_file": env.str("CREDENTIALS_FILE"),
    "sheet_url": env.str("GSHEET_URL")
}


def is_file_name_on_1st(file_name: str) -> bool:
    """Check if the given file name is for the 1st day of the month"""
    date_part = int(file_name.split("_")[1].split(".")[0][6:8])
    return date_part == 1


def get_sheet_name(file_name: str) -> str:
    """Get the sheet name from the given file name"""
    date_str = file_name.split("_")[1][:8]
    date = datetime.strptime(date_str, "%Y%m%d")
    return date.strftime("%Y-%m-%d")


def clean_dataframe(file_path: str) -> pd.DataFrame:
    """Clean up the dataframe from the given file"""
    df = pd.read_csv(file_path)

    # Drop rows with missing values
    df = df.dropna(subset=["VIN", "Number", "Status", "Region", "Department"])

    # Drop rows with invalid values
    df = df[
        ~(df.Status.isin(["АРХИВ", "ЛИЧНАЯ"])) &
        ~(df.Department.isin(["ЛИЧНАЯ", ])) &
        ~(df.Model.isin(["БЭТМОБИЛЬ", ])) &
        ~(df.YearCar.isin(["0001-01-01T00:00:00", ]))
    ]

    # Clean up the YearCar column
    df["YearCar"] = df["YearCar"].apply(lambda x: str(x)[:4])

    # Clean up the timestamp column
    df["Дата"] = df["timestamp"].apply(
        lambda x: datetime.strptime(str(x), "%Y%m%d%H%M%S").strftime("%Y-%m-%d")
    )

    # Drop unnecessary columns
    df = df[
        [
            "Дата", "Model", "YearCar", "Number", "VIN",
            "Department", "Region", "Status"
        ]
    ].fillna('')

    # Rename columns
    df.columns = [
        "date", "model", "year", "number", "vin",
        "department", "region", "status"
    ]

    # Sort the dataframe
    df = df.sort_values(
        by=["model", "year", "department", "region"],
        ascending=[True, True, True, True],
        na_position="first"
    )

    return df


# Main script
def main() -> None:
    # Initialize S3 and Google Sheets clients
    s3_client = S3Client(S3_CONFIG)
    gspread_client = gspread.service_account(
        filename=GSHEET_CONFIG["credentials_file"])
    spreadsheet = gspread_client.open_by_url(GSHEET_CONFIG["sheet_url"])
    worksheets = spreadsheet.worksheets()

    # Download and process files from S3
    s3_objects = s3_client.list_objects(S3_BUCKET)
    for s3_object in s3_objects:
        if ("cars" in s3_object
                and is_file_name_on_1st(s3_object)
                and s3_object not in os.listdir(os.path.join(BASE_DIR, "data"))):
            s3_client.download_object(
                S3_BUCKET, s3_object, os.path.join(BASE_DIR, s3_object))

    # Process each file and update corresponding sheets
    data_files = os.listdir(os.path.join(BASE_DIR, "data"))
    for data_file in data_files:
        logger.info(f"Processing {data_file}")
        dataframe = clean_dataframe(os.path.join(BASE_DIR, "data", data_file))
        sheet_title = get_sheet_name(data_file)
        dataframe_rows = len(dataframe)
        dataframe_cols = len(dataframe.columns)

        if sheet_title not in [ws.title for ws in worksheets]:
            logger.info(f"Creating sheet {sheet_title}")
            worksheet = spreadsheet.add_worksheet(
                title=sheet_title, rows=dataframe_rows, cols=dataframe_cols)
        else:
            logger.info(f"Updating sheet {sheet_title}")
            worksheet = spreadsheet.worksheet(sheet_title)
            worksheet.resize(
                worksheet.row_count + dataframe_rows, dataframe_cols)

        worksheet.update(
            [dataframe.columns.values.tolist()] + dataframe.values.tolist())
        set_frozen(worksheet, rows=1)
        logger.info(f"Sheet {sheet_title} processed")

    # Aggregate data into "all_data" sheet
    consolidated_data = []
    for ws in worksheets:
        if ws.title not in ["all_data", "сводная таблица"]:
            consolidated_data += ws.get_all_values()[1:]

    if "all_data" not in [ws.title for ws in worksheets]:
        all_data_sheet = spreadsheet.add_worksheet(
            title="all_data", rows=1, cols=dataframe_cols)
        worksheets.append(all_data_sheet)
    else:
        all_data_sheet = spreadsheet.worksheet("all_data")

    header_row = worksheets[1].get_all_values()[0]
    all_data_sheet.resize(len(consolidated_data) + 1, len(header_row))
    all_data_sheet.clear()
    all_data_sheet.update([header_row] + consolidated_data)
    set_frozen(all_data_sheet, rows=1)

    logger.info("All data updated successfully")


if __name__ == "__main__":
    main()
