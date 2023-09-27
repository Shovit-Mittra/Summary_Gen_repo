import json
import smtplib
from email.message import EmailMessage

import boto3
import numpy as np
import openpyxl
import pandas as pd
import s3fs
from process_function import (
    compare_columns,
    generate_validation_report,
    read_files,
    save_reports,
    send_email,
    store_level_summary,
)


def lambda_handler(event, context):
    
    if 'Records' in event:  # S3 event
        input_bucketName = event["Records"][0]["s3"]["bucket"]["name"]
        input_fileName = event["Records"][0]["s3"]["object"]["key"]
        print("S3 event received")
        
    if "us-prod-report-source-data" in input_bucketName:
        market = "us"
    else:
        market = None
    
    if market:
        with open("lambda_config.json") as f:
            cfg = json.load(f)
        receiver_email = cfg['receiver_email']
        secret_name = cfg['secret_name']
        region_name = cfg['region_name']
        email_port = cfg['email_port']
        ENV = cfg["ENV"]
        portal_report_path = "s3://" + input_bucketName + "/" + input_fileName
        GPT_report_path = cfg['paths'][market]['GPT_file_path']
        summary_report_path = cfg['paths'][market]['output_s3_path']
        cc_recipients = cfg['cc_recipients']
        portal_excel_sheet_names = cfg['constants']['portal_excel_sheet_names']
        path_prefixes = cfg['constants']['path_prefixes']

        print(f"receiver_email: {receiver_email}")
        print(f"ENV : {ENV}")
        print(f"portal_report_path: {portal_report_path}")
        print(f"GPT_report_path: {GPT_report_path}")
        print(f"summary_report_path: {summary_report_path}")
        print(f"cc_recipients: {cc_recipients}")
        print(f"portal_excel_sheet_names: {portal_excel_sheet_names}")
        print(f"path_prefixes: {path_prefixes}")
        
        generate_validation_report(
            env = ENV,
            market = market,
            portal_report_path = portal_report_path, 
            GPT_report_path = GPT_report_path, 
            summary_report_path = summary_report_path,
            portal_excel_sheet_names = portal_excel_sheet_names,  
            path_prefixes = path_prefixes, 
            secret_name = secret_name,
            region_name = region_name,
            email_port = email_port,
            receiver_email = receiver_email,
            cc_recipients = cc_recipients
        )
        
        return {
            "statusCode": 200,
            "body": f"validation successfully",
        }
    else:
        return {
            "statusCode": 400,
            "body": "ERROR: EMR cluster cannot be started as market is not identified",
        }
    