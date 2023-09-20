import pandas as pd
import numpy as np
import smtplib
from email.message import EmailMessage
import boto3
import s3fs
import openpyxl
import json

def key_vault(secret_name, region_name):
    """
    Retrieves the required email details from the AWS Secrets Manager.
    
    Parameters
    ----------
    secret_name : str
        The name of the secret containing the email details.
    region_name : str
    The AWS region where the secret manager is located.

    Returns
    -------
    secret_string_json : dict
        A dictionary containing all the required for email.
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    secret_value = client.get_secret_value(SecretId=secret_name)
    secret_string_json = json.loads(secret_value["SecretString"])
    return secret_string_json

# read the files 
def read_files(emr_report_path, engine_report_path):
    """
    Read files from the specified paths and return the dataframes and report date.

    Parameters
    ----------
    emr_report_path : str 
        The path to the EMR report file.
    engine_report_path : str
        The path to the engine report directory.

    Returns
    -------
    tuple: 
        - emr_overall_summary : DataFrame 
            The Dataframe containing the overall summary.
        - emr_summary_per_store : DataFrame
            The Dataframe containing the summary per store.
        - engine_overall_summary : Dataframe 
            The DataFrame containing the PMIX summary from engine report.
        - engine_summary_per_store : Dataframe 
            The DataFrame containing the TotXStores summary from engine report.
        - report_date : str
            Date extracted from the EMR report filename.

    """
    Status = "True"
    message_dict = {}
    xls = pd.ExcelFile(emr_report_path)
    emr_overall_summary = pd.read_excel(xls, 'overall_summary')
    emr_summary_per_store = pd.read_excel(xls, 'summary_per_store')
    print(f"EMR Path: {emr_report_path}")
    report_date = emr_report_path.split("/")[-1].split(".xlsx")[0].split("Daily_Pmix_")[-1]
    engine_overall_summary_path = engine_report_path + "Daily_Pmix_Summary_" + report_date + ".txt.gz"
    print(f"Path: {engine_overall_summary_path}")
    engine_summary_per_store_path = engine_report_path + "Daily_Pmix_TotXStore_" + report_date + ".txt.gz"
    print(f"Path: {engine_summary_per_store_path}")
    
    engine_overall_summary = pd.DataFrame()
    engine_summary_per_store = pd.DataFrame()
    try:
        engine_overall_summary = pd.read_csv(engine_overall_summary_path, sep="\t")
        engine_overall_summary = engine_overall_summary.rename(columns={"total_units" : "total_alacarte_units"})
    except FileNotFoundError:
        Status = "False"
        print(f"Error: File '{engine_overall_summary_path}' not found.")
        message_dict["error"] = f"File not found in the path : {engine_overall_summary_path}"
    try:
        engine_summary_per_store = pd.read_csv(engine_summary_per_store_path, sep="\t")
        engine_summary_per_store = engine_summary_per_store.rename(columns=lambda x: x.lower())
    except FileNotFoundError:
        Status = "False"
        print(f"Error: File '{engine_summary_per_store_path}' not found.")
        message_dict["error"] = f"File not found in the path : {engine_summary_per_store_path}"
    return (emr_overall_summary, emr_summary_per_store, engine_overall_summary, engine_summary_per_store, report_date, message_dict, Status)

# creating the overall summary
def compare_columns(engine_column_name, emr_column_name, engine_df, emr_df):
    """
    Compare the values of two columns between two DataFrames.

    Parameters
    ----------
    engine_column_name : str
        The name of the column in the engine's report for overall summary.
    emr_column_name : str
        The name of the column in the EMR report for overall summary.
    engine_df : DataFrame 
        The DataFrame containing the summary for engine's report.
    emr_df : DataFrame 
        The DataFrame containing the summary for EMR's report.

    Returns
    -------
    tuple: A tuple containing the following elements:
        - engine_column_name : str
            The name of the column from engine's report.
        - engine_value : int 
            The value of the column from engine's report.
        - emr_value : int 
            The value of the column from EMR's report. If the column is not available, then the value is None.
        - status : bool
            True if the values in EMR's column and engine's columns are equal, False otherwise.
        - diff : int
            The difference between engine's value and EMR's value. If the column is not available, then the value is None.
    """
    if engine_column_name not in engine_df.columns:
        emr_value = int(emr_df[emr_column_name].iloc[0])
        return (engine_column_name, None, emr_value, None, None)
    engine_value = int(engine_df[engine_column_name].iloc[0])
    if emr_column_name == "NA" :
        return (engine_column_name, engine_value, None, None, None)
    emr_value = int(emr_df[emr_column_name].iloc[0])
    diff = engine_value - emr_value
    status = np.where(abs(diff) > 0.0005 * engine_value, "FAIL", "PASS")
    return (engine_column_name, engine_value, emr_value, diff, status)

def update_status(row):
    """
    Update the status columns based on deviation values and their respective GPT values.

    Parameters
    ----------
    row : pandas.Series
        A row from the DataFrame containing values of the required columns.

    Returns
    -------
    Series: 
        A Series containing the status values based on deviation conditions.

    Notes
    -----
    - If the deviation value is null, the corresponding status will be set as an empty string.
    - If the deviation value is not null, the status will be "FAIL" if the absolute deviation
        is greater than 0.05% of the corresponding value; otherwise, the status will be "PASS".
    """
    statuses = []
    
    if pd.notnull(row["Deviation in Total Net Sales"]):
        if abs(row["Deviation in Total Net Sales"]) > 0.0005 * row["Total Net Sales (GPT)"]:
            statuses.append("FAIL")
        else:
            statuses.append("PASS")
    else:
        statuses.append("")

    if pd.notnull(row["Deviation in Total Alacarte Units"]):
        if abs(row["Deviation in Total Alacarte Units"]) > 0.0005 * row["Total Alacarte Units (GPT)"]:
            statuses.append("FAIL")
        else:
            statuses.append("PASS")
    else:
        statuses.append("")

    if pd.notnull(row["Deviation in Number of Unique Days"]):
        if abs(row["Deviation in Number of Unique Days"]) > 0.0005 * row["Number of Unique Days (GPT)"]:
            statuses.append("FAIL")
        else:
            statuses.append("PASS")
    else:
        statuses.append("")

    return pd.Series(statuses, index=["Number of Unique Days Status (abs(0.05%))", "Total Net Sales Status (abs(0.05%))", "Total Alacarte Units Status (abs(0.05%))"])

# creating the store level summary
def store_level_summary(engine_summary_per_store, emr_summary_per_store):
    """
    Generate a summary of store-level data by merging two dataframes.

    Parameters
    ----------
    engine_summary_per_store : DataFrame
        The DataFrame containing store data of engine's reports.
    emr_summary_per_store : DataFrame
        The DataFrame containing summary data per store of EMR's report.

    Returns
    -------
    summary_store : DataFrame
        Summary of store-level data including deviations and statuses.
    """
    merged_df=pd.merge(engine_summary_per_store, emr_summary_per_store, left_on='mcd_gbal_lcat_id_nu', right_on='global_store_id', how='outer')
    summary_store = merged_df[["global_store_id", "unique_days", "total_net_sales", "total_units", "number_of_days_got_data","sum_net_Sales", "sum_alacarte_units"]]
    summary_store = summary_store.rename(
        columns={
            "global_store_id": "Global_Store_Id",  
            "total_net_sales": "Total Net Sales (GPT)",
            "unique_days": "Number of Unique Days (GPT)", 
            "total_units": "Total Alacarte Units (GPT)",
            "sum_net_Sales": "Total Net Sales (Portal)",
            "number_of_days_got_data": "Number of Unique Days (Portal)", 
            "sum_alacarte_units": "Total Alacarte Units (Portal)",
        })
    summary_store["Deviation in Number of Unique Days"] = summary_store["Number of Unique Days (GPT)"] - summary_store["Number of Unique Days (Portal)"]
    summary_store["Deviation in Total Net Sales"] = summary_store["Total Net Sales (GPT)"] - summary_store["Total Net Sales (Portal)"]
    summary_store["Deviation in Total Alacarte Units"] = summary_store["Total Alacarte Units (GPT)"] - summary_store["Total Alacarte Units (Portal)"]
    summary_store[["Number of Unique Days Status (abs(0.05%))", "Total Net Sales Status (abs(0.05%))", "Total Alacarte Units Status (abs(0.05%))"]] = summary_store.apply(update_status, axis=1)
    summary_store[['Global_Store_Id', 'Number of Unique Days (GPT)', 'Total Net Sales (GPT)', 'Total Alacarte Units (GPT)', 
    'Number of Unique Days (Portal)', 'Total Net Sales (Portal)', 'Total Alacarte Units (Portal)', 
    'Deviation in Number of Unique Days', 'Deviation in Total Net Sales', 'Deviation in Total Alacarte Units', 
    'Number of Unique Days Status (abs(0.05%))', 'Total Net Sales Status (abs(0.05%))', 'Total Alacarte Units Status (abs(0.05%))']]
    return summary_store

# save the reports to s3
def save_reports(summary_path, overall_summary, store_summary):
    """
    Saves a validation report file to S3.

    Parameters
    ----------
    summary_path : str
        The path where the report file is to be saved in S3.
    store_summary : DataFrame 
        The DataFrame containing the store level summary.
    overall_summary : DataFrame 
        The DataFrame containing the overall summary.

    Returns
    -------
        None
    """
    with pd.ExcelWriter(summary_path) as writer:
        overall_summary.to_excel(
            writer, sheet_name="PMIX_Overall_Summary", header=True, index=False
        )
        store_summary.to_excel(
            writer, sheet_name="Daily_PMIX_TotXStore", header=True, index=False
        )
    
def send_email(sender_email, receiver_email, password, cc_recipients, host, port, message_dict, attachment_paths):
    """
    Sends an email with attachments using SMTP and Gmail.

    Parameters
    ----------
    sender_email : str
        The email address of the sender.
    receiver_email : str
        The email address of the receiver.
    password : str
        The app password for the sender's email account.
    cc_recipients : list 
        A list of email addresses to be included as CC recipients.
    message_dict : dict 
        A dictionary containing the message content.
        - The dictionary should have the "subject" key for the email subject
        - Other key-value pairs for additional message details
    attachment_paths : list
        A list of paths to attachments.

    Returns
    -------
        None
    """
    subject = message_dict["subject"]

    # Create an instance of EmailMessage
    message = EmailMessage()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_email)
    message["Subject"] = subject
    if cc_recipients is not None:
        message["CC"] = cc_recipients
    
    html_content = '''
        <html>
        <body>
        '''
    
    for key, value in message_dict.items():
        if key != "subject":
            html_content += f'<p><b>{key.capitalize()}:</b> {value}</p>'
    
    html_content += '''
        </body>
        </html>
    '''
    
    # Set the HTML content as the email body
    message.add_alternative(html_content, subtype='html')
    
    # Attachments from S3
    s3 = boto3.client("s3")
    object_keys = []
    bucket_names = []
    
    for path in attachment_paths:
        if path.startswith("s3://"):
            bucket_name = path[5:].split('/', 1)[0]
            object_key = path[5:].split('/', 1)[1]
            object_keys.append(object_key)
            bucket_names.append(bucket_name)
        else:
            print(f"Error: Invalid path format for {path}")
            continue

    print(bucket_names)
    print(object_keys)
    
    for i in range(len(object_keys)):
        try:
            response = s3.get_object(Bucket=bucket_names[i], Key=object_keys[i])
            attachment_data = response["Body"].read()
    
            message.add_attachment(
                attachment_data,
                maintype="application",
                subtype="octet-stream",
                filename=object_keys[i].split("/")[-1]
            )
        except Exception as e:
            print(f"Error retrieving attachment from {bucket_names[i]}/{object_keys[i]}: {e}")
    
    # Convert the message to a string
    message_str = message.as_string()
    
    # Send the email
    with smtplib.SMTP_SSL(host, port) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email + cc_recipients, message_str)
        server.quit()
        
# main function
def generate_validation_report(env, market, emr_report_path, engine_report_path, summary_report_path, secret_name, region_name, email_port, receiver_email, cc_recipients=[]):
    """
    Generates a validation report for PMIX comparison between Lisa's and EMR's reports.
    
    Parameters
    ----------
    env : str
        The name of the environment.
    market : str
        The name of the market.
    emr_report_path : str
        The Path to the portal's report file.
    lisa_report_path : str
        The path to the engine's report file.
    summary_report_path : str
        The path to save the summary report in S3.
    sender_email : 
        The email address of the sender.
    password : 
        The app password of the sender's email.
    receiver_email : list
        A list of email addresses of the receivers.
    cc_recipients : list, optional 
        The list of email addresses to receive CC, default is [].

    """
    only_file_name = emr_report_path.split("/")[-1].split(".xlsx")[0]
    emr_overall_summary, emr_summary_per_store, engine_overall_summary, engine_summary_per_store, report_date, message, status = read_files(emr_report_path, engine_report_path)
    
    secret_string_json = key_vault(secret_name = secret_name, region_name = region_name)
    
    if status=="False":
        message["subject"] = "Error in PMIX" + " ( " + market + " ) " + "Summary Comparison " + report_date
        message["environment"] = f"{env}"
        message["market"] = f"{market}"
        message["file name"] = f"{only_file_name}"
        
        send_email(
            sender_email = secret_string_json['EMAIL_HOST_USER'],
            receiver_email = receiver_email, 
            password = secret_string_json['EMAIL_HOST_PASSWORD'],
            cc_recipients = cc_recipients,
            host = secret_string_json['EMAIL_HOST'],
            port = email_port,
            message_dict = message, 
            attachment_paths = []
        )
        return 0
    
    summary_path = summary_report_path + "PMIX_Validation_" + report_date + ".xlsx"
    report_df = [
        compare_columns("unique_stores", "#distinct_stores", engine_overall_summary, emr_overall_summary),
        compare_columns("unique_days", "#unique_days_loaded", engine_overall_summary, emr_overall_summary),
        compare_columns("total_net_sales", "Overall_Net_Sales", engine_overall_summary, emr_overall_summary),
        compare_columns("total_rows", "#total_rows", engine_overall_summary, emr_overall_summary),
        compare_columns("unique_items", "#unique_items", engine_overall_summary, emr_overall_summary),
        compare_columns("total_alacarte_units", "#total_alacarte_units", engine_overall_summary, emr_overall_summary),
        compare_columns("days_removed", "days_truncated", engine_overall_summary, emr_overall_summary),
    ]
    pmix_overall_summary = pd.DataFrame(report_df, columns=["Check", "GPT", "Portal", "Difference", "Status (abs(0.05%)"])
    print("pmix overall summary generated")

    store_summary = store_level_summary(engine_summary_per_store, emr_summary_per_store)
    print("store summary generated")
    
    print("saving reports to S3")
    save_reports(summary_path, pmix_overall_summary, store_summary)
    print(f"reports saved to S3 at: {summary_path}")
    
    overall_summary_flag = "PASS"
    store_summary_flag = "PASS"
    
    if "FAIL" in pmix_overall_summary["Status (abs(0.05%)"].values:
        overall_summary_flag = "FAIL"
    # else:
    #     overall_summary_flag = "PASS"
    print(f"Status of overall PMIX summary : {overall_summary_flag}")
    
    if (store_summary[["Number of Unique Days Status (abs(0.05%))", "Total Net Sales Status (abs(0.05%))", "Total Alacarte Units Status (abs(0.05%))"]] == "FAIL").any(axis=None):
        store_summary_flag = "FAIL"
    # else:
    #     store_summary_flag = "PASS"
    print(f"Status of store level PMIX summary : {store_summary_flag}")
    
    message_dict = {}
    message_dict["subject"] = "PMIX" + " ( " + market + " ) " + "Summary Comparison " + report_date
    message_dict["environment"] = f"{env}"
    message_dict["market"] = f"{market}"
    message_dict["file name"] = f"{only_file_name}"
    message_dict["report path"] = f"{summary_path}"
    message_dict["overall summary status"] = f"{overall_summary_flag}"
    message_dict["store summary status"] = f"{store_summary_flag}"
    
    
    send_email(
        sender_email = secret_string_json['EMAIL_HOST_USER'],
        receiver_email = receiver_email, 
        password = secret_string_json['EMAIL_HOST_PASSWORD'],
        cc_recipients = cc_recipients,
        host = secret_string_json['EMAIL_HOST'],
        port = email_port,
        message_dict = message_dict, 
        attachment_paths = [summary_path]
    )
    print("email sent successfully")
    return 0