# %%Sample usage

# 1. Enable an API connect by following instructions at https://developers.google.com/sheets/api/quickstart/python. You'll also want to enable Gmail API if you're going to use that (see more https://developers.google.com/gmail/api/quickstart/python)
# 2. Save the generated JSON credentials file at os.path.join(folder_credentials, 'google_workspaces_api_credential.json')
# 3. pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib gspread pandas 
# 4. It doesn't matter that os.path.join(folder_credentials, 'google_workspaces_api_token.json') doesn't exist (it'll be created automatically)

# # Set paths to credentials
# folder_credentials = r'C:\Users\trist\side_projects\experimenter\credentials'
# path_google_workspaces_api_credential = os.path.join(folder_credentials, 'google_workspaces_api_credential.json')
# path_google_workspaces_api_token = os.path.join(folder_credentials, 'google_workspaces_api_token.json') 

# # Create connection to google services (on first call, you'll be prompted to sign into Google)
# get_google_workspaces_access(path_google_workspaces_api_credential, path_google_workspaces_api_token)
# gspread_sheets_con = get_gspread_sheets_con(path_google_workspaces_api_credential, path_google_workspaces_api_token)

# # Create a df from a google sheet (you'll need to also create a logger)

# sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
# sheet_name = 'Interested'
# df = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)


# %%API Connections

def get_google_workspaces_access(path_google_workspaces_api_credential, path_google_workspaces_api_token):

    # Google Workspace API(Google Sheets, Gmail)
    # NOTE: If you want to use a different email address to log into the Google API, delete the file at path_google_workspaces_api_token so that you can redo the authentication flow
    # NOTE: If you enable a new API or set a new API scope, you'll need to delete the file at path_google_workspaces_api_token so that you can redo the authentication flow
    # NOTE: If Google isn't allowing you to log into the remote server due to security restrictions, you can Temporarily turn off login challenges for user https://support.google.com/a/answer/12077697?fl=1
    # https://developers.google.com/sheets/api/quickstart/python
    # https://developers.google.com/gmail/api/quickstart/python
    # https://developers.google.com/sheets/api

    # pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
    import os.path
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    # Set scopes for API
    # Gmail scopes at https://developers.google.com/gmail/api/auth/scopes
    # Sheets scopes at https://developers.google.com/sheets/api/guides/authorizing
    # NOTE: If you enable a new API or set a new API scope, you'll need to delete the file at path_google_workspaces_api_token so that you can redo the authentication flow
    google_api_scopes = [
        'https://www.googleapis.com/auth/spreadsheets', 
        'https://www.googleapis.com/auth/gmail.modify']

    google_credentials = None

    # NOTE: The .json file at path_google_workspaces_api_token is created automatically when the authorization flow completes for the first time or if you delete it. It stores the user's access and refresh tokens.
    if os.path.exists(path_google_workspaces_api_token):
        google_credentials = Credentials.from_authorized_user_file(
            filename=path_google_workspaces_api_token, scopes=google_api_scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not google_credentials or not google_credentials.valid:
        if google_credentials and google_credentials.expired and google_credentials.refresh_token:
            google_credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file=path_google_workspaces_api_credential, scopes=google_api_scopes)
            google_credentials = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(path_google_workspaces_api_token, 'w') as token:
            token.write(google_credentials.to_json())

    return {'google_credentials': google_credentials,
            'path_google_workspaces_api_credential': path_google_workspaces_api_credential,
            'path_google_workspaces_api_token': path_google_workspaces_api_token}


def get_gmail_con(path_google_workspaces_api_credential, path_google_workspaces_api_token):

    from googleapiclient.discovery import build

    try:
        dict_credentials = get_google_workspaces_access(
            path_google_workspaces_api_credential, path_google_workspaces_api_token)

        return build(
            serviceName='gmail',
            version='v1',
            credentials=dict_credentials['google_credentials'])

    except:
        # If first time failed, retry after 1 minute
        time.sleep(60)

        dict_credentials = get_google_workspaces_access(
            path_google_workspaces_api_credential, path_google_workspaces_api_token)

        return build(
            serviceName='gmail',
            version='v1',
            credentials=dict_credentials['google_credentials'])


def get_gspread_sheets_con(path_google_workspaces_api_credential, path_google_workspaces_api_token):

    import gspread

    try:
        dict_credentials = get_google_workspaces_access(
            path_google_workspaces_api_credential, path_google_workspaces_api_token)

        return gspread.oauth(
            credentials_filename=dict_credentials['path_google_workspaces_api_credential'],
            authorized_user_filename=dict_credentials['path_google_workspaces_api_token'])

    except:
        # If first time failed, retry after 1 minute
        time.sleep(60)

    dict_credentials = get_google_workspaces_access(
        path_google_workspaces_api_credential, path_google_workspaces_api_token)

    return gspread.oauth(
        credentials_filename=dict_credentials['path_google_workspaces_api_credential'],
        authorized_user_filename=dict_credentials['path_google_workspaces_api_token'])

# %% Google Sheets Functions

# Create dataframe from Google Sheet
def create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger):
    """#Sample call
    sheet_name = 'Survey Actions'
    df = create_df_from_google_sheet(gspread_sheets_con = gspread_sheets_con, sheet_id = kokoro_measurements_google_sheets_id, sheet_name = sheet_name)"""

    import gspread
    import pandas as pd

    # Connect to Google Sheets
    workbook = gspread_sheets_con.open_by_key(sheet_id)
    worksheet = workbook.worksheet(sheet_name)

    # Pull data
    try:
        df = pd.DataFrame(worksheet.get_all_records())
        return df
    except gspread.GSpreadException as e:
        logger.error(f'GSpreadException: {e}')
        if str(e) == "the given 'expected_headers' are not uniques":
            logger.error(
                "This typically occurs if you have multiple empty columns in your Google Sheet")
            logger.error("Delete the empty columns to solve")
            logger.error(
                "Or use get_all_records(expected_headers=expected_headers)")
            logger.error("See https://github.com/burnash/gspread/issues/1007")
    except Exception as e:
        logger.error(f'{e}')


# Write dataframe to Google Sheet
def clear_google_sheet_and_write_df(gspread_sheets_con, sheet_id, sheet_name, df):
    """#Sample call
    sheet_name = 'Survey Links'    
    clear_google_sheet_and_write_df(gspread_sheets_con = gspread_sheets_con, sheet_id = kokoro_measurements_google_sheets_id, sheet_name = sheet_name, df = combined_survey_links_df)"""

    import numpy as np

    # Connect to Google Sheets
    workbook = gspread_sheets_con.open_by_key(sheet_id)
    worksheet = workbook.worksheet(sheet_name)

    # Clear data from range
    worksheet.clear()

    # Replace nan values with None as JSON will not accept them
    df = df.replace({np.nan: None})

    # Convert dataframe to simple list and create JSON of data to add
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

# %% Gmail Functions

## Retrieve email

def retrieve_messages_using_gmail(gmail_con):

    #https://developers.google.com/gmail/api/reference/rest/v1/users.threads/list
    return gmail_con.users().threads().list(userId="me").execute()

#TODO: Update this function to use the gmail API

    # import pandas as pd
    # import numpy as np
    # from time import sleep
    # #Custom functions
    # from google_services_functions import (
    # create_df_from_google_sheet,
    # clear_google_sheet_and_write_df)

    # # Sleep (to give Twilio time to process messages)
    # logger.info(f"""Waiting 10 seconds to give Gmail time to process messages""")
    # sleep(10)

    # # Log activity
    # logger.info(f"""Retrieving messages from Gmail""")
    
    # # Retrieve all messages from Gmail
    


    # # Retrieve all messages from Twilio
    # messages = twilio_client.messages.list()

    # # Create list of all messages
    # list_messages = []

    # for message in messages:
    #     list_messages.append(
    #         [message.from_[1:], # remove + from phone number
    #         message.to[1:], # remove + from phone number
    #         message.body,
    #         message.status,
    #         message.date_created,
    #         message.date_sent,
    #         message.date_updated,
    #         message.direction,
    #         message.sid,]
    #     )

    # # Convert list of messages to dataframe
    # df_messages = pd.DataFrame(
    #     list_messages, 
    #     columns=['from', 'to', 'body', 'status', 'date_created', 'date_sent', 'date_updated', 'direction', 'msg_sid']
    # )

    # # Import user info
    # sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    # sheet_name = 'User Info'
    # df_user_info = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # # Import scheduled message datetimes
    # sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    # sheet_name = 'Scheduled Messages'
    # df_scheduled_messages = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # ## Format data

    # # Remove canceled messages
    # df_messages = df_messages[df_messages['status'] != 'canceled']

    # # Convert phone numbers to int (to merge with User Info)
    # df_messages['to'] = pd.to_numeric(df_messages['to'], errors='coerce').astype('Int64')
    # df_messages['from'] = pd.to_numeric(df_messages['from'], errors='coerce').astype('Int64')

    # # Identify user / our number
    # df_messages['our_number'] = np.where(
    #     df_messages['direction'] == 'inbound',
    #     df_messages['to'],
    #     df_messages['from'])

    # df_messages['user_number'] = np.where(
    #     df_messages['direction'] == 'inbound',
    #     df_messages['from'],
    #     df_messages['to'])

    # # Identify direction
    # df_messages['sender'] = np.where(
    #     df_messages['direction'] == 'inbound',
    #     'user',
    #     'company')

    # # Format datetimes as strings (so that we can JSON serialize them when writing to Google Sheets)
    # df_messages['date_sent'] = df_messages['date_sent'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    # df_messages['date_created'] = df_messages['date_created'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    # df_messages['date_updated'] = df_messages['date_updated'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # # Add user info
    # merge_cols = ['user_number']
    # df_messages = pd.merge(df_messages,
    #     df_user_info,
    #     left_on = merge_cols,
    #     right_on = merge_cols,
    #     how = "left")

    # # Add date_to_send for scheduled, canceled messages
    # keep_cols = ['msg_sid', 'datetime_to_send_utc', 'datetime_to_send_local']
    # merge_cols = ['msg_sid']
    # df_messages = pd.merge(df_messages,
    #     df_scheduled_messages[keep_cols],
    #     left_on = merge_cols,
    #     right_on = merge_cols,
    #     how = "left")

    # # Reorder columns
    # col_order = [
    #     'first_name', 'last_name', 'body', 'sender', 'date_sent', 'status', 'datetime_to_send_utc', 'datetime_to_send_local',
    #     'our_number', 'user_number', 'date_created', 'date_updated', 'msg_sid']
    # df_messages = df_messages[col_order]

    # # Sort by user, date_sent, date_to_send, date_created
    # df_messages = df_messages.sort_values(by=['user_number', 'date_sent', 'datetime_to_send_utc', 'date_created'])

    # ## Write scheduled messages to Google Sheets
    # sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    # sheet_name = 'User Messages'
    # clear_google_sheet_and_write_df(gspread_sheets_con, sheet_id, sheet_name, df_messages)

    # # Log activity
    # logger.info(f"""Retrieved messages from Twilio and wrote to Google Sheets""")

## Send email

def send_email_using_gmail(sender_email, recipient_email, subject, message_text_html, gmail_con):
    """Sample call
    sender_email = 'tristan@tryexperimenter.com'
    recipient_email = 'tristan@tryexperimenter.com'
    subject = "Sample Subject"
    message_text_html = "This is the first line. <br> This is the second line."
    send_email_using_gmail(
            sender_email = sender_email,
            recipient_email = recipient_email,
            subject = subject,
            message_text_html = message_text_html,
            gmail_con = gmail_con)    
    """

    from email.mime.text import MIMEText
    import base64

    # Set settings
    message = MIMEText(message_text_html, 'html')
    message['To'] = recipient_email
    message['From'] = sender_email
    message['Subject'] = subject

    encoded_message = base64.urlsafe_b64encode(message.as_bytes()) \
        .decode()

    create_message = {
        'raw': encoded_message
    }

    # Send message
    send_message = (gmail_con.users().messages().send
                    (userId="me", body=create_message).execute())

    return send_message
