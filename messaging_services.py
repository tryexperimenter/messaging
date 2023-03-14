# %% Set Functions to Run

code_to_run = "instantiate code_to_run"
from datetime import datetime
date_starting_monday = datetime(2023,3,13) #used for scheduling messages, needs to be the monday of the week you want to schedule messages for
# code_to_run = "send_messages"
# code_to_run = "schedule_messages"
# code_to_run = "cancel_scheduled_messages"
# code_to_run = "retrieve_messages"
# code_to_run = 'schedule_mini_experiment_messages'

# %% Set Up

# %%%Set folder/file paths
folder_main = r'C:\Users\trist\experimenter\messaging' 
folder_custom_functions = os.path.join(folder_main,'functions')
folder_credentials = os.path.join(folder_main,'credentials')
folder_logs = os.path.join(folder_main,'logs')
path_google_workspaces_api_credential = os.path.join(folder_credentials, 'google_workspaces_api_credential.json')
path_google_workspaces_api_token = os.path.join(folder_credentials, 'google_workspaces_api_token.json') 

# %%%Import standard modules
import os, sys
import pandas as pd
from datetime import datetime
from dotenv import dotenv_values
from time import sleep
from twilio.rest import Client
from sendgrid import SendGridAPIClient


# %%%Import custom functions
sys.path.insert(1, folder_custom_functions) # caution: path[0] is reserved for script path (or '' in REPL)
from google_services_functions import (
    get_google_workspaces_access,
    get_gspread_sheets_con,
    create_df_from_google_sheet,
    get_gmail_con,
    retrieve_messages_using_gmail)
from messaging_functions import (
    assign_dates_to_send,
    send_messages,
    schedule_messages,
    cancel_scheduled_messages,
    retrieve_messages)
from logging_functions import (
    get_logger,
    close_logger)
from short_io_functions import (
    generate_short_url
    )

# %%% Enable autoreload
# If you change code in custom functions, you'll need to reload them. Otherwise, you'll have to close out of Python and reimport everything.
# https://switowski.com/blog/ipython-autoreload/
%load_ext autoreload
%autoreload 2

# %%% Open logger
if 'logger' not in locals():
    logger = get_logger(log_file_path=os.path.join(folder_logs,"log - " +
                        datetime.now().strftime("%Y-%m-%d %H.%M.%S") + ".log"))

# %%% Load environment variables
env_vars = {
    **dotenv_values(".env"),
    # **os.environ,  # override loaded values with environment variables
}

# %%% Create connection to Twilio
twilio_client = Client(
    env_vars.get('TWILIO_ACCOUNT_SID'), 
    env_vars.get('TWILIO_AUTH_TOKEN'))

# %%% Create connection to SendGrid
# Note that you'll need to enable all of the actions you want to take in SendGrid's UI when you create the API key (e.g., scheduledule sends)
sendgrid_client = SendGridAPIClient(env_vars.get('SENDGRID_API_KEY'))

# %%% Store short.io API key
short_io_api_key = env_vars.get('SHORT_IO_API_KEY')

# %%% Create connection to Google Services
get_google_workspaces_access(path_google_workspaces_api_credential, path_google_workspaces_api_token)
gspread_sheets_con = get_gspread_sheets_con(path_google_workspaces_api_credential, path_google_workspaces_api_token)
gmail_con = get_gmail_con(path_google_workspaces_api_credential, path_google_workspaces_api_token)


# %% Retrieve Messages (from Gmail)

if code_to_run == "retrieve_messages_from_gmail":
    
        # Retrieve messages
        logger.info(f"""Calling retrieve_messages_using_gmail()""")
        response = retrieve_messages_using_gmail(gmail_con=gmail_con)

# %% Send Message(s) (from Google Sheets)

if code_to_run == "send_messages":

    # Load messages to send
    logger.info(f"""Loading messages to send""")
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'Messages to Send'
    df_messages = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # Only send messages with "yes" in the "send" column
    df_messages = df_messages[df_messages['send'].str.lower() == 'yes']

    # Send messages
    logger.info(f"""Calling send_messages()""")
    send_messages(df_messages=df_messages, logger=logger, sendgrid_client=sendgrid_client, twilio_client=twilio_client)

    code_to_run = "retrieve_messages"

# %% Schedule Messages (from Google Sheets)

if code_to_run == "schedule_messages":

    # Load messages to schedule
    logger.info(f"""Loading messages to schedule""")
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'Messages to Schedule'
    df_messages = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # Only send messages with "yes" in the "send" column
    df_messages = df_messages[df_messages['send'].str.lower() == 'yes']

    # Assign dates to send
    logger.info(f"""Calling assign_dates_to_send()""")
    df_messages = assign_dates_to_send(df_messages=df_messages, date_starting_monday = date_starting_monday, logger=logger)

    # Calculate local datetime to send
    df_messages['datetime_no_tz'] = pd.to_datetime(dict(year=df_messages.year, month=df_messages.month, day=df_messages.day, hour=df_messages.hour, minute=df_messages.minute))

    df_messages['datetime_local_to_send'] = df_messages.apply(lambda row: row['datetime_no_tz'].tz_localize(row['time_zone']), axis=1)

    df_messages.drop(columns=['datetime_no_tz', 'year', 'month', 'day', 'hour', 'minute'], inplace=True)

    # Schedule messages
    logger.info(f"""Calling schedule_messages()""")
    response = schedule_messages(df_messages=df_messages, gspread_sheets_con=gspread_sheets_con, sendgrid_client=sendgrid_client, twilio_client=twilio_client, logger=logger,)

    code_to_run = "retrieve_messages"


# %% Cancel Scheduled Messages (from Google Sheets)

if code_to_run == 'cancel_scheduled_messages':

    # Load messages to cancel
    logger.info(f"""Loading messages to cancel""")
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'Messages to Cancel'
    df_messages = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # Cancel messages
    logger.info(f"""Calling cancel_scheduled_messages()""")
    cancel_scheduled_messages(df_messages=df_messages, twilio_client=twilio_client, sendgrid_client=sendgrid_client, logger=logger)

    code_to_run = "retrieve_messages"

# %% Schedule Mini Experiment Messages

if code_to_run == 'schedule_mini_experiment_messages':

    import pandas as pd

    ## Load Mini Experimenters
    logger.info(f"""Loading Mini Experimenters""")
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'Mini Experimenters'
    df_mini_experimenters = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)
    # Restrict to "active" experimenters
    df_mini_experimenters = df_mini_experimenters[df_mini_experimenters['status'] == 'active']
    # Keep only the columns we need
    keep_cols = ['first_name', 'user_phone', 'user_email', 'channel', 'url_stub_experimenter_log', 'experiment_week', 'time_zone']
    df_mini_experimenters = df_mini_experimenters[keep_cols]

    ## Load Mini Experiment Messages
    logger.info(f"""Loading Mini Experiment Messages""")
    sheet_id = '1ioITvfbx3HmA1HhdEhHRywVSc7T3xRPmtWobfv4Rk5M'
    sheet_name = 'Mini Experiment Messages'
    df_mini_experiment_messages = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)
    # Restrict to "active" messages
    df_mini_experiment_messages = df_mini_experiment_messages[df_mini_experiment_messages['status'] == 'active']
    # Keep only the columns we need
    keep_cols = ['experiment_week', 'msg_body_sms', 'subject_email', 'msg_body_email','days_of_week', 'hour', 'minute', 'url_view_observations']
    df_mini_experiment_messages = df_mini_experiment_messages[keep_cols]

    ## Generate reflection URL 
    logger.info(f"""Generating reflection URL""")

    # Long reflection URL
    df_mini_experimenters['url_long_record_observations'] = df_mini_experimenters.apply(
        lambda row: f"https://tryexperimenter.com/observations?first_name={row['first_name']}&user_phone={row['user_phone']}&user_email={row['user_email']}&experiments=wk{row['experiment_week']}&url_stub_experimenter_log={row['url_stub_experimenter_log']}", 
        axis=1)

    # Reflection URL
    df_mini_experimenters['url_record_observations'] = df_mini_experimenters.apply(
        lambda row: generate_short_url(
            long_url = row['url_long_record_observations'], 
            short_io_api_key=short_io_api_key),
        axis=1)

    # Long reflection URL (prior week)
    df_mini_experimenters['url_long_record_observations_prior_week'] = df_mini_experimenters.apply(
        lambda row: f"https://tryexperimenter.com/observations?first_name={row['first_name']}&user_phone={row['user_phone']}&user_email={row['user_email']}&experiments=wk{row['experiment_week']-1}&url_stub_experimenter_log={row['url_stub_experimenter_log']}", 
        axis=1)

    # Reflection URL (prior week)
    df_mini_experimenters['url_record_observations_prior_week'] = df_mini_experimenters.apply(
        lambda row: generate_short_url(
            long_url = row['url_long_record_observations_prior_week'], 
            short_io_api_key=short_io_api_key),
        axis=1)

    # Experimenter Log URL
    df_mini_experimenters['url_experimenter_log'] = df_mini_experimenters.apply(
        lambda row: f"tryexperimenter.com/{row['url_stub_experimenter_log']}",
        axis=1)

    ## Add messages to send to each mini experimenter in the coming week
    merge_cols = ['experiment_week']
    df_messages = pd.merge(df_mini_experimenters,
        df_mini_experiment_messages,
        left_on = merge_cols,
        right_on = merge_cols,
        how = "left")

    # Remove any rows for experimenters where there are no messages to send
    df_messages = df_messages[df_messages['msg_body_sms'].notnull() & df_messages['msg_body_email'].notnull()]


    ## Assign datetimes to send

    # Assign dates
    logger.info(f"""Calling assign_dates_to_send()""")
    df_messages = assign_dates_to_send(df_messages=df_messages, date_starting_monday = date_starting_monday, logger=logger)

    # Calculate local datetime to send
    logger.info(f"""Calculate local datetime to send""")
    df_messages['datetime_no_tz'] = pd.to_datetime(dict(year=df_messages.year, month=df_messages.month, day=df_messages.day, hour=df_messages.hour, minute=df_messages.minute))

    df_messages['datetime_local_to_send'] = df_messages.apply(lambda row: row['datetime_no_tz'].tz_localize(row['time_zone']), axis=1)

    df_messages.drop(columns=['datetime_no_tz', 'year', 'month', 'day', 'hour', 'minute'], inplace=True)

    ## Customize subject / msg_body for each message
    # i.e., fill in variables in msg_body_sms, subject_email, msg_body_email
    logger.info(f"""Filling in variables in msg_body_sms, subject_email, msg_body_email""")
    for index, (
        msg_body_sms, 
        subject_email, 
        msg_body_email, 
        experiment_week,
        first_name, 
        url_record_observations, 
        url_record_observations_prior_week,
        url_view_observations, 
        url_experimenter_log,
        ) in \
    enumerate(zip(
        df_messages['msg_body_sms'],
        df_messages['subject_email'],
        df_messages['msg_body_email'],
        df_messages['experiment_week'],
        df_messages['first_name'],
        df_messages['url_record_observations'],
        df_messages['url_record_observations_prior_week'],
        df_messages['url_view_observations'],
        df_messages['url_experimenter_log'],
    )):

        # Replace '' with 'ERROR!!!' for empty variables so that we can catch variable replacements that will just be empty strings (e.g., "Hey {first_name}!" gets turned into  "Hey !" if first_name is ''))})
        for var in [
            'experiment_week',
            'first_name',
            'url_record_observations',
            'url_record_observations_prior_week',
            'url_view_observations',
            'url_experimenter_log',
            ]:
            exec(f"""if {var} == '': {var} = 'ERROR!!!'""")

        # Fill in variables
        for str in ['msg_body_sms', 'subject_email', 'msg_body_email']:

            # Replace any {variables} in the string with the value of local variables of the same name (e.g., {first_name} gets replaced with the value of local variable first_name)
            exec(f"{str} = {str}.format(**locals())")

            # Raise an error if there are any "ERROR!!!" in the string (e.g., if there are any variables we tried to replace with empty strings)
            exec(f"""if "ERROR!!!" in {str}: raise ValueError("Row {index - 1}, name: {first_name} of df_messages has 1+ ERROR!!! in {str}, indicating that a replacement variable was empty.")""")

            # Assign string with filled in {variables} back to df_messages so we can access it later
            exec(f"df_messages['{str}'][index] = {str}")                

    ## Schedule messages
    logger.info(f"""Calling schedule_messages()""")
    schedule_messages(df_messages=df_messages, gspread_sheets_con=gspread_sheets_con, twilio_client=twilio_client, sendgrid_client=sendgrid_client, logger=logger)

    code_to_run = "retrieve_messages"

# %% Retrieve Messages from Twilio (and store in Google Sheets)

if code_to_run == 'retrieve_messages':

    # Retrieve messages from Twilio
    logger.info(f"""Calling retrieve_messages()""")
    retrieve_messages(gspread_sheets_con=gspread_sheets_con, logger=logger, twilio_client=twilio_client)


# %%% Close logger and delete logger object so we create a new one next time we run this script
close_logger(logger)
del(logger)


