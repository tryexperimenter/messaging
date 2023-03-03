from sendgrid_functions import (
    send_email,
    cancel_scheduled_emails_for_batch_id)

from twilio_sms_functions import (
    send_sms,
    cancel_scheduled_sms,)

# %% Helper Functions

def assign_dates_to_send(df_messages, date_starting_monday, logger):

    import pandas as pd
    #Custom functions
    from datetime_functions import (
        upcoming_7_dates_starting_monday
    )
    
    ## Create one row per day in days_of_week to send
    df = df_messages.copy()    
    df = df.assign(day_of_week=df.days_of_week.str.split(',')).explode('day_of_week')
    df = df.assign(day_of_week=df.day_of_week.str.strip()) #remove any spaces that were present in between commas
    df = df.drop(columns=['days_of_week'])
    df_messages = df.copy()

    # Get upcoming dates (use reference_date if set (e.g., when we're running code on Monday and function will return an error))
    logger.info(f"""Calling upcoming_7_dates_starting_monday()""")
    df_upcoming_dates = upcoming_7_dates_starting_monday(date_starting_monday=date_starting_monday)

    # Add dates to send each message
    merge_cols = ['day_of_week']
    df_messages = pd.merge(df_messages,
        df_upcoming_dates,
        left_on = merge_cols,
        right_on = merge_cols,
        how = "left")
    df_messages.drop(columns=['day_of_week'], inplace=True)

    return df_messages

# %% Main Functions

def send_smses(
    df_messages,
    twilio_client,
    logger,
):

    # Send SMSes if there are any to send
    if len(df_messages) >= 0:

        logger.info(f"""Looping through SMSes to send""")

        for (user_phone, msg_body_sms) in \
        zip(df_messages['user_phone'], df_messages['msg_body_sms']):

            logger.info(f"""Calling send_sms()""")
            response = send_sms(
                recipient=user_phone, 
                msg_body=msg_body_sms, 
                logger=logger, 
                twilio_client=twilio_client)
    
    else:
        logger.info(f"""No SMSes to send""")


def send_emails(
    df_messages,
    sendgrid_client,
    logger,
):

    # Send emails if there are any to send
    if len(df_messages) >= 0:

        logger.info(f"""Looping through emails to send""")

        for (user_email, subject_email, msg_body_email) in \
        zip(df_messages['user_email'], df_messages['subject_email'], df_messages['msg_body_email']):

            logger.info(f"""Calling send_email()""")
            dict_response = send_email(
                from_email = 'experiments@tryexperimenter.com', 
                from_display_name = 'Experimenter',
                to_email = user_email, 
                subject = subject_email, 
                message_text_html = msg_body_email, 
                add_unsubscribe_link = True,
                sendgrid_client = sendgrid_client, 
                logger = logger
                )

    else:
        logger.info(f"""No emails to send""")
        

def schedule_smses(
    df_messages,
    twilio_client,
    logger,
):

    import pandas as pd
    from pytz import timezone

    # Setup
    df = df_messages

    # Schedule SMSes if there are any to schedule
    if len(df) > 0:

        # Create list to record scheduled messages
        list_scheduled_messages = []

        # Loop through SMSes to schedule
        logger.info(f"""Looping through SMSes to schedule""")

        for (user_phone, msg_body_sms, datetime_local_to_send) in \
        zip(df['user_phone'], df['msg_body_sms'], df['datetime_local_to_send']):

            logger.info(f"""Calling send_sms()""")

            # Set datetime UTC to send
            datetime_utc_to_send = datetime_local_to_send.astimezone(timezone('UTC'))

            dict_response = send_sms(
                datetime_utc_to_send=datetime_utc_to_send,
                recipient=user_phone,
                msg_body=msg_body_sms,
                logger=logger,
                twilio_client=twilio_client
            )

            # Record scheduled SMS
            list_scheduled_messages.append(
                [dict_response['twilio_api_response'].sid,
                dict_response['twilio_api_response'].to[1:], # remove + from phone number
                dict_response['twilio_api_response'].body,
                dict_response['twilio_api_response'].status,
                dict_response['datetime_created'],
                datetime_utc_to_send,
                datetime_local_to_send]
            )
    
    else:
        logger.info(f"""No SMSes to schedule""")
        return None

    # Convert list of scheduled SMSes to dataframe
    df_scheduled_messages = pd.DataFrame(
        list_scheduled_messages, 
        columns=['msg_id_sms', 'user_number', 'msg_body_sms', 'status', 'datetime_created', 'datetime_utc_to_send', 'datetime_local_to_send']
    )

    return df_scheduled_messages

def schedule_emails(
    df_messages,
    sendgrid_client,
    logger,
):

    import pandas as pd
    from pytz import timezone

    # Setup
    df = df_messages

    # Schedule emails if there are any to schedule
    if len(df) > 0:

        # Create list to record scheduled messages
        list_scheduled_messages = []

        # Loop through emails to schedule
        logger.info(f"""Looping through emails to schedule""")

        for (user_email, subject_email, msg_body_email, datetime_local_to_send) in \
        zip(df['user_email'], df['subject_email'], df['msg_body_email'], df['datetime_local_to_send']):

            logger.info(f"""Calling send_email() to schedule email""")

            # Set datetime UTC to send
            datetime_utc_to_send = datetime_local_to_send.astimezone(timezone('UTC'))

            # Schedule email
            dict_response = send_email(
                datetime_utc_to_send = datetime_utc_to_send,
                from_email = 'experiments@tryexperimenter.com', 
                from_display_name = 'Experimenter',
                to_email = user_email, 
                subject = subject_email, 
                message_text_html = msg_body_email, 
                add_unsubscribe_link = True,
                sendgrid_client = sendgrid_client, 
                logger = logger
                )

            # Record scheduled emails
            list_scheduled_messages.append(
                [dict_response['batch_id'],
                user_email,
                subject_email,
                msg_body_email,
                dict_response['status_code'],
                dict_response['datetime_created'],
                datetime_utc_to_send,
                datetime_local_to_send]
            )

    else:

        logger.info(f"""No emails to schedule""")

        return None

    # Convert list of scheduled emails to dataframe
    df_scheduled_messages = pd.DataFrame(
        list_scheduled_messages, 
        columns=['batch_id_email', 'user_email', 'subject_email', 'msg_body_email', 'status', 'datetime_created', 'datetime_utc_to_send', 'datetime_local_to_send']
    )

    return df_scheduled_messages    

def send_messages(
    df_messages,
    sendgrid_client,
    twilio_client,
    logger,
):
    send_smses(df_messages = df_messages[df_messages['channel'] == 'sms'], twilio_client=twilio_client, logger=logger)
    send_emails(df_messages = df_messages[df_messages['channel'] == 'email'], sendgrid_client=sendgrid_client, logger=logger)


def schedule_messages(
    df_messages,
    gspread_sheets_con,
    sendgrid_client,
    twilio_client,
    logger,
):

    import pandas as pd
    #Custom functions
    from google_services_functions import (
    create_df_from_google_sheet,
    clear_google_sheet_and_write_df)
    from dataframe_functions import (
    vertically_concatenate_dfs)

    logger.info(f"""Calling schedule_smses()""")
    df_scheduled_smses = schedule_smses(df_messages = df_messages[df_messages['channel'] == 'sms'], twilio_client=twilio_client, logger=logger)
    logger.info(f"""Calling schedule_emails()""")
    df_scheduled_emails = schedule_emails(df_messages = df_messages[df_messages['channel'] == 'email'], sendgrid_client=sendgrid_client, logger=logger)

    ## Store scheduled messages (used for knowing when scheduled messages are scheduled to be sent)
    logger.info(f"""Processing df_scheduled_emails and df_scheduled_smses""")

    # Combine scheduled SMSes and scheduled emails into one dataframe
    df_scheduled_messages = vertically_concatenate_dfs([df_scheduled_smses, df_scheduled_emails])

    # Remove timezone info from datetime_local_to_send (so that we can convert time into a string for writing to Google Sheets)
    df_scheduled_messages['datetime_local_to_send'] = [datetime_local_to_send.replace(tzinfo=None) for datetime_local_to_send in df_scheduled_messages['datetime_local_to_send']] 

    # Format datetimes as strings (so that we can JSON serialize them when writing to Google Sheets)
    df_scheduled_messages['datetime_created'] = df_scheduled_messages['datetime_created'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_scheduled_messages['datetime_utc_to_send'] = df_scheduled_messages['datetime_utc_to_send'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_scheduled_messages['datetime_local_to_send'] = df_scheduled_messages['datetime_local_to_send'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    logger.info(f"""Combining newly scheduled messages and previously scheduled messages""")

    # Load previously scheduled messages
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'Scheduled Messages'
    df_previously_scheduled_messages = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # Combine previously scheduled and newly scheduled messages; keep record for most recent msg_sid (in case we rescheduled a message)
    df_scheduled_messages = vertically_concatenate_dfs([df_previously_scheduled_messages, df_scheduled_messages])
    df_scheduled_messages = df_scheduled_messages.sort_values(by=['msg_id_sms', 'batch_id_email', 'datetime_created'], ascending=[True, True, False])
    df_scheduled_messages.drop_duplicates(subset=['msg_id_sms', 'batch_id_email'], keep='first', inplace=True)

    # Sort by date to send
    df_scheduled_messages = df_scheduled_messages.sort_values(by=['datetime_utc_to_send'], ascending=[True])

    # Write scheduled messages to Google Sheets
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'Scheduled Messages'
    clear_google_sheet_and_write_df(gspread_sheets_con, sheet_id, sheet_name, df_scheduled_messages)

    logger.info(f"""Finished writing scheduled messages to Google Sheets""")


def cancel_scheduled_messages(
    df_messages,
    twilio_client,
    sendgrid_client,
    logger
):

    # Cancel scheduled SMSes for any msg_id_sms in df_messages
    for msg_id_sms in df_messages['msg_id_sms'][df_messages['msg_id_sms'].str.len()>0]:

        cancel_scheduled_sms(msg_sid=msg_id_sms, logger=logger, twilio_client=twilio_client)

    # Cancel scheduled emails for any batch_id_email in df_messages
    for batch_id_email in df_messages['batch_id_email'][df_messages['batch_id_email'].str.len()>0]:

        cancel_scheduled_emails_for_batch_id(batch_id=batch_id_email, sendgrid_client=sendgrid_client, logger=logger)  


def retrieve_messages(
    gspread_sheets_con,
    logger,
    twilio_client
):

    import pandas as pd
    import numpy as np
    from time import sleep
    #Custom functions
    from google_services_functions import (
    create_df_from_google_sheet,
    clear_google_sheet_and_write_df)

    # Sleep (to give Twilio time to process messages)
    logger.info(f"""Waiting 10 seconds to give Twilio time to process messages""")
    sleep(10)

    # Log activity
    logger.info(f"""Retrieving messages from Twilio""")

    # Retrieve all messages from Twilio
    # https://www.twilio.com/docs/sms/tutorials/how-to-retrieve-and-modify-message-history-python
    response = twilio_client.messages.list()

    # Create list of all messages
    list_messages = []

    for message in response:
        list_messages.append(
            [message.from_[1:], # remove + from phone number
            message.to[1:], # remove + from phone number
            message.body,
            message.status,
            message.date_created,
            message.date_sent,
            message.date_updated,
            message.direction,
            message.sid,]
        )

    # Convert list of messages to dataframe
    df_messages = pd.DataFrame(
        list_messages, 
        columns=['from', 'to', 'body', 'status', 'date_created', 'date_sent', 'date_updated', 'direction', 'msg_id_sms']
    )

    # Import user info
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'User Info'
    df_user_info = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # Import scheduled message datetimes
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'Scheduled Messages'
    df_scheduled_messages = create_df_from_google_sheet(gspread_sheets_con, sheet_id, sheet_name, logger)

    # Keep just SMS specific column / rows
    df_scheduled_messages = df_scheduled_messages[['msg_id_sms','user_number','msg_body_sms','status','datetime_created','datetime_utc_to_send','datetime_local_to_send']]
    df_scheduled_messages = df_scheduled_messages[df_scheduled_messages['msg_id_sms'].str.len()>0]

    ## Format data

    # Remove canceled messages
    df_messages = df_messages[df_messages['status'] != 'canceled']

    # Convert phone numbers to int (to merge with User Info)
    df_messages['to'] = pd.to_numeric(df_messages['to'], errors='coerce').astype('Int64')
    df_messages['from'] = pd.to_numeric(df_messages['from'], errors='coerce').astype('Int64')

    # Identify user / our number
    df_messages['our_number'] = np.where(
        df_messages['direction'] == 'inbound',
        df_messages['to'],
        df_messages['from'])

    df_messages['user_number'] = np.where(
        df_messages['direction'] == 'inbound',
        df_messages['from'],
        df_messages['to'])

    # Identify direction
    df_messages['sender'] = np.where(
        df_messages['direction'] == 'inbound',
        'user',
        'company')

    # Format datetimes as strings (so that we can JSON serialize them when writing to Google Sheets)
    df_messages['date_sent'] = df_messages['date_sent'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_messages['date_created'] = df_messages['date_created'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_messages['date_updated'] = df_messages['date_updated'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Add user info
    merge_cols = ['user_number']
    df_messages = pd.merge(df_messages,
        df_user_info,
        left_on = merge_cols,
        right_on = merge_cols,
        how = "left")

    # Add date_to_send for scheduled, canceled messages
    keep_cols = ['msg_id_sms', 'datetime_utc_to_send', 'datetime_local_to_send']
    merge_cols = ['msg_id_sms']
    df_messages = pd.merge(df_messages,
        df_scheduled_messages[keep_cols],
        left_on = merge_cols,
        right_on = merge_cols,
        how = "left")

    # Reorder columns
    col_order = [
        'first_name', 'last_name', 'body', 'sender', 'date_sent', 'status', 'datetime_utc_to_send', 'datetime_local_to_send',
        'our_number', 'user_number', 'date_created', 'date_updated', 'msg_id_sms']
    df_messages = df_messages[col_order]

    # Sort by user, date_sent, date_to_send, date_created
    df_messages = df_messages.sort_values(by=['user_number', 'date_sent', 'datetime_utc_to_send', 'date_created'])

    ## Write scheduled messages to Google Sheets
    sheet_id = '1oCHvyb7HWgOuLwJCR76fgMc-3FgH_sFNTQ7XWC1YglY'
    sheet_name = 'User Messages'
    clear_google_sheet_and_write_df(gspread_sheets_con, sheet_id, sheet_name, df_messages)

    # Log activity
    logger.info(f"""Retrieved messages from Twilio and wrote to Google Sheets""")