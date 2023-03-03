# %% Sample Usage

# from twilio.rest import Client (done in code that imports these functions)

# # Load environment variables (done in code that imports these functions)
# env_vars = {
#     **dotenv_values(".env"),
#     # **os.environ,  # override loaded values with environment variables
# }
# # Create connection to Twilio (done in code that imports these functions)
# account_sid = env_vars.get('TWILIO_ACCOUNT_SID')
# auth_token = env_vars.get('TWILIO_AUTH_TOKEN')
# twilio_client = Client(account_sid, auth_token)

# # Send message (you'll need to also create a logger)
# recipient = '+19999999999'
# msg_body = f"""Test Send Message"""
# response = send_message(recipient=recipient, msg_body=msg_body, logger=logger, twilio_client=twilio_client)

# ### Schedule a message (min 15 minutes in advance, max 7 days in advance)
# https://www.twilio.com/docs/sms/api/message-resource#schedule-a-message-resource
# https://support.twilio.com/hc/en-us/articles/4412165297947-Message-Scheduling-FAQs-and-Limitations

# ### Send Message

def send_sms(
        recipient,
        msg_body,
        logger,
        twilio_client,
        messaging_service_sid='MG67a1f177cebd95aab636110ad817dd16',
        datetime_utc_to_send=None,
):

    from datetime import datetime
    #Custom functions
    from message_validation_functions import (
        validate_text,
        text_is_too_long
    )

    # Validate msg_body is okay to send to a user (not empty, no {variable_x} that haven't been replaced, etc.)
    validate_text(text=msg_body, logger=logger)
    text_is_too_long(text=msg_body, max_size=1600, logger=logger)

    # Instantiate response dictionary
    dict_response = {}

    logger.info("***Sending SMS using Twilio***")
    logger.info(f"recipient: {recipient}")
    logger.info(f"msg_body: {msg_body}")
    logger.info(f"messaging_service_sid: {messaging_service_sid}")

    # Send message immediately
    if datetime_utc_to_send is None:

        logger.info(f"send_time (UTC): immediately")

        response = twilio_client.messages \
            .create(
                body=msg_body,
                messaging_service_sid=messaging_service_sid,
                to='+' + str(recipient)
            )

    # Schedule message to send
    else:

        logger.info(f"send_time (UTC): {datetime_utc_to_send}")

        response = twilio_client.messages \
            .create(
                messaging_service_sid=messaging_service_sid,
                body=msg_body,
                send_at=datetime_utc_to_send,
                to='+' + str(recipient),
                schedule_type='fixed'
            )


    logger.info(f"Twilio API response status: {response.status}")
    logger.info(f"msg_id_sms: {response.sid}")
    dict_response['datetime_created'] = datetime.utcnow()   
    dict_response['twilio_api_response'] = response

    return dict_response

def cancel_scheduled_sms(
    msg_sid, # e.g., 'SM25db35c75cf6c40de4ef83c84a90de84'
    logger,
    twilio_client
):

    logger.info("***Cancel Message***")
    logger.info(f"msg_sid: {msg_sid}")

    try:
        response = twilio_client.messages(msg_sid) \
            .update(status='canceled')
        logger.info(f"API response: {response}")

    except Exception as e:
        logger.error(f"Error cancelling message: {e}")
        response = e

    return response

