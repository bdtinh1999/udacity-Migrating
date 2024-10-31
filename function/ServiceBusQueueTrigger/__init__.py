import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

def main(message: func.ServiceBusMessage):
    # Decode the message body to get the notification ID
    notification_id = int(message.get_body().decode('utf-8'))
    logging.info('Processed ServiceBus message with ID: %s', notification_id)

    db_connection = None
    try:
        # Establish a connection to the database
        connection_string = os.getenv('MyDbConnection')
        db_connection = psycopg2.connect(connection_string)

        # Fetch the notification message and subject from the database
        cursor = db_connection.cursor()
        cursor.execute("SELECT message, subject FROM notification WHERE id = %s;", (notification_id,))
        notification_content, notification_subject = cursor.fetchone()

        # Retrieve the email addresses of the attendees
        cursor.execute("SELECT email FROM attendee;")
        attendee_list = cursor.fetchall()
        logging.info(f'Found {len(attendee_list)} attendees.')

        # Send email to each attendee
        email_addresses = [attendee[0] for attendee in attendee_list]
        sent_count = send_email(email_addresses, notification_subject, notification_content)

        # Update the notification table with the notification status
        notification_status = f'Notified {sent_count} attendees'
        completion_time = datetime.utcnow()
        cursor.execute("UPDATE notification SET status = %s, completed_date = %s WHERE id = %s;", 
                       (notification_status, completion_time, notification_id))
        db_connection.commit()

    except Exception as error:
        logging.error('Error processing notification %s: %s', notification_id, error)
    finally:
        if db_connection is not None:
            db_connection.close()

def send_email(recipient_emails, subject_line, email_body):
    # If there are no recipients, return 0
    if not recipient_emails:
        return 0

    smtp_server = 'smtp.gmail.com'
    smtp_port = 465
    sender_email = os.getenv('SENDER_EMAIL')
    email_password = os.getenv('SENDER_PWD')

    sent_email_count = 0
    try:
        # Create a secure SMTP session
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as email_session:
            # Log in to the email server
            email_session.login(sender_email, email_password)

            # Iterate through the recipient emails and send the email
            for recipient in recipient_emails:
                try:
                    msg = MIMEText(email_body)
                    msg['Subject'] = subject_line
                    msg['From'] = sender_email
                    msg['To'] = recipient
                    email_session.sendmail(sender_email, recipient, msg.as_string())
                    sent_email_count += 1
                    logging.info(f'Email sent to {recipient}')
                except Exception as send_error:
                    logging.error(f'Failed to send email to {recipient}: {send_error}')
    except Exception as smtp_error:
        logging.error('SMTP session failed: %s', smtp_error)

    return sent_email_count
