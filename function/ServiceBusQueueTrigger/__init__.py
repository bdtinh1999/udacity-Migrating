import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

def main(message: func.ServiceBusMessage):
    notification_id = int(message.get_body().decode('utf-8'))
    logging.info('Processed ServiceBus message with ID: %s', notification_id)

    db_connection = None
    try:
        # Connect to the database
        connection_string = os.getenv('MyDbConnection')
        db_connection = psycopg2.connect(connection_string)

        cursor = db_connection.cursor()
        cursor.execute("SELECT message, subject FROM notification WHERE id = %s;", (notification_id,))
        result = cursor.fetchone()

        if result is None:
            logging.error('No content found for notification ID %s', notification_id)
            return

        notification_content, notification_subject = result

        cursor.execute("SELECT email FROM attendee;")
        attendee_list = cursor.fetchall()
        logging.info(f'Found {len(attendee_list)} attendees.')

        email_addresses = [attendee[0] for attendee in attendee_list]
        sent_count = send_email(email_addresses, notification_subject, notification_content)

        notification_status = f'Notified {sent_count} attendees'
        completion_time = datetime.utcnow()
        cursor.execute("UPDATE notification SET status = %s, completed_date = %s WHERE id = %s;", 
                       (notification_status, completion_time, notification_id))
        db_connection.commit()

    except Exception as error:
        logging.error('Error processing notification %s: %s', notification_id, str(error))
    finally:
        if db_connection is not None:
            db_connection.close()

def send_email(recipient_emails, subject_line, email_body):
    if not recipient_emails:
        return 0

    smtp_server = 'smtp.gmail.com'
    smtp_port = 465
    sender_email = os.getenv('SENDER_EMAIL')
    email_password = os.getenv('SENDER_PWD')

    sent_email_count = 0
    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as email_session:
            email_session.login(sender_email, email_password)

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
