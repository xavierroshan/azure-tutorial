import azure.functions as func
import logging
import json
import sqlite3
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import os

app = func.FunctionApp()

@app.queue_trigger(arg_name="msg", queue_name="order-queue", connection="AzureWebJobsStorage")
def order_processor(msg: func.QueueMessage):
    """
    Processes orders from a queue, updates a database, and sends a confirmation email.
    """
    try:
        # Decode message body
        order_data = json.loads(msg.get_body().decode('utf-8'))
        logging.info(f"Processing order: {order_data}")

        # Validate order data
        required_fields = ['order_id', 'customer_email', 'amount']
        if not all(field in order_data for field in required_fields):
            raise ValueError("Missing required order fields")

        # Update database (using SQLite for simplicity)
        conn = sqlite3.connect('orders.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO orders (order_id, customer_email, amount, status)
            VALUES (?, ?, ?, ?)
        ''', (order_data['order_id'], order_data['customer_email'], order_data['amount'], 'Processed'))
        conn.commit()
        conn.close()

        # Send confirmation email using SendGrid
        message = Mail(
            from_email='no-reply@ecommerce.com',
            to_emails=order_data['customer_email'],
            subject='Order Confirmation',
            html_content=f'Thank you for your order #{order_data["order_id"]}! Total: ${order_data["amount"]}')
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
        logging.info(f"Sent confirmation email for order {order_data['order_id']}")

    except json.JSONDecodeError:
        logging.error("Invalid JSON in message body")
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
    except Exception as e:
        logging.error(f"Error processing order: {str(e)}")
        raise  # Re-raise to trigger retry