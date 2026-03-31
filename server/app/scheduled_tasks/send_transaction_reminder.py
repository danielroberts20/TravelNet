from notifications import send_notification


if __name__ == "__main__":
    send_notification(
        title="Upload Transactions 💰",
        body="Don't forget to upload your transactions for this month!"
    )