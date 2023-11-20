from slack_sdk import WebClient

from flytekit.loggers import logger

from .base_notification import BaseNotifier, get_notification_secret


class SlackNotifier(BaseNotifier):
    def send_notification(self, message: str, notification_conf: dict[str, str]):
        try:
            token = get_notification_secret("slack-api")
            client = WebClient(token=token)
            client.chat_postMessage(channel=notification_conf["channel"], text=message)

            logger.info("Slack notification sent successfully!")
        except Exception as e:
            logger.error(
                f"Failed to send slack notification, please check the variable in slack_conf and the slack-api token.\n\
                    Error: {e}"
            )