"""
routing mechanism (hard, need discussion)
1. route to the notification class
ex: sendgrid, slack. pageduty, etc.

// maybe use the __module__ in python!

methods:
1. send_notification
2. check_if_config_is_valid
3. get notification secret from flytekit

error handling:
1. use try... except... to catch the error

"""
import importlib

import flytekit
from flytekit.loggers import logger


class BaseNotifier:
    def send_notification(self, message: str, notification_conf: dict[str, str]):
        raise NotImplementedError("send_notification function should be implemented by subclasses.")

    def get_notification_secret(self, notification_type: str) -> str:
        raise NotImplementedError("get_notification_secret function should be implemented by subclasses.")


def get_notifier(notification_type: str) -> BaseNotifier:
    try:
        # return None if notification_type is empty
        if not notification_type:
            return None

        package_name = "flytekitplugins.vscode.notification"
        module = importlib.import_module(f".{notification_type}_notification", package=package_name)
        notifier_instance = getattr(module, f"{notification_type.capitalize()}Notifier")
        return notifier_instance()
    except Exception as e:
        logger.error(f"Cannot find the notification class for {notification_type}: {str(e)}")
        return None


def get_notification_secret(notification_type: str) -> str:
    try:
        return flytekit.current_context().secrets.get(notification_type, "token")
    except Exception as e:
        logger.error(
            f"Cannot find the {notification_type} notification secret.\n\
                        Error: {e}"
        )
        return ""