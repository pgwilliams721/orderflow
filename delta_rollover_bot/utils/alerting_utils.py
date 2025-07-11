import aiohttp
import asyncio
import logging
from typing import Optional, Dict, Any

# from telegram import Bot # Optional: For Telegram alerts, if python-telegram-bot is used

logger = logging.getLogger(__name__)

class Alerter:
    """
    Handles sending alerts via webhook and optionally Telegram.
    """
    def __init__(self, config: Optional[Any] = None): # config can be the main Config object
        self.webhook_url: Optional[str] = None
        self.discord_webhook_url: Optional[str] = None
        self.telegram_bot_token: Optional[str] = None
        self.telegram_chat_id: Optional[str] = None
        # self.telegram_bot: Optional[Bot] = None # For python-telegram-bot

        if config:
            self.webhook_url = config.get("alerting.webhook_url")
            self.discord_webhook_url = config.get("alerting.discord_webhook_url")
            self.telegram_bot_token = config.get("alerting.telegram_bot_token")
            self.telegram_chat_id = config.get("alerting.telegram_chat_id")

            # if self.telegram_bot_token:
            #     try:
            #         self.telegram_bot = Bot(token=self.telegram_bot_token)
            #         logger.info("Telegram bot initialized for alerts.")
            #     except Exception as e:
            #         logger.error(f"Failed to initialize Telegram bot: {e}. Telegram alerts will be disabled.")
            #         self.telegram_bot = None
            # else:
            #     logger.info("Telegram bot token not configured. Telegram alerts disabled.")
        else:
            logger.info("Alerter initialized without config. Alerts will be disabled unless configured later.")

        if self.webhook_url and (self.webhook_url == "YOUR_WEBHOOK_URL" or not self.webhook_url.startswith("http")):
            logger.warning(f"Generic Webhook URL ('{self.webhook_url}') seems invalid or is a placeholder. Generic webhook alerts might fail.")
            # self.webhook_url = None # Optionally disable if clearly a placeholder

        if self.discord_webhook_url and (self.discord_webhook_url == "YOUR_DISCORD_WEBHOOK_URL" or not self.discord_webhook_url.startswith("http")):
            logger.warning(f"Discord Webhook URL ('{self.discord_webhook_url}') seems invalid or is a placeholder. Discord alerts might fail.")
            # self.discord_webhook_url = None

        if self.telegram_bot_token and self.telegram_bot_token == "YOUR_TELEGRAM_BOT_TOKEN":
            logger.warning("Telegram bot token is a placeholder. Telegram alerts will be disabled.")
            # self.telegram_bot_token = None
            # self.telegram_bot = None

        if self.telegram_chat_id and self.telegram_chat_id == "YOUR_TELEGRAM_CHAT_ID":
            logger.warning("Telegram chat ID is a placeholder. Telegram alerts might fail.")
            # self.telegram_chat_id = None


    async def send_webhook_alert(self, message: str, payload: Optional[Dict[str, Any]] = None):
        """
        Sends an alert message to the configured webhook URL.
        'payload' allows for more structured data (e.g., Slack blocks). If None, sends simple text.
        """
        if not self.webhook_url or self.webhook_url == "YOUR_WEBHOOK_URL":
            logger.debug(f"Webhook URL not configured or is placeholder. Skipping webhook alert: {message[:50]}...")
            return

        try:
            async with aiohttp.ClientSession() as session:
                # Default payload for simple text message (common for Discord, basic Slack)
                data_to_send = {'content': message}
                if payload: # If a specific payload structure is provided, use that
                    data_to_send = payload

                # For Slack, a common simple payload is {'text': message}
                # Or for more complex messages, use Slack's Block Kit format in `payload`.
                # Example for Slack Block Kit (if `payload` is structured accordingly):
                # data_to_send = payload if payload else {"text": message}


                async with session.post(self.webhook_url, json=data_to_send) as response:
                    if response.status >= 200 and response.status < 300:
                        logger.info(f"Webhook alert sent successfully: {message[:50]}...")
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to send webhook alert (status {response.status}): {message[:50]}... Error: {error_text[:200]}")
        except aiohttp.ClientError as e:
            logger.error(f"Error sending webhook alert to {self.webhook_url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during webhook alert: {e}", exc_info=True)

    async def send_telegram_alert(self, message: str):
        """
        Sends an alert message via Telegram (placeholder).
        Requires `python-telegram-bot` library and bot configuration.
        """
        if not self.telegram_bot_token or not self.telegram_chat_id or \
           self.telegram_bot_token == "YOUR_TELEGRAM_BOT_TOKEN" or \
           self.telegram_chat_id == "YOUR_TELEGRAM_CHAT_ID":
            logger.debug(f"Telegram not configured or using placeholders. Skipping Telegram alert: {message[:50]}...")
            return

        # Implementation using python-telegram-bot (if library was installed and imported)
        # if not self.telegram_bot:
        #     logger.error("Telegram bot not initialized. Cannot send Telegram alert.")
        #     return
        # try:
        #     await self.telegram_bot.send_message(chat_id=self.telegram_chat_id, text=message)
        #     logger.info(f"Telegram alert sent successfully: {message[:50]}...")
        # except Exception as e:
        #     logger.error(f"Failed to send Telegram alert: {e}")
        logger.warning(f"Telegram alert functionality is a placeholder. Message: {message[:100]}")


    async def send_alert(self, message: str, payload: Optional[Dict[str, Any]] = None):
        """
        Sends the alert via all configured channels.
        """
        logger.info(f"ALERT: {message}") # Log the alert locally too

        # Create tasks for each alert type to send them concurrently
        alert_tasks = []
        # Send to generic webhook
        if self.webhook_url and self.webhook_url != "YOUR_WEBHOOK_URL" and not self.webhook_url.startswith("YOUR_"):
            alert_tasks.append(self.send_webhook_alert(message, payload)) # Generic webhook might use the payload as is

        # Send to Discord webhook (Discord expects {'content': 'message'})
        if self.discord_webhook_url and self.discord_webhook_url != "YOUR_DISCORD_WEBHOOK_URL" and not self.discord_webhook_url.startswith("YOUR_"):
            # If a specific payload for Discord is needed, it should be constructed here or passed.
            # For now, send the simple text message.
            discord_payload = {'content': message}
            # If original `payload` is intended for Discord, it should be used, but `send_webhook_alert`
            # also defaults to `{'content': message}` if payload is None.
            # To avoid double 'content' nesting if payload itself is {'content': ...}, be careful.
            # If `payload` is provided and structured for something else (e.g. Slack), it might not render well in Discord.
            # Safest for a dedicated Discord hook is to ensure it gets a Discord-compatible payload.
            alert_tasks.append(self.send_webhook_alert(message, payload=discord_payload if not payload else payload))


        if self.telegram_bot_token and self.telegram_chat_id and \
           self.telegram_bot_token != "YOUR_TELEGRAM_BOT_TOKEN" and \
           self.telegram_chat_id != "YOUR_TELEGRAM_CHAT_ID":
            alert_tasks.append(self.send_telegram_alert(message))

        if alert_tasks:
            await asyncio.gather(*alert_tasks)
        else:
            logger.debug("No alert channels (webhook/Telegram) are properly configured to send.")


if __name__ == '__main__': # pragma: no cover
    # Example Usage (requires a running event loop)

    # Setup basic logging to see alerter's logs
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    class DummyAlertConfig:
        def get(self, key, default=None):
            # To test, set up a mock webhook receiver like https://webhook.site/
            # Or use a real Discord/Slack webhook URL.
            # IMPORTANT: Replace "YOUR_TEST_WEBHOOK_URL" with a real URL to test.
            # If it remains placeholder, alerts will be skipped.
            test_webhook = "YOUR_TEST_WEBHOOK_URL" # e.g., from webhook.site

            # Similarly for Telegram (though implementation is placeholder)
            test_tg_token = "YOUR_TEST_TELEGRAM_TOKEN"
            test_tg_chat_id = "YOUR_TEST_TELEGRAM_CHAT_ID"

            cfg = {
                "alerting.webhook_url": test_webhook,
                "alerting.telegram_bot_token": test_tg_token,
                "alerting.telegram_chat_id": test_tg_chat_id,
            }
            return cfg.get(key, default)

    async def test_alerts():
        config = DummyAlertConfig()
        alerter = Alerter(config)

        logger.info("--- Testing Alerter ---")

        # Test generic send_alert
        await alerter.send_alert("This is a test alert from Alerter!")

        # Test webhook with simple message
        await alerter.send_webhook_alert("Simple webhook test message.")

        # Test webhook with custom payload (e.g., for Slack)
        slack_payload = {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "A *structured* test alert via webhook! :tada:"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "plain_text",
                            "text": "Source: Alerter Test",
                            "emoji": True
                        }
                    ]
                }
            ]
        }
        # Note: If your webhook URL is for Discord, it expects 'content' or 'embeds', not Slack blocks.
        # This payload is Slack-specific. For Discord, use simple 'content' or construct an 'embeds' object.
        # If using a generic webhook like webhook.site, it will just show the JSON.
        if "hooks.slack.com" in (config.get("alerting.webhook_url") or ""):
             await alerter.send_webhook_alert("Structured Slack Alert", payload=slack_payload)
        else:
            logger.info("Webhook URL is not for Slack, sending simpler payload for structured test.")
            await alerter.send_webhook_alert("Structured (but generic JSON) alert test", payload={"title": "Test Alert", "details": "This is a test with custom JSON fields."})


        # Test Telegram (will log placeholder warning)
        await alerter.send_telegram_alert("Test Telegram message (placeholder).")

        logger.info("--- Alerter tests finished ---")
        logger.info("If you configured a real webhook URL, check if messages were received.")

    try:
        asyncio.run(test_alerts())
    except Exception as e:
        logger.error(f"Error in test_alerts: {e}")
