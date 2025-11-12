"""
Notification system for HEX Data Processor.

Supports Telegram, Email, and Webhook notifications.
"""

import os
import smtplib
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import httpx
from dotenv import load_dotenv

from .logger import get_logger, log_async_function_call
from .config import NotificationsConfig

# Load environment variables
load_dotenv()


class NotificationError(Exception):
    """Raised when notification fails."""
    pass


class TelegramNotifier:
    """Telegram bot notification handler."""
    
    def __init__(self, bot_token: str, chat_id: str):
        """Initialize Telegram notifier."""
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.logger = get_logger(__name__)
    
    @log_async_function_call()
    async def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        """
        Send message via Telegram bot.
        
        Args:
            message: Message to send
            parse_mode: Message parsing mode (HTML, Markdown)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            url = f"{self.base_url}/sendMessage"
            
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": parse_mode
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=data)
                response.raise_for_status()
                
                result = response.json()
                if result.get("ok"):
                    self.logger.info("Telegram message sent successfully")
                    return True
                else:
                    error = result.get("description", "Unknown error")
                    self.logger.error(f"Telegram API error: {error}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message: {str(e)}")
            return False
    
    async def send_success_notification(self, run_id: str, stats: Dict[str, Any]) -> bool:
        """Send success notification."""
        message = f"""
✅ <b>Data Processing Completed</b>

📊 <b>Run ID:</b> {run_id}
📈 <b>Items Processed:</b> {stats.get('processed_count', 0)}
💾 <b>Items Saved:</b> {stats.get('saved_count', 0)}
⏱️ <b>Duration:</b> {stats.get('duration', 'N/A')}
📁 <b>Output:</b> {stats.get('output_path', 'N/A')}

<b>Timestamp:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
        """.strip()
        
        return await self.send_message(message)
    
    async def send_error_notification(self, run_id: str, error: str, context: Dict[str, Any]) -> bool:
        """Send error notification."""
        message = f"""
❌ <b>Data Processing Failed</b>

🚨 <b>Run ID:</b> {run_id}
🔥 <b>Error:</b> {error}
🎯 <b>Target:</b> {context.get('target', 'N/A')}
⏱️ <b>Started:</b> {context.get('start_time', 'N/A')}

<b>Timestamp:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
        """.strip()
        
        return await self.send_message(message)


class EmailNotifier:
    """Email notification handler."""
    
    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str,
        password: str,
        use_tls: bool = True
    ):
        """Initialize email notifier."""
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.logger = get_logger(__name__)
    
    @log_async_function_call()
    async def send_email(
        self,
        to_addresses: List[str],
        subject: str,
        body: str,
        from_address: Optional[str] = None
    ) -> bool:
        """
        Send email notification.
        
        Args:
            to_addresses: List of recipient email addresses
            subject: Email subject
            body: Email body
            from_address: Sender email address
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = from_address or self.username
            msg['To'] = ', '.join(to_addresses)
            msg['Subject'] = subject
            
            # Add body
            msg.attach(MIMEText(body, 'html'))
            
            # Send email in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._send_sync, msg)
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return False
    
    def _send_sync(self, msg: MIMEMultipart) -> bool:
        """Synchronous email sending."""
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                
                server.login(self.username, self.password)
                server.send_message(msg)
            
            self.logger.info("Email sent successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return False
    
    async def send_success_notification(
        self,
        to_addresses: List[str],
        run_id: str,
        stats: Dict[str, Any]
    ) -> bool:
        """Send success notification."""
        subject = f"✅ Data Processing Completed - {run_id}"
        
        body = f"""
        <html>
        <body>
            <h2 style="color: #28a745;">✅ Data Processing Completed</h2>
            
            <table style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Run ID:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{run_id}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Items Processed:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{stats.get('processed_count', 0)}</td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Items Saved:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{stats.get('saved_count', 0)}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Duration:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{stats.get('duration', 'N/A')}</td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Output Path:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{stats.get('output_path', 'N/A')}</td>
                </tr>
            </table>
            
            <p style="margin-top: 20px; color: #6c757d;">
                <em>Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</em>
            </p>
        </body>
        </html>
        """
        
        return await self.send_email(to_addresses, subject, body)
    
    async def send_error_notification(
        self,
        to_addresses: List[str],
        run_id: str,
        error: str,
        context: Dict[str, Any]
    ) -> bool:
        """Send error notification."""
        subject = f"❌ Data Processing Failed - {run_id}"
        
        body = f"""
        <html>
        <body>
            <h2 style="color: #dc3545;">❌ Data Processing Failed</h2>
            
            <table style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Run ID:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{run_id}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Error:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6; color: #dc3545;">{error}</td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Target:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{context.get('target', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #dee2e6;"><strong>Start Time:</strong></td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">{context.get('start_time', 'N/A')}</td>
                </tr>
            </table>
            
            <p style="margin-top: 20px; color: #6c757d;">
                <em>Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</em>
            </p>
        </body>
        </html>
        """
        
        return await self.send_email(to_addresses, subject, body)


class WebhookNotifier:
    """Webhook notification handler."""
    
    def __init__(self, webhook_url: str, timeout: int = 10):
        """Initialize webhook notifier."""
        self.webhook_url = webhook_url
        self.timeout = timeout
        self.logger = get_logger(__name__)
    
    @log_async_function_call()
    async def send_webhook(self, payload: Dict[str, Any]) -> bool:
        """
        Send webhook notification.
        
        Args:
            payload: JSON payload to send
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add timestamp
            payload['timestamp'] = datetime.utcnow().isoformat() + 'Z'
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(self.webhook_url, json=payload)
                response.raise_for_status()
                
                self.logger.info(f"Webhook sent successfully to {self.webhook_url}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to send webhook: {str(e)}")
            return False
    
    async def send_success_notification(self, run_id: str, stats: Dict[str, Any]) -> bool:
        """Send success notification."""
        payload = {
            "event": "processing_completed",
            "status": "success",
            "run_id": run_id,
            "stats": stats
        }
        
        return await self.send_webhook(payload)
    
    async def send_error_notification(self, run_id: str, error: str, context: Dict[str, Any]) -> bool:
        """Send error notification."""
        payload = {
            "event": "processing_failed",
            "status": "error",
            "run_id": run_id,
            "error": error,
            "context": context
        }
        
        return await self.send_webhook(payload)


class NotificationManager:
    """Main notification manager."""
    
    def __init__(self, config: Optional[NotificationsConfig] = None):
        """Initialize notification manager."""
        self.config = config
        self.logger = get_logger(__name__)
        
        self.telegram_notifier = None
        self.email_notifier = None
        self.webhook_notifier = None
        
        if config:
            self._setup_notifiers()
    
    def _setup_notifiers(self):
        """Setup notification handlers from config."""
        # Telegram
        if self.config.telegram and self.config.telegram.enabled:
            self.telegram_notifier = TelegramNotifier(
                bot_token=self.config.telegram.bot_token,
                chat_id=self.config.telegram.chat_id
            )
        
        # Email
        if self.config.email and self.config.email.enabled:
            self.email_notifier = EmailNotifier(
                smtp_host=self.config.email.smtp_host,
                smtp_port=self.config.email.smtp_port,
                username=self.config.email.username,
                password=self.config.email.password,
                use_tls=self.config.email.use_tls
            )
        
        # Webhook
        if self.config.webhook and self.config.webhook.enabled:
            self.webhook_notifier = WebhookNotifier(
                webhook_url=self.config.webhook.url,
                timeout=self.config.webhook.timeout
            )
    
    async def send_success_notifications(self, run_id: str, stats: Dict[str, Any]) -> Dict[str, bool]:
        """Send success notifications via all enabled channels."""
        results = {}
        
        # Telegram
        if self.telegram_notifier and self.config.telegram.on_success:
            results['telegram'] = await self.telegram_notifier.send_success_notification(run_id, stats)
        
        # Email
        if self.email_notifier and self.config.email.on_success:
            results['email'] = await self.email_notifier.send_success_notification(
                self.config.email.to_addresses, run_id, stats
            )
        
        # Webhook
        if self.webhook_notifier and self.config.webhook.on_success:
            results['webhook'] = await self.webhook_notifier.send_success_notification(run_id, stats)
        
        return results
    
    async def send_error_notifications(self, run_id: str, error: str, context: Dict[str, Any]) -> Dict[str, bool]:
        """Send error notifications via all enabled channels."""
        results = {}
        
        # Telegram
        if self.telegram_notifier and self.config.telegram.on_error:
            results['telegram'] = await self.telegram_notifier.send_error_notification(run_id, error, context)
        
        # Email
        if self.email_notifier and self.config.email.on_error:
            results['email'] = await self.email_notifier.send_error_notification(
                self.config.email.to_addresses, run_id, error, context
            )
        
        # Webhook
        if self.webhook_notifier and self.config.webhook.on_error:
            results['webhook'] = await self.webhook_notifier.send_error_notification(run_id, error, context)
        
        return results
    
    def get_enabled_channels(self) -> List[str]:
        """Get list of enabled notification channels."""
        channels = []
        
        if self.telegram_notifier:
            channels.append('telegram')
        if self.email_notifier:
            channels.append('email')
        if self.webhook_notifier:
            channels.append('webhook')
        
        return channels


if __name__ == "__main__":
    # Test notification system
    async def test_notifications():
        # This would require actual configuration values
        print("Notification system test requires configuration")
        
        # Example usage would be:
        # config = NotificationsConfig(...)
        # manager = NotificationManager(config)
        # await manager.send_success_notifications("test_run", {"processed_count": 10})
    
    asyncio.run(test_notifications())