import asyncio
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional
import pytz
import sqlite3
from telegram.error import (
    Forbidden, BadRequest, NetworkError, ChatMigrated, RetryAfter
)
from db import get_db_connection
from utils import (
    get_group,
    remove_group,  # Keep remove_group
    get_global_settings, update_group_status,
    update_group_message, add_group,
    load_data
)

if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

import logging
from logger_config import setup_logger

# Setup logger
setup_logger()
logger = logging.getLogger(__name__)


class MessageScheduler:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.pending_groups: Dict[str, dict] = {}  # Add this to store groups waiting for bot
        logger.info("Scheduler ready")

    def calculate_next_schedule(self, current_time: datetime, next_schedule_str: Optional[str], delay: int) -> datetime:
        """Calculate the appropriate next schedule time."""
        try:
            next_schedule = datetime.fromisoformat(next_schedule_str.replace('Z', '+00:00'))
            return next_schedule if next_schedule > current_time else current_time + timedelta(seconds=delay)
        except (ValueError, TypeError):
            return current_time + timedelta(seconds=delay)  # Default to delay if parsing fails
        
    async def schedule_message(
        self, 
        bot, 
        group_id: str,
        message_reference: Optional[dict] = None, 
        delay: Optional[int] = None
    ):
        """Schedule messages for a group."""
        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with get_db_connection() as conn, conn:
                        settings = get_global_settings(conn.cursor())

                        if group_id not in self.tasks:
                            add_group(group_id, "Unknown Group", conn.cursor())
                        
                        message_ref = message_reference or settings.get("message_reference")
                        if not message_ref:
                            logger.error("No message reference found")
                            return False

                        delay_val = delay or settings["delay"]

                        next_time = datetime.now(pytz.UTC)
                        update_group_message(group_id, None, next_time)
                        update_group_status(group_id, True)

                        self.tasks[group_id] = asyncio.create_task(
                            self._message_loop(bot, group_id, message_ref, delay_val)
                        )
                        conn.commit()
                        logger.info(f"Started message loop for group {group_id}")
                        break
                except sqlite3.OperationalError as e:
                    if attempt == max_retries - 1 or "database is locked" in str(e):
                        logger.warning(f"Database locked, retrying... (Attempt {attempt + 1})")
                        await asyncio.sleep(0.5 * (attempt + 1))
                await asyncio.sleep(0.1 * (attempt + 1))
                continue

        except Exception as e:
            logger.error(f"Failed to schedule messages for group {group_id}: {e}")
            raise

    async def _send_and_delete_message(self, bot, group_id: str, message_reference: dict, group_data: dict):
        """Send the message and delete the previous one."""
        try:
            logger.debug(f'bot: {bot}, group_id: {group_id}, message_reference: {message_reference}, group_data: {group_data}')
            sent_message = await bot.copy_message(
                chat_id=int(group_id),
                from_chat_id=message_reference["chat_id"],
                message_id=message_reference["message_id"]
            )

            # Message sent successfully - try to delete previous
            last_msg_id = group_data.get("last_msg_id")
            if last_msg_id:
                try:
                    logger.debug(f'bot: {bot}, group_id: {group_id}, last_msg_id: {last_msg_id}')
                    await bot.delete_message(int(group_id), last_msg_id)
                except Exception as e:
                    logger.warning(f"Failed to delete previous message: {e}")
            return sent_message
        except (Forbidden, BadRequest, NetworkError, ChatMigrated, RetryAfter) as e:
            error_msg = str(e).lower()
            logger.error(f"Telegram error in group {group_id}: {str(e)}")

            # Force cleanup for any permission/access error
            if any(msg in error_msg for msg in [
                "forbidden",
                "not a member",
                "chat not found",
                "bot was kicked",
                "not enough rights",
                "chat_write_forbidden",
                "the message can't be copied",
                "chat_send_plain_forbidden"
            ]):
                update_group_status(group_id, False)
                try:                    
                    remove_group(group_id)
                    logger.info(f"Successfully removed group {group_id} due to: {str(e)}")
                except Exception as cleanup_error:
                    logger.error(f"Error during cleanup for group {group_id}: {cleanup_error}")
                return None  # Fatal error - exit loop
            raise  # Re-raise other exceptions for retry logic in _message_loop


    async def _message_loop(self, bot, group_id: str, message_reference: dict, delay: int):
        """Message loop that sends first message immediately then follows delay."""
        retry_count = 0
        max_retries = 3
        first_run = True

        while True:
            try:
                group_data = get_group(group_id)
                if not group_data or not group_data["active"]:
                    logger.info(f"Loop stopped for group {group_id} - User command or group not found")
                    break

                try:
                    if not first_run:
                        current_time = datetime.now(pytz.UTC)
                        next_schedule_str = group_data.get("next_schedule")
                        next_time = self.calculate_next_schedule(current_time, next_schedule_str, delay)
                        wait_time = (next_time - current_time).total_seconds()

                        if wait_time > 0:
                            await asyncio.sleep(wait_time)

                    first_run = False

                    async with timeout(30):
                        sent_message = await self._send_and_delete_message(bot, group_id, message_reference, group_data)
                        if sent_message is None:
                            return # Exit if fatal error

                        # Update group data
                        retry_count = 0  # Reset retry count on success
                        next_time = datetime.now(pytz.UTC) + timedelta(seconds=delay)
                        update_group_message(group_id, sent_message.message_id, next_time)

                except asyncio.TimeoutError:
                    logger.error(f"Timeout sending message to group {group_id}")
                    retry_count += 1
                    if retry_count >= max_retries:
                        await self.remove_scheduled_job(group_id)
                        return
                    continue

            except Exception as e:
                logger.error(f"Unexpected error in message loop: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    await self.remove_scheduled_job(group_id)
                    return
                await asyncio.sleep(5)

    async def cleanup_group(self, group_id: str, reason: str):
        """Cleanup all resources for a group in the correct order."""
        try:
            # First mark the group as inactive
            update_group_status(group_id, False)
            
            # Then remove the scheduled job
            await self.remove_scheduled_job(group_id)
            
            # Finally remove the group data
            remove_group(group_id)
            
            logger.info(f"Completed cleanup for group {group_id}. Reason: {reason}")
        except Exception as e:
            logger.error(f"Error during cleanup for group {group_id}: {e}")

    async def handle_group_migration(self, bot, old_group_id: str, new_group_id: str):
        """Handle group migration by updating group ID."""
        try:
            logger.info(f"Updating group {old_group_id} → {new_group_id}")
            group_data = get_group(old_group_id)
            if group_data:
                remove_group(old_group_id)  # Remove old group
            # Then remove the scheduled job
            if old_group_id in self.tasks:
                try:
                    task = self.tasks[old_group_id]
                    if not task.done():
                        task.cancel()  # Cancel the task if it's not already done
                        try:
                            await task  # Wait for cancellation to complete
                        except asyncio.CancelledError:
                            pass  # This is expected
                        except Exception as e:
                            logger.warning(f"Error while waiting for task cancellation: {e}")

                    del self.tasks[old_group_id]  # Remove from tasks dict
                    logger.info(f"Successfully cancelled scheduled task for group {old_group_id}")
                except Exception as e:
                    logger.error(f"Error cancelling task for group {old_group_id}: {e}")
                group_data["group_id"] = str(new_group_id)  # Update the group ID
                add_group(str(new_group_id), group_data["name"])  # Add the group with a new ID
                # Ensure next_schedule is a valid datetime string
                next_schedule = group_data["next_schedule"]
                if isinstance(next_schedule, datetime):
                    next_schedule_str = next_schedule.isoformat()
                else:
                    next_schedule_str = next_schedule  # Assume it's already a string

                utils.update_group_message(str(new_group_id), group_data["last_msg_id"], next_schedule_str)  # Update message
                utils.update_group_status(str(new_group_id), group_data["active"])  # Update status

                await self.remove_scheduled_job(old_group_id)
                await self.schedule_message(bot, str(new_group_id))                
                logger.info(f"Group migration completed")
            else:
                logger.info(f"Group {old_group_id} not found, skipping migration")

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            remove_group(old_group_id)

    async def update_running_tasks(self, bot, new_message_reference: Optional[dict] = None, new_delay: Optional[int] = None):
        """Update all running tasks with new settings."""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                conn.execute("BEGIN IMMEDIATE")
                settings = get_global_settings(cursor)

                updated_count = 0
                
                # Iterate through a copy of the tasks dictionary
                for group_id, task in list(self.tasks.items()):
                    group_data = get_group(group_id)
                    if not task.done() and group_data and group_data.get("active", False):              

                        # Schedule new task with updated settings
                        await self.schedule_message(
                            bot,
                            group_id,
                            message_reference=new_message_reference or settings.get("message_reference"),
                            delay=new_delay or settings["delay"]
                        )

                        updated_count += 1
                        logger.info(f"Updated task for group {group_id} with new settings")
                
                conn.commit()
                return updated_count

        except Exception as e:
            logger.error(f"Failed to update running tasks: {e}")
            return 0


    def is_running(self, group_id: str) -> bool:
        return group_id in self.tasks and not self.tasks[group_id].done()

    def get_active_tasks(self) -> int:
        return len([task for task in self.tasks.values() if not task.done()])

    async def start(self):
        """Initialize scheduler and recover active tasks."""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                settings = get_global_settings(cursor)
                current_time = datetime.now(pytz.UTC)
                groups_data = load_data(cursor)["groups"]
                
                for group_id, group in groups_data.items():
                    if group.get("active", False):
                        next_schedule = group.get("next_schedule")
                        next_schedule_str = next_schedule.isoformat() if next_schedule else None
                        next_time = self.calculate_next_schedule(current_time, next_schedule_str, settings["delay"])
                        update_group_message(group_id, group.get("last_msg_id"), next_time, cursor)
                        logger.info(f"Recovered schedule for group {group_id} - Next: {next_time.isoformat()}")
                        self.pending_groups[group_id] = {
                            "message_reference": settings.get("message_reference"),
                            "delay": settings["delay"],
                            "next_time": next_time
                        }
                conn.commit()
                logger.info(f"Scheduler initialized - {len(self.pending_groups)} active groups pending")
        except Exception as e:
            logger.error(f"Scheduler start failed: {e}")

    async def initialize_pending_tasks(self, bot):
        """Initialize tasks for recovered groups with bot instance."""
        try:
            for group_id, settings in self.pending_groups.items():
                next_time = settings["next_time"]
                current_time = datetime.now(pytz.UTC)
                wait_time = (next_time - current_time).total_seconds()

                if wait_time > 0:
                    self.tasks[group_id] = asyncio.create_task(
                        self._delayed_message_loop(
                            bot,
                            group_id,
                            settings["message_reference"],
                            settings["delay"],
                            initial_delay=wait_time
                        )
                    )
                else:
                    self.tasks[group_id] = asyncio.create_task(
                        self._message_loop(
                            bot,
                            group_id,
                            settings["message_reference"],
                            settings["delay"]
                        )
                    )
                logger.info(f"Created task for recovered group {group_id} - Wait time: {max(0, wait_time):.1f}s")

            started_count = len(self.pending_groups)
            self.pending_groups.clear()
            return started_count
        except Exception as e:
            logger.error(f"Failed to initialize pending tasks: {e}")
            return 0

    async def _delayed_message_loop(self, bot, group_id: str, message_reference: dict, delay: int, initial_delay: float):
        """Message loop with initial delay for recovered tasks."""
        try:
            await asyncio.sleep(initial_delay)
            await self._message_loop(bot, group_id, message_reference, delay)
        except Exception as e:
            logger.error(f"Delayed message loop failed for group {group_id}: {e}")

    async def shutdown(self):
        try:
            task_count = len(self.tasks)
            for group_id in list(self.tasks.keys()):
                await self.remove_scheduled_job(group_id)
            self.tasks.clear()
            logger.info(f"Scheduler stopped - {task_count} tasks cancelled")
        except Exception as e:
            logger.error(f"Scheduler shutdown failed: {e}")

# Global scheduler instance
scheduler = MessageScheduler()

# Public interface
async def schedule_message(bot, group_id, message_reference=None, delay=None):
    return await scheduler.schedule_message(bot, group_id, message_reference, delay)

async def remove_scheduled_job(group_id, reason="Manual removal"):
    await scheduler.cleanup_group(group_id, reason)

async def start_scheduler():
    await scheduler.start()

async def stop_scheduler():
    await scheduler.shutdown()

def is_running(group_id: str) -> bool:
    return scheduler.is_running(group_id)

def get_active_tasks_count() -> int:
    return scheduler.get_active_tasks()