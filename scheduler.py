import asyncio
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
import pytz
from telegram.error import (
    Forbidden, BadRequest, NetworkError, ChatMigrated, RetryAfter
)
from utils import (
    load_data, save_data, remove_group, 
    get_global_settings, update_group_status,
    update_group_message
)

if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

# Disable all external loggers
for logger_name in ['httpx', 'telegram', 'apscheduler', 'asyncio']:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

# Configure clean logging without timestamps
class CleanFormatter(logging.Formatter):
    def format(self, record):
        return f"{record.getMessage()}"

# Setup root logger
handler = logging.StreamHandler()
handler.setFormatter(CleanFormatter())
logger = logging.getLogger('scheduler')
logger.handlers = [handler]
logger.setLevel(logging.INFO)

class MessageScheduler:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.pending_groups: Dict[str, dict] = {}  # Add this to store groups waiting for bot
        logger.info("Scheduler ready")

    def calculate_next_schedule(self, current_time: datetime, next_schedule_str: Optional[str], delay: int) -> datetime:
        """Calculate the appropriate next schedule time."""
        try:
            if next_schedule_str:
                next_schedule = datetime.fromisoformat(next_schedule_str.replace('Z', '+00:00'))
                if next_schedule > current_time:
                    return next_schedule
            return current_time + timedelta(seconds=delay)
        except Exception:
            return current_time + timedelta(seconds=delay)
        
    async def schedule_message(
        self, 
        bot, 
        group_id: str, 
        message_reference: Optional[dict] = None, 
        delay: Optional[int] = None
    ):
        """Schedule messages for a group."""
        try:
            data = load_data()
            settings = get_global_settings()
            
            if group_id not in data["groups"]:
                logger.error(f"Cannot start loop - Group {group_id} not found")
                return False
                
            message_ref = message_reference or settings.get("message_reference")
            if not message_ref:
                logger.error("No message reference found")
                return False
                
            delay_val = delay or settings["delay"]
            
            await self.remove_scheduled_job(group_id)
            
            # Set initial next schedule
            next_time = datetime.now(pytz.UTC)
            update_group_message(group_id, None, next_time)
            
            update_group_status(group_id, True)
            self.tasks[group_id] = asyncio.create_task(
                self._message_loop(bot, group_id, message_ref, delay_val)
            )
            logger.info(f"Started message loop for group {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to schedule messages for group {group_id}: {e}")
            return False

    async def _message_loop(self, bot, group_id: str, message_reference: dict, delay: int):
        """Message loop that sends first message immediately then follows delay."""
        retry_count = 0
        max_retries = 3
        first_run = True

        while True:
            try:
                data = load_data()
                if not data["groups"][group_id]["active"]:
                    logger.info(f"Loop stopped for group {group_id} - User command")
                    break

                try:
                    if not first_run:
                        current_time = datetime.now(pytz.UTC)
                        next_schedule_str = data["groups"][group_id].get("next_schedule")
                        next_time = self.calculate_next_schedule(current_time, next_schedule_str, delay)
                        wait_time = (next_time - current_time).total_seconds()
                        
                        if wait_time > 0:
                            await asyncio.sleep(wait_time)

                    first_run = False

                    async with timeout(30):
                        try:
                            sent_message = await bot.copy_message(
                                chat_id=int(group_id),
                                from_chat_id=message_reference["chat_id"],
                                message_id=message_reference["message_id"]
                            )
                        except (Forbidden, BadRequest) as e:
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
                                # First update the group status to inactive
                                update_group_status(group_id, False)
                                
                                try:
                                    # Then remove the scheduled job
                                    await self.remove_scheduled_job(group_id)
                                    # Finally remove the group data
                                    remove_group(group_id)
                                    logger.info(f"Successfully removed group {group_id} due to: {str(e)}")
                                except Exception as cleanup_error:
                                    logger.error(f"Error during cleanup for group {group_id}: {cleanup_error}")
                                return  # Exit the loop completely
                            raise  # Re-raise other errors
                            
                        # Message sent successfully - try to delete previous
                        last_msg_id = data["groups"][group_id].get("last_msg_id")
                        if last_msg_id:
                            try:
                                await bot.delete_message(int(group_id), last_msg_id)
                            except Exception as e:
                                logger.warning(f"Failed to delete previous message: {e}")
                        
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

    async def remove_scheduled_job(self, group_id: str):
        """Remove a scheduled job and ensure it's cancelled."""
        if group_id in self.tasks:
            try:
                task = self.tasks[group_id]
                if not task.done():
                    task.cancel()  # Cancel the task if it's not already done
                    try:
                        await task  # Wait for cancellation to complete
                    except asyncio.CancelledError:
                        pass  # This is expected
                    except Exception as e:
                        logger.warning(f"Error while waiting for task cancellation: {e}")
                
                del self.tasks[group_id]  # Remove from tasks dict
                logger.info(f"Successfully cancelled scheduled task for group {group_id}")
            except Exception as e:
                logger.error(f"Error cancelling task for group {group_id}: {e}")
                # Force remove the task from the dictionary even if cancellation failed
                self.tasks.pop(group_id, None)

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
            data = load_data()
            
            if old_group_id in data["groups"]:
                data["groups"][str(new_group_id)] = data["groups"][old_group_id]
                del data["groups"][old_group_id]
                save_data(data)
                
                await self.remove_scheduled_job(old_group_id)
                await self.schedule_message(bot, str(new_group_id))
                logger.info(f"Group migration completed")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            await self.remove_scheduled_job(old_group_id)
            remove_group(old_group_id)

    async def update_running_tasks(self, bot, new_message_reference: Optional[dict] = None, new_delay: Optional[int] = None):
        """Update all running tasks with new settings."""
        try:
            data = load_data()
            settings = get_global_settings()  # Get latest settings
            current_time = datetime.now(pytz.UTC)
            updated_count = 0
            
            # Store current tasks
            current_tasks = self.tasks.copy()
            
            # Cancel and recreate each running task with new settings
            for group_id, task in current_tasks.items():
                if not task.done() and data["groups"].get(group_id, {}).get("active", False):
                    # Cancel current task
                    await self.remove_scheduled_job(group_id)
                    
                    # Create new task with updated settings
                    self.tasks[group_id] = asyncio.create_task(
                        self._message_loop(
                            bot,
                            group_id,
                            new_message_reference or settings.get("message_reference"),
                            new_delay or settings["delay"]
                        )
                    )
                    
                    # Update next schedule
                    next_time = current_time + timedelta(seconds=(new_delay or settings["delay"]))
                    update_group_message(group_id, data["groups"][group_id].get("last_msg_id"), next_time)
                    
                    updated_count += 1
                    logger.info(f"Updated task for group {group_id} with new settings")
            
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
            data = load_data()
            settings = get_global_settings()
            current_time = datetime.now(pytz.UTC)
            
            for group_id, group in data["groups"].items():
                if group.get("active", False):
                    # Get the stored next schedule time
                    next_schedule_str = group.get("next_schedule")
                    
                    if next_schedule_str:
                        try:
                            # Parse the stored next schedule time
                            next_time = datetime.fromisoformat(next_schedule_str.replace('Z', '+00:00'))
                            
                            # If next_time is in the past, calculate new time from current
                            if next_time <= current_time:
                                next_time = current_time + timedelta(seconds=settings["delay"])
                        except (ValueError, TypeError):
                            # If there's any error parsing the time, use current time + delay
                            next_time = current_time + timedelta(seconds=settings["delay"])
                    else:
                        # If no schedule time exists, use current time + delay
                        next_time = current_time + timedelta(seconds=settings["delay"])
                    
                    # Update the schedule
                    update_group_message(group_id, group.get("last_msg_id"), next_time)
                    logger.info(f"Recovered schedule for group {group_id} - Next: {next_time.isoformat()}")
                    
                    # Store group info for later task creation
                    self.pending_groups[group_id] = {
                        "message_reference": settings.get("message_reference"),
                        "delay": settings["delay"],
                        "next_time": next_time  # Store the calculated next time
                    }
                
            logger.info(f"Scheduler initialized - {len(self.pending_groups)} active groups pending")
        except Exception as e:
            logger.error(f"Scheduler start failed: {e}")

    async def initialize_pending_tasks(self, bot):
        """Initialize tasks for recovered groups with bot instance."""
        try:
            current_time = datetime.now(pytz.UTC)
            
            for group_id, settings in self.pending_groups.items():
                next_time = settings["next_time"]
                wait_time = (next_time - current_time).total_seconds()
                
                if wait_time > 0:
                    # Create task with initial delay to match scheduled time
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
                    # If scheduled time is in the past, start immediately
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
            self.pending_groups.clear()  # Clear pending groups after starting tasks
            return started_count
        except Exception as e:
            logger.error(f"Failed to initialize pending tasks: {e}")
            return 0

    async def _delayed_message_loop(self, bot, group_id: str, message_reference: dict, delay: int, initial_delay: float):
        """Message loop with initial delay for recovered tasks."""
        try:
            # Wait for the initial delay
            await asyncio.sleep(initial_delay)
            
            # Then start the regular message loop
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

async def remove_scheduled_job(group_id):
    await scheduler.remove_scheduled_job(group_id)

async def start_scheduler():
    await scheduler.start()

async def stop_scheduler():
    await scheduler.shutdown()

def is_running(group_id: str) -> bool:
    return scheduler.is_running(group_id)

def get_active_tasks_count() -> int:
    return scheduler.get_active_tasks()