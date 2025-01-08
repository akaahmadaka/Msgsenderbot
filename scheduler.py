import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
import pytz
from telegram.error import Forbidden, ChatMigrated, BadRequest, NetworkError
from utils import (
    load_data, save_data, remove_group, 
    get_global_settings, update_group_status,
    update_group_message
)

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
        
    async def schedule_message(self, bot, group_id: str, message: Optional[str] = None, delay: Optional[int] = None):
        try:
            data = load_data()
            settings = get_global_settings()
            
            if group_id not in data["groups"]:
                logger.error(f"Cannot start loop - Group {group_id} not found in database")
                return False
                
            message = message or settings["message"]
            delay = delay or settings["delay"]
            
            await self.remove_scheduled_job(group_id)
            
            # Set initial next schedule to current time so first message sends immediately
            next_time = datetime.now(pytz.UTC)
            update_group_message(group_id, data["groups"][group_id].get("last_msg_id"), next_time)
            
            update_group_status(group_id, True)
            self.tasks[group_id] = asyncio.create_task(
                self._message_loop(bot, group_id, message, delay)
            )
            logger.info(f"Started message loop for group {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to schedule messages for group {group_id} - {str(e)}")
            return False
            
    async def _message_loop(self, bot, group_id: str, message: str, delay: int):
        """Message loop that sends first message immediately then follows delay."""
        retry_count = 0
        max_retries = 3
        first_run = True  # Add flag for first message
        
        while True:
            try:
                data = load_data()
                if not data["groups"][group_id]["active"]:
                    logger.info(f"Loop stopped for group {group_id} - User command")
                    break

                # For first message, send immediately without delay
                if first_run:
                    first_run = False
                else:
                    # Calculate wait time for subsequent messages
                    current_time = datetime.now(pytz.UTC)
                    next_schedule_str = data["groups"][group_id].get("next_schedule")
                    next_time = self.calculate_next_schedule(current_time, next_schedule_str, delay)
                    wait_time = (next_time - current_time).total_seconds()
                    
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)

                try:
                    async with asyncio.timeout(30):
                        # Send new message
                        sent_message = await bot.send_message(
                            chat_id=int(group_id), 
                            text=message
                        )
                        
                        # Try to delete previous message
                        last_msg_id = data["groups"][group_id].get("last_msg_id")
                        if last_msg_id:
                            try:
                                await bot.delete_message(
                                    chat_id=int(group_id),
                                    message_id=last_msg_id
                                )
                            except Exception as del_err:
                                logger.warning(f"Could not delete previous message in {group_id}: {del_err}")
                        
                        # Calculate and set next schedule
                        next_time = datetime.now(pytz.UTC) + timedelta(seconds=delay)
                        update_group_message(group_id, sent_message.message_id, next_time)
                        
                        retry_count = 0

                except Forbidden as e:
                    error_msg = str(e).lower()
                    if "bot was kicked" in error_msg:
                        logger.error(f"Bot kicked from group {group_id} - Removing group")
                        await self.handle_critical_error(group_id, "Bot kicked", False)
                    elif "user is deactivated" in error_msg:
                        logger.error(f"Group {group_id} was deleted - Removing group")
                        await self.handle_critical_error(group_id, "Group deleted", False)
                    else:
                        logger.error(f"Bot blocked in group {group_id} - Removing group")
                        await self.handle_critical_error(group_id, "Bot blocked", False)
                    break
                    
                except ChatMigrated as e:
                    new_chat_id = e.new_chat_id
                    logger.info(f"Group {group_id} migrated → {new_chat_id}")
                    await self.handle_group_migration(bot, group_id, new_chat_id)
                    break
                    
                except BadRequest as e:
                    error_msg = str(e).lower()
                    if "chat not found" in error_msg:
                        logger.error(f"Group {group_id} not found - Removing group")
                        await self.handle_critical_error(group_id, "Group not found", False)
                    elif "not enough rights" in error_msg:
                        logger.error(f"Bot restricted in group {group_id} - Attempting to leave")
                        await self.handle_restriction(bot, group_id)
                    elif "bot was kicked" in error_msg:
                        logger.error(f"Bot kicked from group {group_id} - Removing group")
                        await self.handle_critical_error(group_id, "Bot kicked", False)
                    else:
                        logger.error(f"Error in group {group_id} - {str(e)}")
                        await self.handle_critical_error(group_id, str(e), False)
                    break
                    
                except (NetworkError, asyncio.TimeoutError) as e:
                    retry_count += 1
                    error_type = "Network error" if isinstance(e, NetworkError) else "Timeout error"
                    
                    if retry_count <= max_retries:
                        logger.warning(f"{error_type} in group {group_id} - Retry {retry_count}/{max_retries}")
                        await asyncio.sleep(5 * retry_count)
                        continue
                    else:
                        logger.error(f"{error_type} in group {group_id} - Max retries reached")
                        await self.handle_error(group_id)
                        break
                
            except Exception as e:
                logger.error(f"Unexpected error in group {group_id} - {str(e)}")
                await self.handle_critical_error(group_id, str(e), False)
                break

    async def handle_restriction(self, bot, group_id: str):
        try:
            logger.info(f"Leaving group {group_id}")
            try:
                await bot.leave_chat(chat_id=int(group_id))
                logger.info(f"Left group {group_id} successfully")
            except Exception as e:
                logger.error(f"Failed to leave group {group_id} - {str(e)}")
            
            logger.info(f"Removing group {group_id} details")
            await self.handle_critical_error(group_id, "Bot restricted and left", False)
            
        except Exception as e:
            logger.error(f"Failed to handle restriction for group {group_id} - {str(e)}")

    async def handle_group_migration(self, bot, old_group_id: str, new_group_id: str):
        try:
            logger.info(f"Updating group {old_group_id} → {new_group_id}")
            data = load_data()
            
            if old_group_id in data["groups"]:
                data["groups"][new_group_id] = data["groups"][old_group_id]
                del data["groups"][old_group_id]
                save_data(data)
                
                await self.remove_scheduled_job(old_group_id)
                await self.schedule_message(bot, new_group_id)
                logger.info(f"Group migration completed")
            
        except Exception as e:
            logger.error(f"Migration failed - {str(e)}")
            await self.handle_critical_error(old_group_id, "Migration failed", False)

    async def handle_critical_error(self, group_id: str, reason: str, should_retry: bool = False):
        try:
            if not should_retry:
                update_group_status(group_id, False)
                remove_group(group_id)
                await self.remove_scheduled_job(group_id)
                logger.info(f"Removed group {group_id}")
            else:
                update_group_status(group_id, False)
                await self.remove_scheduled_job(group_id)
                logger.warning(f"Temporary error for group {group_id} - Will retry")
                
        except Exception as e:
            logger.error(f"Failed to handle error for group {group_id} - {str(e)}")

    async def handle_error(self, group_id: str):
        try:
            update_group_status(group_id, False)
            await self.remove_scheduled_job(group_id)
        except Exception as e:
            logger.error(f"Error handler failed for group {group_id} - {str(e)}")

    async def remove_scheduled_job(self, group_id: str):
        if group_id in self.tasks:
            try:
                self.tasks[group_id].cancel()
                await asyncio.sleep(0.1)
                del self.tasks[group_id]
            except Exception as e:
                logger.error(f"Failed to remove task for group {group_id} - {str(e)}")

    async def update_running_tasks(self, bot, new_message: Optional[str] = None, new_delay: Optional[int] = None):
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
                            new_message or settings["message"],
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
                    next_schedule_str = group.get("next_schedule")
                    next_time = self.calculate_next_schedule(
                        current_time,
                        next_schedule_str,
                        settings["delay"]
                    )
                    # Update the schedule
                    update_group_message(group_id, group.get("last_msg_id"), next_time)
                    logger.info(f"Recovered schedule for group {group_id} - Next: {next_time.isoformat()}")
                    
                    # Create and start the message loop task
                    self.tasks[group_id] = asyncio.create_task(
                        self._message_loop(
                            None,  # Bot will be injected later from main.py
                            group_id,
                            settings["message"],
                            settings["delay"]
                        )
                    )
            
            logger.info(f"Scheduler initialized - {len(self.tasks)} active groups recovered")
        except Exception as e:
            logger.error(f"Scheduler start failed - {str(e)}")

    async def shutdown(self):
        try:
            task_count = len(self.tasks)
            for group_id in list(self.tasks.keys()):
                await self.remove_scheduled_job(group_id)
            self.tasks.clear()
            logger.info(f"Scheduler stopped - {task_count} tasks cancelled")
        except Exception as e:
            logger.error(f"Scheduler shutdown failed - {str(e)}")

# Global scheduler instance
scheduler = MessageScheduler()

# Public interface
async def schedule_message(bot, group_id, message=None, delay=None):
    return await scheduler.schedule_message(bot, group_id, message, delay)

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