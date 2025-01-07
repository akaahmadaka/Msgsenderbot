# scheduler.py
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
from utils import (
    load_data, update_group_status, update_group_message,
    increment_error_count, remove_group, get_global_settings
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MessageScheduler:
    def __init__(self):
        """Initialize the scheduler with an empty task dictionary."""
        self.tasks: Dict[str, asyncio.Task] = {}
        logger.info("Message Scheduler initialized")

    async def schedule_message(self, bot, group_id: str, message: Optional[str] = None, delay: Optional[int] = None):
        """Schedule periodic messages for a group."""
        try:
            # Get settings
            data = load_data()
            settings = get_global_settings()
            
            if group_id not in data["groups"]:
                logger.error(f"Group {group_id} not found")
                return False

            # Use provided values or defaults
            message = message or settings["message"]
            delay = delay or settings["delay"]

            # Cancel existing task if any
            await self.remove_scheduled_job(group_id)

            # Create new task
            update_group_status(group_id, True)
            self.tasks[group_id] = asyncio.create_task(
                self._message_loop(bot, group_id, message, delay)
            )
            logger.info(f"Started message loop for group {group_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to schedule message for group {group_id}: {e}")
            return False

    async def _message_loop(self, bot, group_id: str, message: str, delay: int):
        """Internal message loop for sending periodic messages."""
        while True:
            try:
                # Check if group is still active
                data = load_data()
                if not data["groups"][group_id]["active"]:
                    logger.info(f"Stopping loop for inactive group {group_id}")
                    break

                # Send message with timeout
                try:
                    async with asyncio.timeout(30):
                        sent_message = await bot.send_message(
                            chat_id=int(group_id),
                            text=message
                        )
                        
                        # Update next schedule time
                        next_time = datetime.now() + timedelta(seconds=delay)
                        
                        # Update message info
                        update_group_message(
                            group_id, 
                            sent_message.message_id,
                            next_time
                        )
                        logger.info(f"Sent message to group {group_id}")

                except asyncio.TimeoutError:
                    logger.error(f"Message send timeout for group {group_id}")
                    await self.handle_error(group_id)
                    break

                # Wait for next iteration
                await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"Error in message loop for group {group_id}: {e}")
                await self.handle_error(group_id)
                break

    async def handle_error(self, group_id: str):
        """Handle errors and manage group status."""
        try:
            error_count = increment_error_count(group_id)
            
            if error_count >= 5:
                # Remove group after too many errors
                remove_group(group_id)
                logger.info(f"Removed group {group_id} due to excessive errors")
            else:
                # Deactivate group
                update_group_status(group_id, False)
                
            await self.remove_scheduled_job(group_id)
            
        except Exception as e:
            logger.error(f"Error handling failure for group {group_id}: {e}")

    async def remove_scheduled_job(self, group_id: str):
        """Stop and remove a scheduled task."""
        if group_id in self.tasks:
            try:
                self.tasks[group_id].cancel()
                await asyncio.sleep(0.1)
                del self.tasks[group_id]
                logger.info(f"Removed scheduled task for group {group_id}")
            except Exception as e:
                logger.error(f"Failed to remove task for group {group_id}: {e}")

    def is_running(self, group_id: str) -> bool:
        """Check if a group has an active task."""
        return group_id in self.tasks and not self.tasks[group_id].done()

    def get_active_tasks(self) -> int:
        """Get count of active tasks."""
        return len([task for task in self.tasks.values() if not task.done()])

    async def start(self):
        """Initialize scheduler and reset group states."""
        try:
            data = load_data()
            for group_id in data["groups"]:
                update_group_status(group_id, False)
            logger.info("Scheduler started and groups reset")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")

    async def shutdown(self):
        """Clean shutdown of all tasks."""
        try:
            task_count = len(self.tasks)
            for group_id in list(self.tasks.keys()):
                await self.remove_scheduled_job(group_id)
            self.tasks.clear()
            logger.info(f"Scheduler shutdown complete. Cancelled {task_count} tasks")
        except Exception as e:
            logger.error(f"Error during scheduler shutdown: {e}")

# Global scheduler instance
scheduler = MessageScheduler()

# Public interface functions
async def schedule_message(bot, group_id, message=None, delay=None):
    """Public function to schedule messages."""
    return await scheduler.schedule_message(bot, group_id, message, delay)

async def remove_scheduled_job(group_id):
    """Public function to remove a scheduled job."""
    await scheduler.remove_scheduled_job(group_id)

async def start_scheduler():
    """Public function to start the scheduler."""
    await scheduler.start()

async def stop_scheduler():
    """Public function to stop the scheduler."""
    await scheduler.shutdown()

def is_running(group_id: str) -> bool:
    """Public function to check if a group is active."""
    return scheduler.is_running(group_id)

def get_active_tasks_count() -> int:
    """Public function to get active task count."""
    return scheduler.get_active_tasks()
