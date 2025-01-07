# scheduler.py
import asyncio
from utils import load_data, save_data, remove_group, get_global_settings
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class MessageScheduler:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        logger.info(f"Message Scheduler initialized at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
    async def schedule_message(self, bot, group_id: str, message: Optional[str] = None, delay: Optional[int] = None):
        """Schedule a message to be sent to a group."""
        try:
            # Load current data and global settings
            data = load_data()
            global_settings = get_global_settings()
            group_data = data["groups"].get(group_id)
            
            if not group_data:
                logger.error(f"Group {group_id} not found in data")
                return False
                
            # Use provided message/delay or fallback to global settings
            message = message or global_settings["message"]
            delay = delay or global_settings["delay"]
            
            # Cancel existing task if any
            await self.remove_scheduled_job(group_id)
            
            # Create new task
            self.tasks[group_id] = asyncio.create_task(
                self._message_loop(bot, group_id, message, delay)
            )
            
            logger.info(f"New message loop scheduled for group {group_id} with delay {delay}s")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling message for group {group_id}: {e}")
            return False

    async def _message_loop(self, bot, group_id: str, message: str, delay: int):
        """Internal message loop that runs continuously."""
        while True:
            try:
                data = load_data()
                if not data["groups"].get(group_id, {}).get("loop_running", False):
                    logger.info(f"Loop stopped for group {group_id}")
                    break

                # Send message with timeout
                try:
                    async with asyncio.timeout(30):  # 30 seconds timeout
                        sent_message = await bot.send_message(
                            chat_id=int(group_id), 
                            text=message
                        )
                        logger.info(f"Message sent to group {group_id}")
                except asyncio.TimeoutError:
                    logger.error(f"Timeout sending message to group {group_id}")
                    await self.handle_send_error(bot, group_id)
                    break

                # Update data with UTC timestamp
                data = load_data()
                if group_id in data["groups"]:
                    next_run = datetime.now(timezone.utc) + timedelta(seconds=delay)
                    data["groups"][group_id].update({
                        "last_message_id": sent_message.message_id,
                        "last_run": datetime.now(timezone.utc).isoformat(),
                        "next_run": next_run.isoformat(),
                        "loop_running": True
                    })
                    save_data(data)
                
                # Wait for next iteration
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error in message loop for group {group_id}: {e}")
                await self.handle_send_error(bot, group_id)
                break

    async def handle_send_error(self, bot, group_id: str):
        """Handle errors when sending messages."""
        try:
            data = load_data()
            if group_id in data["groups"]:
                data["groups"][group_id]["loop_running"] = False
                data["groups"][group_id]["error_time"] = datetime.now(timezone.utc).isoformat()
                save_data(data)
            
            await self.remove_scheduled_job(group_id)
            logger.info(f"Handled error for group {group_id}")
            
        except Exception as e:
            logger.error(f"Error handling send error for group {group_id}: {e}")

    async def remove_scheduled_job(self, group_id: str):
        """Remove a scheduled job for a specific group."""
        if group_id in self.tasks:
            try:
                self.tasks[group_id].cancel()
                await asyncio.sleep(0.1)  # Give task time to cancel
                del self.tasks[group_id]
                logger.info(f"Removed scheduled job for group {group_id}")
            except Exception as e:
                logger.error(f"Error removing scheduled job for group {group_id}: {e}")

    def is_running(self, group_id: str) -> bool:
        """Check if a group has a running task."""
        return group_id in self.tasks and not self.tasks[group_id].done()

    def get_active_tasks(self) -> int:
        """Get the number of active tasks."""
        return len([task for task in self.tasks.values() if not task.done()])

    async def start(self):
        """Start the scheduler and reset all group states."""
        try:
            data = load_data()
            reset_count = 0
            for group_id, group_data in data["groups"].items():
                if group_data.get("loop_running", False):
                    group_data["loop_running"] = False
                    reset_count += 1
            save_data(data)
            logger.info(f"Scheduler started. Reset {reset_count} group states.")
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")

    async def shutdown(self):
        """Cancel all running tasks and cleanup."""
        active_tasks = len(self.tasks)
        try:
            for group_id in list(self.tasks.keys()):
                await self.remove_scheduled_job(group_id)
            self.tasks.clear()
            logger.info(f"Scheduler shutdown complete. Cancelled {active_tasks} tasks.")
        except Exception as e:
            logger.error(f"Error during scheduler shutdown: {e}")

# Global scheduler instance
scheduler = MessageScheduler()

# Public interface functions
async def schedule_message(bot, group_id, message=None, delay=None):
    """Schedule a message for a group."""
    return await scheduler.schedule_message(bot, group_id, message, delay)

async def remove_scheduled_job(group_id):
    """Remove a scheduled job for a group."""
    await scheduler.remove_scheduled_job(group_id)

async def start_scheduler():
    """Start the scheduler."""
    await scheduler.start()

async def stop_scheduler():
    """Stop the scheduler."""
    await scheduler.shutdown()

def is_running(group_id: str) -> bool:
    """Check if a group has a running message loop."""
    return scheduler.is_running(group_id)

def get_active_tasks_count() -> int:
    """Get the number of active tasks."""
    return scheduler.get_active_tasks()