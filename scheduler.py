# scheduler.py
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
from telegram.error import Forbidden, ChatMigrated, BadRequest, NetworkError
from utils import (
    load_data, save_data, remove_group, 
    get_global_settings, update_group_status
)

# Disable external loggers
logging.getLogger("apscheduler").setLevel(logging.WARNING)

# Configure simple logging
class SimpleFormatter(logging.Formatter):
    def format(self, record):
        return f"{record.getMessage()}"

handler = logging.StreamHandler()
handler.setFormatter(SimpleFormatter())
logger = logging.getLogger(__name__)
logger.handlers = [handler]
logger.setLevel(logging.INFO)

class MessageScheduler:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        logger.info("Scheduler ready")
        
    async def schedule_message(self, bot, group_id: str, message: Optional[str] = None, delay: Optional[int] = None):
        try:
            data = load_data()
            settings = get_global_settings()
            
            if group_id not in data["groups"]:
                logger.error(f"Group {group_id} not found")
                return False
                
            message = message or settings["message"]
            delay = delay or settings["delay"]
            
            await self.remove_scheduled_job(group_id)
            
            update_group_status(group_id, True)
            self.tasks[group_id] = asyncio.create_task(
                self._message_loop(bot, group_id, message, delay)
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Schedule failed for group {group_id}: {e}")
            return False

    async def _message_loop(self, bot, group_id: str, message: str, delay: int):
        while True:
            try:
                data = load_data()
                if not data["groups"][group_id]["active"]:
                    break

                try:
                    async with asyncio.timeout(30):
                        sent_message = await bot.send_message(
                            chat_id=int(group_id), 
                            text=message
                        )
                        
                except Forbidden as e:
                    logger.error(f"Bot was blocked/kicked from group {group_id}: {e}")
                    await self.handle_critical_error(group_id, "Bot was blocked or kicked from the group")
                    break
                    
                except ChatMigrated as e:
                    new_chat_id = e.new_chat_id
                    logger.error(f"Group {group_id} was migrated to {new_chat_id}")
                    await self.handle_critical_error(group_id, f"Group was migrated to {new_chat_id}")
                    break
                    
                except BadRequest as e:
                    if "chat not found" in str(e).lower():
                        logger.error(f"Group {group_id} not found: {e}")
                        await self.handle_critical_error(group_id, "Group not found")
                    elif "bot was kicked" in str(e).lower():
                        logger.error(f"Bot was kicked from group {group_id}: {e}")
                        await self.handle_critical_error(group_id, "Bot was kicked from the group")
                    else:
                        logger.error(f"Bad request for group {group_id}: {e}")
                        await self.handle_error(group_id)
                    break
                    
                except NetworkError as e:
                    logger.error(f"Network error in group {group_id}: {e}")
                    await self.handle_error(group_id)
                    break
                    
                except asyncio.TimeoutError:
                    logger.error(f"Timeout sending message to group {group_id}")
                    await self.handle_error(group_id)
                    break

                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Unexpected error in group {group_id}: {e}")
                await self.handle_error(group_id)
                break

    async def handle_critical_error(self, group_id: str, reason: str):
        """Handle critical errors that require group removal"""
        try:
            logger.error(f"Critical error in group {group_id}: {reason}")
            update_group_status(group_id, False)
            remove_group(group_id)  # Remove group from data.json
            await self.remove_scheduled_job(group_id)
        except Exception as e:
            logger.error(f"Failed to handle critical error for group {group_id}: {e}")

    async def handle_error(self, group_id: str):
        """Handle non-critical errors"""
        try:
            update_group_status(group_id, False)
            await self.remove_scheduled_job(group_id)
        except Exception as e:
            logger.error(f"Error handler failed for group {group_id}: {e}")

    async def remove_scheduled_job(self, group_id: str):
        if group_id in self.tasks:
            try:
                self.tasks[group_id].cancel()
                await asyncio.sleep(0.1)
                del self.tasks[group_id]
            except Exception as e:
                logger.error(f"Failed to remove job for group {group_id}: {e}")

    def is_running(self, group_id: str) -> bool:
        return group_id in self.tasks and not self.tasks[group_id].done()

    def get_active_tasks(self) -> int:
        return len([task for task in self.tasks.values() if not task.done()])

    async def start(self):
        try:
            data = load_data()
            for group_id in data["groups"]:
                update_group_status(group_id, False)
        except Exception as e:
            logger.error(f"Scheduler start failed: {e}")

    async def shutdown(self):
        try:
            for group_id in list(self.tasks.keys()):
                await self.remove_scheduled_job(group_id)
            self.tasks.clear()
        except Exception as e:
            logger.error(f"Scheduler shutdown failed: {e}")

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
