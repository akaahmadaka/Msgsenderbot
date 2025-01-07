# scheduler.py
import asyncio
from utils import load_data, save_data, remove_group
import datetime
from typing import Dict, Optional

class MessageScheduler:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        
    async def schedule_message(self, bot, group_id: str, message: Optional[str] = None, delay: Optional[int] = None):
        """Schedule a message to be sent to a group."""
        # Load current data
        data = load_data()
        group_data = data["groups"].get(group_id)
        
        if not group_data:
            return
            
        # Use provided message/delay or fallback to stored values
        message = message or group_data.get("message", "Default message")
        delay = delay or group_data.get("delay", 3600)
        
        # Cancel existing task if any
        self.remove_scheduled_job(group_id)
        
        # Create new task
        self.tasks[group_id] = asyncio.create_task(
            self._message_loop(bot, group_id, message, delay)
        )
    
    async def _message_loop(self, bot, group_id: str, message: str, delay: int):
        """Internal message loop that runs continuously."""
        while True:
            try:
                # Send message
                sent_message = await bot.send_message(chat_id=int(group_id), text=message)
                
                # Update data
                data = load_data()
                if group_id in data["groups"]:
                    data["groups"][group_id]["last_message_id"] = sent_message.message_id
                    data["groups"][group_id]["next_run_time"] = (
                        datetime.datetime.now() + datetime.timedelta(seconds=delay)
                    ).isoformat()
                    save_data(data)
                
                # Wait for next iteration
                await asyncio.sleep(delay)
                
            except Exception as e:
                print(f"Error in message loop for group {group_id}: {e}")
                await self.handle_send_error(bot, group_id)
                break
    
    async def handle_send_error(self, bot, group_id: str):
        """Handle errors when sending messages."""
        try:
            await bot.leave_chat(chat_id=int(group_id))
        except Exception as e:
            print(f"Error leaving group {group_id}: {e}")
        finally:
            remove_group(group_id)
            self.remove_scheduled_job(group_id)
    
    def remove_scheduled_job(self, group_id: str):
        """Remove a scheduled job for a specific group."""
        if group_id in self.tasks:
            self.tasks[group_id].cancel()
            del self.tasks[group_id]
    
    def start(self):
        """Start the scheduler (no-op in this implementation)."""
        pass
    
    def shutdown(self):
        """Cancel all running tasks."""
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()

# Global scheduler instance
scheduler = MessageScheduler()

# Expose the same interface as before
async def schedule_message(bot, group_id, message=None, delay=None):
    await scheduler.schedule_message(bot, group_id, message, delay)

def remove_scheduled_job(group_id):
    scheduler.remove_scheduled_job(group_id)

def start_scheduler():
    scheduler.start()

def stop_scheduler():
    scheduler.shutdown()