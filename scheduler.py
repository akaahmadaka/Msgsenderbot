import asyncio
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional
import pytz
import aiosqlite # Changed import
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
                    async with get_db_connection() as conn: # Use async with
                        # Use await for async functions
                        settings = await get_global_settings() # Removed cursor

                        if group_id not in self.tasks:
                            # Ensure group exists before scheduling
                            await add_group(group_id, "Unknown Group") # Removed cursor

                        message_ref = message_reference or settings.get("message_reference")
                        if not message_ref:
                            logger.error(f"No message reference found for group {group_id}")
                            return False # Indicate failure

                        delay_val = delay if delay is not None else settings.get("delay")
                        if delay_val is None:
                             logger.error(f"No delay value found for group {group_id}")
                             return False # Indicate failure

                        next_time = datetime.now(pytz.UTC)
                        # Update DB asynchronously
                        await update_group_message(group_id, None, next_time)
                        await update_group_status(group_id, True)

                        # Cancel existing task if it exists before creating a new one
                        if group_id in self.tasks and not self.tasks[group_id].done():
                            self.tasks[group_id].cancel()
                            try:
                                await self.tasks[group_id]
                            except asyncio.CancelledError:
                                logger.debug(f"Cancelled existing task for group {group_id} before rescheduling.")
                            except Exception as e_cancel:
                                logger.error(f"Error cancelling existing task for {group_id}: {e_cancel}")


                        self.tasks[group_id] = asyncio.create_task(
                            self._message_loop(bot, group_id, message_ref, delay_val)
                        )
                        # Commit is handled within the utility functions now
                        logger.info(f"Started/Updated message loop for group {group_id}")
                        return True # Indicate success
                except aiosqlite.OperationalError as e: # Catch aiosqlite error
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        wait_time = 0.1 * (attempt + 1)
                        logger.warning(f"Database locked on schedule_message, retrying in {wait_time}s... (Attempt {attempt + 1})")
                        await asyncio.sleep(wait_time) # Use asyncio.sleep
                        continue # Retry the loop
                    else: # Max retries reached or different OperationalError
                         logger.error(f"Database operational error scheduling for group {group_id} after {attempt + 1} attempts: {e}")
                         return False # Indicate failure
                except Exception as e: # Catch other potential errors
                     logger.error(f"Unexpected error scheduling messages for group {group_id}: {e}")
                     # Depending on the error, might want to update group status or cleanup
                     # For now, just indicate failure
                     return False # Indicate failure

            # If loop finishes without success (e.g., max retries on lock)
            logger.error(f"Failed to schedule message for group {group_id} after {max_retries} attempts.")
            return False

        # This part should ideally not be reached if the loop handles returns properly
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
                "chat_send_plain_forbidden",
                "message to copy not found" # Add specific error for copy_message failure
            ]):
                logger.warning(f"Fatal Telegram error for group {group_id}, initiating cleanup: {str(e)}")
                # Use the dedicated cleanup function which handles status update, task removal, and DB removal
                asyncio.create_task(self.cleanup_group(group_id, f"Telegram API Error: {str(e)}"))
                # No need to call update_group_status or remove_group directly here
                # try:
                #     await update_group_status(group_id, False) # Added await
                #     await remove_group(group_id) # Added await
                #     logger.info(f"Successfully removed group {group_id} due to: {str(e)}")
                # except Exception as cleanup_error:
                #     logger.error(f"Error during cleanup for group {group_id}: {cleanup_error}")
                return None  # Fatal error - exit loop
            raise  # Re-raise other exceptions for retry logic in _message_loop


    async def _message_loop(self, bot, group_id: str, message_reference: dict, delay: int):
        """Message loop that sends first message immediately then follows delay."""
        retry_count = 0
        max_retries = 3
        first_run = True

        while True:
            try:
                # Fetch group data asynchronously
                group_data = await get_group(group_id) # Added await
                if not group_data or not group_data.get("active"): # Check .get("active") for safety
                    logger.info(f"Loop stopping for group {group_id} - Group not found or inactive in DB.")
                    # Ensure task is removed from internal tracking if it stops itself
                    if group_id in self.tasks:
                         del self.tasks[group_id]
                    break # Exit the loop cleanly

                try:
                    if not first_run:
                        current_time = datetime.now(pytz.UTC)
                        # group_data["next_schedule"] should be a datetime object from utils.get_group
                        next_schedule_dt = group_data.get("next_schedule")
                        # Pass datetime object directly if available, else None
                        next_time = self.calculate_next_schedule(current_time, next_schedule_dt, delay)
                        wait_time = (next_time - current_time).total_seconds()

                        if wait_time > 0:
                            logger.debug(f"Group {group_id}: Waiting {wait_time:.2f} seconds...")
                            await asyncio.sleep(wait_time)

                    first_run = False

                    # Timeout for the combined send/delete and DB update operation
                    async with timeout(45): # Increased timeout slightly
                        sent_message = await self._send_and_delete_message(bot, group_id, message_reference, group_data)
                        if sent_message is None:
                            # Fatal error handled in _send_and_delete_message, cleanup initiated there.
                            # Task should exit.
                            logger.warning(f"Exiting loop for group {group_id} due to fatal error during send/delete.")
                            if group_id in self.tasks: del self.tasks[group_id] # Ensure removal from tracking
                            return # Exit the task

                        # Update group data asynchronously
                        retry_count = 0  # Reset retry count on success
                        next_time_update = datetime.now(pytz.UTC) + timedelta(seconds=delay)
                        await update_group_message(group_id, sent_message.message_id, next_time_update) # Added await

                except asyncio.TimeoutError:
                    logger.error(f"Timeout during send/delete/update for group {group_id}")
                    retry_count += 1 # Increment here
                    if retry_count >= max_retries:
                        logger.error(f"Max retries reached for group {group_id} due to timeout. Initiating cleanup.")
                        await self.cleanup_group(group_id, "Timeout after max retries")
                        return # Exit the task
                    await asyncio.sleep(5 * retry_count) # Sleep here

                except aiosqlite.Error as db_err: # Catch database errors specifically
                     logger.error(f"Database error in message loop for group {group_id}: {db_err}")
                     retry_count += 1 # Increment here
                     if retry_count >= max_retries:
                         logger.error(f"Max retries reached for group {group_id} due to DB error. Initiating cleanup.")
                         await self.cleanup_group(group_id, f"DB Error after max retries: {db_err}")
                         return # Exit the task
                     await asyncio.sleep(5 * retry_count) # Sleep here

                except Exception as e: # Catch other unexpected errors
                    logger.error(f"Unexpected error in message loop for group {group_id}: {e}", exc_info=True)
                    retry_count += 1 # Increment here
                    if retry_count >= max_retries:
                        logger.error(f"Max retries reached for group {group_id} due to unexpected error. Initiating cleanup.")
                        await self.cleanup_group(group_id, f"Unexpected error after max retries: {e}")
                        return # Exit the task
                    await asyncio.sleep(5 * retry_count) # Sleep here

            # Add except block for the outer try (line 178)
            except Exception as outer_e:
                logger.error(f"Critical error in outer message loop for group {group_id}: {outer_e}", exc_info=True)
                # Attempt cleanup on any outer loop error
                await self.cleanup_group(group_id, f"Outer loop error: {outer_e}")
                break # Exit the while loop

    async def cleanup_group(self, group_id: str, reason: str):
        """Cleanup all resources for a group in the correct order."""
        # Ensure this function is idempotent and handles potential errors gracefully
        logger.info(f"Starting cleanup for group {group_id}. Reason: {reason}")
        try:
            # 1. Cancel and remove the asyncio task
            if group_id in self.tasks:
                task = self.tasks.pop(group_id) # Remove from dict immediately
                if not task.done():
                    task.cancel()
                    try:
                        await task # Allow cancellation to propagate
                    except asyncio.CancelledError:
                        logger.debug(f"Task for group {group_id} cancelled successfully.")
                    except Exception as e_cancel:
                        # Log error but continue cleanup
                        logger.error(f"Error awaiting cancelled task for group {group_id}: {e_cancel}")
            else:
                 logger.debug(f"No active task found for group {group_id} during cleanup.")

            # 2. Mark the group as inactive in the database
            try:
                await update_group_status(group_id, False) # Added await
            except Exception as e_status:
                 # Log error but proceed to remove group data
                 logger.error(f"Error setting group {group_id} inactive during cleanup: {e_status}")

            # 3. Remove the group data from the database ONLY if it's not a manual removal
            if reason != "Manual removal": # Match the default reason from remove_scheduled_job
                try:
                    await remove_group(group_id) # Added await
                except Exception as e_remove:
                    # Log error, but cleanup is mostly done
                    logger.error(f"Error removing group {group_id} data during cleanup: {e_remove}")
            else:
                logger.info(f"Skipping database removal for group {group_id} due to manual stop.")

            logger.info(f"Finished cleanup for group {group_id}.")
        except Exception as e:
            logger.error(f"Error during cleanup for group {group_id}: {e}")

    async def handle_group_migration(self, bot, old_group_id: str, new_group_id: str):
        """Handle group migration by updating group ID."""
        # This function needs careful handling of async operations and state
        try:
            logger.info(f"Starting migration: group {old_group_id} → {new_group_id}")

            # 1. Get old group data (asynchronously)
            group_data = await get_group(old_group_id) # Added await
            if not group_data:
                logger.warning(f"Group {old_group_id} not found in DB, cannot migrate.")
                return # Nothing to migrate

            # 2. Cancel and remove the old task (if running)
            if old_group_id in self.tasks:
                task = self.tasks.pop(old_group_id) # Remove from dict
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        logger.debug(f"Cancelled task for old group {old_group_id} during migration.")
                    except Exception as e_cancel:
                        logger.error(f"Error awaiting cancelled task for {old_group_id} during migration: {e_cancel}")
            else:
                logger.debug(f"No active task found for old group {old_group_id} during migration.")

            # 3. Remove old group data from DB (asynchronously)
            try:
                await remove_group(old_group_id) # Added await
            except Exception as e_remove:
                 # Log error, but attempt to continue migration
                 logger.error(f"Error removing old group {old_group_id} data during migration: {e_remove}")

            # 4. Add new group data to DB (asynchronously)
            new_group_id_str = str(new_group_id)
            try:
                await add_group(new_group_id_str, group_data["name"]) # Added await
                # Update message/status for the *new* group ID
                # Ensure next_schedule is a datetime object before passing
                next_schedule_dt = group_data.get("next_schedule") # Should be datetime from get_group
                await update_group_message(new_group_id_str, group_data.get("last_msg_id"), next_schedule_dt) # Added await
                await update_group_status(new_group_id_str, group_data.get("active", False)) # Added await
            except Exception as e_add:
                 logger.error(f"Error adding/updating new group {new_group_id_str} during migration: {e_add}")
                 # If adding the new group fails, the migration is effectively failed.
                 # Consider if cleanup of the new group ID is needed if partially added.
                 return # Stop migration if new group setup fails

            # 5. Schedule message for the new group ID (if it was active)
            if group_data.get("active", False):
                logger.info(f"Scheduling message loop for migrated group {new_group_id_str}")
                # Fetch potentially updated global settings before scheduling
                global_settings = await get_global_settings()
                await self.schedule_message(
                    bot,
                    new_group_id_str,
                    message_reference=global_settings.get("message_reference"),
                    delay=global_settings.get("delay")
                )
            else:
                 logger.info(f"Old group {old_group_id} was inactive, not scheduling loop for new group {new_group_id_str}.")


            logger.info(f"Group migration {old_group_id} → {new_group_id_str} completed.")

        except Exception as e:
            logger.error(f"Unexpected error during group migration {old_group_id} → {new_group_id}: {e}", exc_info=True)
            # Attempt cleanup of old group ID if migration failed mid-way
            try:
                 await self.cleanup_group(old_group_id, f"Migration failure: {e}")
            except Exception as e_cleanup:
                 logger.error(f"Error during cleanup after migration failure for {old_group_id}: {e_cleanup}")

    async def update_running_tasks(self, bot, new_message_reference: Optional[dict] = None, new_delay: Optional[int] = None):
        """Update all running tasks with new settings asynchronously."""
        updated_count = 0
        try:
            # Fetch global settings asynchronously first
            settings = await get_global_settings()
            effective_message_ref = new_message_reference if new_message_reference is not None else settings.get("message_reference")
            effective_delay = new_delay if new_delay is not None else settings.get("delay")

            if not effective_message_ref:
                 logger.error("Cannot update running tasks: No message reference available.")
                 return 0
            if effective_delay is None:
                 logger.error("Cannot update running tasks: No delay available.")
                 return 0

            tasks_to_update = []
            group_ids_to_update = []

            # Iterate through a copy of the tasks dictionary keys
            for group_id in list(self.tasks.keys()):
                task = self.tasks.get(group_id) # Get task safely
                if task and not task.done():
                     # Check group status asynchronously *before* deciding to update
                     group_data = await get_group(group_id)
                     if group_data and group_data.get("active", False):
                         # Schedule the update task (schedule_message handles cancelling old task)
                         tasks_to_update.append(
                             self.schedule_message(
                                 bot,
                                 group_id,
                                 message_reference=effective_message_ref,
                                 delay=effective_delay
                             )
                         )
                         group_ids_to_update.append(group_id) # Keep track for logging

            # Run updates concurrently
            if tasks_to_update:
                 results = await asyncio.gather(*tasks_to_update, return_exceptions=True)
                 for i, result in enumerate(results):
                     group_id = group_ids_to_update[i]
                     if isinstance(result, Exception):
                         logger.error(f"Failed to update task for group {group_id}: {result}")
                     elif result is True: # Assuming schedule_message returns True on success
                         updated_count += 1
                         # logger.info(f"Updated task for group {group_id} with new settings") # schedule_message already logs this
                     else:
                          logger.warning(f"schedule_message returned False for group {group_id} during update.")


            return updated_count

        except aiosqlite.Error as db_err:
             logger.error(f"Database error during update_running_tasks: {db_err}")
             return updated_count # Return count of successful updates before error
        except Exception as e:
            logger.error(f"Failed to update running tasks: {e}", exc_info=True)
            return updated_count # Return count of successful updates before error


    def is_running(self, group_id: str) -> bool:
        return group_id in self.tasks and not self.tasks[group_id].done()

    def get_active_tasks(self) -> int:
        return len([task for task in self.tasks.values() if not task.done()])

    async def start(self):
        """Initialize scheduler and recover active tasks asynchronously."""
        try:
            # Use async context manager and await async functions
            async with get_db_connection() as conn: # Use async with
                # No need for explicit cursor if utils handle it
                settings = await get_global_settings() # Added await
                current_time = datetime.now(pytz.UTC)
                # Load data asynchronously
                all_data = await load_data() # Added await
                groups_data = all_data.get("groups", {})

                if not settings.get("message_reference"):
                     logger.warning("Scheduler start: Global message reference not set. Cannot recover tasks.")
                     return # Cannot proceed without a message to send

                if settings.get("delay") is None:
                     logger.warning("Scheduler start: Global delay not set. Cannot recover tasks.")
                     return # Cannot proceed without a delay

                tasks_to_update_db = []
                for group_id, group in groups_data.items():
                    if group.get("active", False):
                        # next_schedule should be datetime object from load_data
                        next_schedule_dt = group.get("next_schedule")
                        # Calculate next time based on potentially recovered datetime
                        # Pass datetime object directly to calculate_next_schedule if available
                        next_time = self.calculate_next_schedule(current_time, next_schedule_dt, settings["delay"])

                        # Prepare DB update task
                        tasks_to_update_db.append(
                            update_group_message(group_id, group.get("last_msg_id"), next_time)
                        )
                        logger.info(f"Recovered schedule for group {group_id} - Next: {next_time.isoformat()}")
                        self.pending_groups[group_id] = {
                            "message_reference": settings.get("message_reference"),
                            "delay": settings["delay"],
                            "next_time": next_time # Store calculated datetime
                        }

                # Run DB updates concurrently
                if tasks_to_update_db:
                     results = await asyncio.gather(*tasks_to_update_db, return_exceptions=True)
                     failed_updates = [res for res in results if isinstance(res, Exception)]
                     if failed_updates:
                          logger.error(f"Encountered {len(failed_updates)} errors updating group schedules during recovery.")
                          # Decide how to handle partial failures - maybe log group IDs?

                # Commit happens within update_group_message now
                logger.info(f"Scheduler initialized - {len(self.pending_groups)} active groups pending task creation.")
        except aiosqlite.Error as db_err:
             logger.error(f"Database error during scheduler start: {db_err}")
             # Depending on severity, might want to exit or continue without recovery
        except Exception as e:
            logger.error(f"Scheduler start failed: {e}", exc_info=True) # Log traceback

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
    return True # Indicate success

async def start_scheduler():
    await scheduler.start()

async def stop_scheduler():
    await scheduler.shutdown()

def is_running(group_id: str) -> bool:
    return scheduler.is_running(group_id)

def get_active_tasks_count() -> int:
    return scheduler.get_active_tasks()