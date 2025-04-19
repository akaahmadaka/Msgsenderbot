import asyncio
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional
import pytz
from telegram import InlineKeyboardButton, InlineKeyboardMarkup # Added imports
from telegram.error import (
    Forbidden, BadRequest, NetworkError, ChatMigrated, RetryAfter
)
from db import get_db_connection
from utils import (
    get_group,
    remove_group,
    get_global_settings, update_group_status, add_group,
    load_data, update_group_retry_count, get_global_messages,
    update_group_after_send
)

if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

import logging
from logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


class MessageScheduler:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.pending_groups: Dict[str, dict] = {}
        logger.info("Scheduler ready")

    def calculate_next_schedule(self, current_time: datetime, next_schedule_str: Optional[str], delay: int) -> datetime:
        """Calculate the appropriate next schedule time, handling recovery."""
        try:
            next_schedule = datetime.fromisoformat(next_schedule_str.replace('Z', '+00:00'))
            if next_schedule.tzinfo is None:
                next_schedule = pytz.utc.localize(next_schedule)

            if next_schedule > current_time:
                return next_schedule
            else:
                time_diff_seconds = (current_time - next_schedule).total_seconds()
                intervals_missed = int(time_diff_seconds // delay)
                actual_next_time = next_schedule + timedelta(seconds=(intervals_missed + 1) * delay)
                min_next_time = current_time + timedelta(seconds=1)
                return max(actual_next_time, min_next_time)

        except (ValueError, TypeError, AttributeError):
            logger.warning(f"Invalid next_schedule_str '{next_schedule_str}', defaulting to current_time + delay.")
            return current_time + timedelta(seconds=delay)

    async def schedule_message(
        self,
        bot,
        group_id: str,
        delay: Optional[int] = None,
        existing_next_schedule: Optional[datetime] = None,
        is_update_restart: bool = False
    ):
        """Schedule messages for a group, optionally preserving next schedule time."""
        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with get_db_connection() as conn:
                        settings = await get_global_settings()

                        delay_val = delay if delay is not None else settings["delay"]
                        if delay_val is None:
                             logger.error(f"No delay value found for group {group_id}")
                             return False

                        current_time = datetime.now(pytz.UTC)
                        if existing_next_schedule and existing_next_schedule > current_time:
                            next_time = existing_next_schedule
                            logger.debug(f"Using existing next schedule for group {group_id}: {next_time}")
                        else:
                            # If existing schedule is in the past or not provided, start "now" (or after a minimal delay if needed)
                            # For simplicity, we set next_time to current_time, the loop logic handles the first immediate run.
                            next_time = current_time
                            logger.debug(f"Calculating new next schedule for group {group_id} based on current time.")

                        group_data = await get_group(group_id)
                        await update_group_status(group_id, True)
                        await update_group_retry_count(group_id, 0)

                        if group_id in self.tasks and not self.tasks[group_id].done():
                            self.tasks[group_id].cancel()
                            try:
                                await self.tasks[group_id]
                            except asyncio.CancelledError:
                                logger.debug(f"Cancelled existing task for group {group_id} before rescheduling.")
                            except Exception as e_cancel:
                                logger.error(f"Error cancelling existing task for {group_id}: {e_cancel}")

                        self.tasks[group_id] = asyncio.create_task(
                            self._message_loop(bot, group_id, delay_val, is_update_restart=is_update_restart)
                        )
                        logger.info(f"Started/Updated message loop for group {group_id}")
                        return True
                except aiosqlite.OperationalError as e:
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        wait_time = 0.1 * (attempt + 1)
                        logger.warning(f"Database locked on schedule_message, retrying in {wait_time}s... (Attempt {attempt + 1})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                         logger.error(f"Database operational error scheduling for group {group_id} after {attempt + 1} attempts: {e}")
                         return False
                except Exception as e:
                     logger.error(f"Unexpected error scheduling messages for group {group_id}: {e}")
                     return False

            logger.error(f"Failed to schedule message for group {group_id} after {max_retries} attempts.")
            return False

        except Exception as e:
           logger.error(f"Failed to schedule messages for group {group_id}: {e}")
           raise

    async def _send_and_delete_message(self, bot, group_id: str, group_name: str, message_reference: dict, group_data: dict, message_index: int):
        """Send the message (with button if it's the first one) and delete the previous one. Handles fatal errors."""
        FATAL_ERRORS = [
            "chat not found",
            "bot was kicked",
            "user_is_blocked",
            "peer_id_invalid",
        ]
        try:
            logger.debug(f'Attempting to send message index {message_index} to group: {group_name} ({group_id})')

            reply_markup = None
            if message_index == 0: # Only add button to the first message
                # Make sure bot_username is available or fetch it if needed
                # For simplicity, assuming the deep link doesn't need the bot username dynamically here
                # but ideally it should be fetched or passed in.
                # Using a placeholder URL for now, needs adjustment in handlers.py later
                # Corrected: use a callback_data instead of a URL for tracking internal clicks
                button = InlineKeyboardButton("Get Videos", callback_data=f"get_videos_click_{group_id}")
                reply_markup = InlineKeyboardMarkup([[button]])

            sent_message = await bot.copy_message(
                chat_id=int(group_id),
                from_chat_id=message_reference["chat_id"],
                message_id=message_reference["message_id"],
                reply_markup=reply_markup # Pass the button if applicable
            )

            last_msg_id = group_data.get("last_msg_id")
            if last_msg_id:
                try:
                    logger.debug(f'Attempting delete of msg {last_msg_id} in group: {group_name} ({group_id})')
                    await bot.delete_message(int(group_id), last_msg_id)
                except Exception as e_del:
                    logger.warning(f"Failed to delete previous message {last_msg_id} in group {group_name} ({group_id}): {e_del}")
            return sent_message

        except (Forbidden, BadRequest, NetworkError, ChatMigrated, RetryAfter) as e:
            error_msg = str(e).lower()
            logger.error(f"Telegram API error in group {group_name} ({group_id}): {str(e)}")

            if any(fatal_msg in error_msg for fatal_msg in FATAL_ERRORS):
                logger.warning(f"Fatal Telegram error for group {group_name} ({group_id}), initiating cleanup: {str(e)}")
                asyncio.create_task(self.cleanup_group(bot, group_id, f"Fatal Telegram Error: {str(e)}"))
                return None

            elif isinstance(e, ChatMigrated):
                 new_chat_id = e.new_chat_id
                 logger.info(f"Group {group_name} ({group_id}) migrated to supergroup {new_chat_id}. Handling migration.")
                 asyncio.create_task(self.handle_group_migration(bot, group_id, str(new_chat_id)))
                 return None

            else:
                 # Re-raise non-fatal, non-migration errors for retry logic
                 raise e

        except Exception as e:
             logger.error(f"Unexpected error during send/delete for group {group_name} ({group_id}): {e}", exc_info=True)
             raise e


    async def _message_loop(self, bot, group_id: str, delay: int, is_update_restart: bool = False):
        """Message loop handling retries, fatal errors, and cleanup."""
        MAX_MESSAGE_RETRIES = 3
        is_initial_run = not is_update_restart

        while True:
            group_name = f"ID:{group_id}"
            try:
                group_data = await get_group(group_id)
                if not group_data:
                    logger.warning(f"Group {group_id} not found in DB during loop. Stopping task.")
                    if group_id in self.tasks: del self.tasks[group_id]
                    return

                group_name = group_data.get("name", group_name)

                if not group_data.get("active"):
                    logger.info(f"Loop stopping for group {group_name} ({group_id}) - Marked inactive in DB.")
                    if group_id in self.tasks: del self.tasks[group_id]
                    return

                current_retry_count = group_data.get("retry_count", 0)

                # --- Wait Logic ---
                # Determine if we should run immediately (only on the very first run after initial schedule)
                run_immediately = is_initial_run

                if not run_immediately:
                    current_time = datetime.now(pytz.UTC)
                    next_schedule_dt = group_data.get("next_schedule")
                    next_time = self.calculate_next_schedule(current_time, next_schedule_dt.isoformat() if next_schedule_dt else None, delay)
                    wait_time = (next_time - current_time).total_seconds()

                    if wait_time > 0:
                        logger.debug(f"Group {group_name} ({group_id}): Waiting {wait_time:.2f} seconds...")
                        await asyncio.sleep(wait_time)
                # After the first iteration (whether it waited or ran immediately), subsequent runs should always wait.
                is_initial_run = False

                # --- Fetch Messages & Select ---
                global_messages = await get_global_messages()
                if not global_messages:
                    logger.warning(f"No global messages set. Pausing loop for group {group_name} ({group_id}). Will check again in {delay}s.")
                    await asyncio.sleep(delay)
                    continue

                current_message_index = group_data.get("current_message_index", 0)
                num_messages = len(global_messages)
                index_to_use = current_message_index % num_messages
                message_reference_to_send = global_messages[index_to_use]

                # --- Send/Delete Logic ---
                try:
                    async with timeout(45):
                        sent_message = await self._send_and_delete_message(
                            bot, group_id, group_name, message_reference_to_send, group_data, index_to_use
                        )

                        if sent_message is None:
                            logger.warning(f"Exiting loop for group {group_name} ({group_id}) due to fatal error during send/delete.")
                            return

                        # --- Success Case ---
                        logger.info(f"Message (Index {index_to_use}) sent successfully to {group_name} ({group_id}). Msg ID: {sent_message.message_id}")

                        if current_retry_count > 0:
                            await update_group_retry_count(group_id, 0)
                            logger.info(f"Reset retry count for group {group_name} ({group_id}).")

                        next_message_index = (current_message_index + 1) % num_messages
                        next_time_update = datetime.now(pytz.UTC) + timedelta(seconds=delay)
                        await update_group_after_send(group_id, sent_message.message_id, next_message_index, next_time_update)

                # --- Retryable Error Handling ---
                except (asyncio.TimeoutError, NetworkError, RetryAfter, Forbidden, BadRequest) as e:
                    current_retry_count += 1
                    logger.warning(f"Retryable error for group {group_name} ({group_id}) (Attempt {current_retry_count}/{MAX_MESSAGE_RETRIES}): {e}")

                    try:
                        await update_group_retry_count(group_id, current_retry_count)
                    except Exception as db_e:
                        logger.error(f"Failed to update retry count for {group_name} ({group_id}) after error: {db_e}")

                    if current_retry_count >= MAX_MESSAGE_RETRIES:
                        logger.error(f"Max retries ({MAX_MESSAGE_RETRIES}) reached for group {group_name} ({group_id}). Error: {e}. Initiating leave and cleanup.")
                        try:
                            await bot.leave_chat(int(group_id))
                            logger.info(f"Successfully left group {group_name} ({group_id}) after max retries.")
                        except Exception as leave_e:
                            logger.error(f"Failed to leave group {group_name} ({group_id}) after max retries: {leave_e}")
                        await self.cleanup_group(bot, group_id, f"Max retries reached (leave attempted): {e}")
                        return
                    else:
                        logger.info(f"Will retry for group {group_name} ({group_id}) on next schedule.")
                        pass

                except aiosqlite.Error as db_err:
                     logger.error(f"Database error during message loop for group {group_name} ({group_id}): {db_err}")
                     await asyncio.sleep(5) # Short sleep before trying again

                except Exception as e:
                    current_retry_count += 1
                    logger.error(f"Unexpected error in loop for group {group_name} ({group_id}) (Attempt {current_retry_count}/{MAX_MESSAGE_RETRIES}): {e}", exc_info=True)
                    try:
                        await update_group_retry_count(group_id, current_retry_count)
                    except Exception as db_e:
                         logger.error(f"Failed to update retry count for {group_name} ({group_id}) after unexpected error: {db_e}")

                    if current_retry_count >= MAX_MESSAGE_RETRIES:
                        logger.error(f"Max retries ({MAX_MESSAGE_RETRIES}) reached for group {group_name} ({group_id}) due to unexpected error. Initiating leave and cleanup.")
                        try:
                            await bot.leave_chat(int(group_id))
                            logger.info(f"Successfully left group {group_name} ({group_id}) after max retries (unexpected error).")
                        except Exception as leave_e:
                            logger.error(f"Failed to leave group {group_name} ({group_id}) after max retries (unexpected error): {leave_e}")
                        await self.cleanup_group(bot, group_id, f"Max retries reached (unexpected, leave attempted): {e}")
                        return
                    else:
                        logger.info(f"Will retry for group {group_name} ({group_id}) on next schedule after unexpected error.")
                        pass

            # --- Outer Loop Error Handling ---
            except Exception as outer_e:
                logger.error(f"Critical error in outer message loop for group {group_name} ({group_id}): {outer_e}", exc_info=True)
                await self.cleanup_group(bot, group_id, f"Outer loop error: {outer_e}")
                return

    async def cleanup_group(self, bot, group_id: str, reason: str):
        """Cleanup resources, potentially leaving the chat first."""
        group_name = f"ID:{group_id}"
        try:
            group_data = await get_group(group_id)
            if group_data:
                group_name = group_data.get("name", group_name)
        except Exception as e_get:
            logger.warning(f"Could not fetch group name for {group_id} during cleanup: {e_get}")

        logger.info(f"Starting cleanup for group {group_name} ({group_id}). Reason: {reason}")
        try:
            # Attempt to leave chat only if cleanup is due to errors (not manual stop)
            if ("Max retries reached" in reason or "Fatal Telegram Error" in reason) and "leave attempted" not in reason:
                try:
                    logger.info(f"Attempting to leave group {group_name} ({group_id})...")
                    await bot.leave_chat(int(group_id))
                    logger.info(f"Successfully left group {group_name} ({group_id}).")
                except Exception as leave_e:
                    logger.error(f"Failed to leave group {group_name} ({group_id}): {leave_e}")

            if group_id in self.tasks:
                task = self.tasks.pop(group_id)
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        logger.debug(f"Task for group {group_id} cancelled successfully.")
                    except Exception as e_cancel:
                        logger.error(f"Error awaiting cancelled task for group {group_id}: {e_cancel}")
            else:
                 logger.debug(f"No active task found for group {group_name} ({group_id}) during cleanup.")

            try:
                await update_group_status(group_id, False)
            except Exception as e_status:
                 logger.error(f"Error setting group {group_name} ({group_id}) inactive during cleanup: {e_status}")

            # Remove group data only if cleanup is due to errors (not manual stop)
            if reason != "Manual removal":
                try:
                    await remove_group(group_id)
                except Exception as e_remove:
                    logger.error(f"Error removing group {group_name} ({group_id}) data during cleanup: {e_remove}")
            else:
                 logger.info(f"Skipping database removal for group {group_name} ({group_id}) due to manual stop.")

            logger.info(f"Finished cleanup for group {group_name} ({group_id}).")
            return True
        except Exception as e:
            logger.error(f"Error during cleanup for group {group_name} ({group_id}): {e}")
            return False

    async def handle_group_migration(self, bot, old_group_id: str, new_group_id: str):
        """Handle group migration by updating group ID."""
        try:
            logger.info(f"Starting migration: group {old_group_id} → {new_group_id}")

            group_data = await get_group(old_group_id)
            if not group_data:
                logger.warning(f"Group {old_group_id} not found in DB, cannot migrate.")
                return

            if old_group_id in self.tasks:
                task = self.tasks.pop(old_group_id)
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

            try:
                await remove_group(old_group_id)
            except Exception as e_remove:
                 logger.error(f"Error removing old group {old_group_id} data during migration: {e_remove}")

            new_group_id_str = str(new_group_id)
            try:
                await add_group(new_group_id_str, group_data["name"])
                next_schedule_dt = group_data.get("next_schedule")
                # Use update_group_after_send, passing the current index from the old group data
                await update_group_after_send(
                    new_group_id_str,
                    group_data.get("last_msg_id"),
                    group_data.get("current_message_index", 0), # Use old index, default to 0
                    next_schedule_dt
                )
                await update_group_status(new_group_id_str, group_data.get("active", False))
                await update_group_retry_count(new_group_id_str, 0)
            except Exception as e_add:
                 logger.error(f"Error adding/updating new group {new_group_id_str} during migration: {e_add}")
                 return

            if group_data.get("active", False):
                logger.info(f"Scheduling message loop for migrated group {new_group_id_str}")
                global_settings = await get_global_settings()
                await self.schedule_message(
                    bot,
                    new_group_id_str,
                    delay=global_settings.get("delay")
                )
            else:
                 logger.info(f"Old group {old_group_id} was inactive, not scheduling loop for new group {new_group_id_str}.")

            logger.info(f"Group migration {old_group_id} → {new_group_id_str} completed.")

        except Exception as e:
            logger.error(f"Unexpected error during group migration {old_group_id} → {new_group_id}: {e}", exc_info=True)
            # Attempt cleanup of old group ID if migration failed mid-way
            try:
                 await self.cleanup_group(bot, old_group_id, f"Migration failure: {e}")
            except Exception as e_cleanup:
                 logger.error(f"Error during cleanup after migration failure for {old_group_id}: {e_cleanup}")

    async def update_running_tasks(self, bot, new_delay: Optional[int] = None):
        """Update all running tasks with new settings asynchronously."""
        updated_count = 0
        try:
            settings = await get_global_settings()
            effective_delay = new_delay if new_delay is not None else settings.get("delay")

            if effective_delay is None:
                 logger.error("Cannot update running tasks: No delay available (neither new nor existing).")
                 return 0

            tasks_to_update = []
            group_ids_to_update = []

            for group_id in list(self.tasks.keys()):
                task = self.tasks.get(group_id)
                if task and not task.done():
                     group_data = await get_group(group_id)
                     if group_data and group_data.get("active", False):
                         current_next_schedule = group_data.get("next_schedule")

                         tasks_to_update.append(
                             self.schedule_message(
                                 bot,
                                 group_id,
                                 delay=effective_delay,
                                 existing_next_schedule=current_next_schedule,
                                 is_update_restart=True
                             )
                         )
                         group_ids_to_update.append(group_id)

            if tasks_to_update:
                 results = await asyncio.gather(*tasks_to_update, return_exceptions=True)
                 for i, result in enumerate(results):
                     group_id = group_ids_to_update[i]
                     if isinstance(result, Exception):
                         logger.error(f"Failed to update task for group {group_id}: {result}")
                     elif result is True:
                         updated_count += 1
                     else:
                          logger.warning(f"schedule_message returned False for group {group_id} during update.")

            return updated_count

        except aiosqlite.Error as db_err:
             logger.error(f"Database error during update_running_tasks: {db_err}")
             return updated_count
        except Exception as e:
            logger.error(f"Failed to update running tasks: {e}", exc_info=True)
            return updated_count


    def is_running(self, group_id: str) -> bool:
        """Check if a task is currently running for the given group ID."""
        return group_id in self.tasks and not self.tasks[group_id].done()

    def get_active_tasks(self) -> int:
        """Get the count of currently active (not done) tasks."""
        return len([task for task in self.tasks.values() if not task.done()])

    async def start(self):
        """Initialize scheduler and recover active tasks asynchronously."""
        try:
            async with get_db_connection() as conn:
                settings = await get_global_settings()
                current_time = datetime.now(pytz.UTC)
                all_data = await load_data()
                groups_data = all_data.get("groups", {})


                if settings.get("delay") is None:
                     logger.warning("Scheduler start: Global delay not set. Cannot recover tasks.")
                     return

                tasks_to_update_db = []
                for group_id, group in groups_data.items():
                    if group.get("active"):
                        next_schedule_dt = group.get("next_schedule")
                        next_time = self.calculate_next_schedule(current_time, next_schedule_dt.isoformat() if next_schedule_dt else None, settings["delay"])

                        # tasks_to_update_db.append( # Commenting out the append call itself
                            # This seems incorrect, update_group_message was removed.
                            # Should likely be update_group_after_send, but that requires more info (next_index).
                            # Recovery logic might need rethink if we want to preserve exact state.
                            # For now, let's comment this out as the loop will set the next schedule anyway.
                            # update_group_status(group_id, True) # Maybe just ensure active?
                        # )
                        logger.info(f"Marking group {group_id} for task recovery - Next approx: {next_time.isoformat()}")
                        self.pending_groups[group_id] = {
                            "delay": settings["delay"],
                            "next_time": next_time
                        }

                if tasks_to_update_db:
                     results = await asyncio.gather(*tasks_to_update_db, return_exceptions=True)
                     failed_updates = [res for res in results if isinstance(res, Exception)]
                     if failed_updates:
                          logger.error(f"Encountered {len(failed_updates)} errors updating group schedules during recovery.")

                logger.info(f"Scheduler initialized - {len(self.pending_groups)} active groups pending task creation.")
        except aiosqlite.Error as db_err:
             logger.error(f"Database error during scheduler start: {db_err}")
        except Exception as e:
            logger.error(f"Scheduler start failed: {e}", exc_info=True)

    async def initialize_pending_tasks(self, bot):
        """Initialize tasks for recovered groups with bot instance."""
        try:
            for group_id, settings in self.pending_groups.items():
                next_time = settings["next_time"]
                current_time = datetime.now(pytz.UTC)
                wait_time = (next_time - current_time).total_seconds()

                # Always treat recovered tasks as needing to respect the calculated next_time
                # Use _delayed_message_loop if wait_time > 0, otherwise start _message_loop immediately
                # but ensure _message_loop knows it's not the absolute first run (is_update_restart=True might work here)
                if wait_time > 0:
                    self.tasks[group_id] = asyncio.create_task(
                        self._delayed_message_loop(
                            bot,
                            group_id,
                            delay=settings["delay"],
                            initial_delay=wait_time
                        )
                    )
                    logger.info(f"Created delayed task for recovered group {group_id} - Wait time: {wait_time:.1f}s")
                else:
                    # Start immediately but treat it like an update restart so it waits for the *next* cycle
                    self.tasks[group_id] = asyncio.create_task(
                        self._message_loop(
                            bot,
                            group_id,
                            # settings["message_reference"], # No longer needed
                            settings["delay"],
                            is_update_restart=True
                        )
                    )
                    logger.info(f"Created immediate task for recovered group {group_id} (next cycle will wait).")


            started_count = len(self.pending_groups)
            self.pending_groups.clear()
            return started_count
        except Exception as e:
            logger.error(f"Failed to initialize pending tasks: {e}")
            return 0

    async def _delayed_message_loop(self, bot, group_id: str, delay: int, initial_delay: float): # Removed message_reference
        """Message loop with initial delay for recovered tasks."""
        try:
            await asyncio.sleep(initial_delay)
            # Start the main loop, indicating it's like an update restart so it waits for the *next* cycle
            # await self._message_loop(bot, group_id, message_reference, delay, is_update_restart=True) # Old call
            await self._message_loop(bot, group_id, delay, is_update_restart=True)
        except Exception as e:
            logger.error(f"Delayed message loop failed for group {group_id}: {e}")

    async def shutdown(self, bot):
        """Gracefully shutdown the scheduler, cancelling running tasks."""
        try:
            task_count = len(self.tasks)
            logger.info(f"Scheduler shutdown initiated. Cancelling {task_count} tasks...")
            tasks_to_cancel = []
            for group_id, task in self.tasks.items():
                 if not task.done():
                      task.cancel()
                      tasks_to_cancel.append(task)

            if tasks_to_cancel:
                 # Wait for cancellations to complete
                 await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
                 logger.debug(f"Finished awaiting cancellation for {len(tasks_to_cancel)} tasks.")

            self.tasks.clear()
            logger.info(f"Scheduler stopped - {task_count} tasks processed for cancellation.")
        except Exception as e:
            logger.error(f"Scheduler shutdown failed: {e}")

scheduler = MessageScheduler()
