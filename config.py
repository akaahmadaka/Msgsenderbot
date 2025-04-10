import os # Import the os module

# Read BOT_TOKEN from environment variable for deployment
# Fallback to an empty string for local testing if the env var isn't set
BOT_TOKEN = os.environ.get('BOT_TOKEN', "")

# TODO: Move ADMIN_IDS to environment variable (e.g., os.environ.get('ADMIN_IDS', '').split(','))
# Ensure the environment variable is a comma-separated list of integers.
admin_ids_str = os.environ.get('ADMIN_IDS', '')
ADMIN_IDS = [int(admin_id.strip()) for admin_id in admin_ids_str.split(',') if admin_id.strip().isdigit()]
DEEP_LINK_TEMPLATE = "t.me/{bot_username}?startgroup=startloop"
WELCOME_MSG = "👋 Add me in group and send /startloop\nOr click here 👉 {deep_link}"
GLOBAL_DELAY = 30  # in seconds

LOG_LEVEL = "INFO"