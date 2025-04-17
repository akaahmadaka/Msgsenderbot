import os 

BOT_TOKEN = os.environ.get('BOT_TOKEN', "")
admin_ids_str = os.environ.get('ADMIN_IDS', '')
ADMIN_IDS = [int(admin_id.strip()) for admin_id in admin_ids_str.split(',') if admin_id.strip().isdigit()]
DEEP_LINK_TEMPLATE = "t.me/{bot_username}?startgroup=getvideo"
WELCOME_MSG = "👋 Add me in group and send /getvideo\nOr click here 👉 {deep_link}"
GLOBAL_DELAY = 30  # in seconds

LOG_LEVEL = "INFO"
