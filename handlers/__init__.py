from handlers.admin import register_admin_handlers
from handlers.callbacks import register_callback_handlers
from handlers.common import register_common_handlers
from handlers.user import register_user_handlers


def register_all_handlers(dp):
    register_common_handlers(dp)
    register_user_handlers(dp)
    register_admin_handlers(dp)
    register_callback_handlers(dp)

