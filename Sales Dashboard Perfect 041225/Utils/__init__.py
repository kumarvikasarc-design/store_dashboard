# utils/__init__.py
# Clean public exports for utils package

# ------------------------------
# DATA LOADER
# ------------------------------
from data_loader import (
    load_item_and_summary_sales,
    load_variance_cogs,
    load_expiry,
    load_recipes,
    load_store_mapping,
)

# ------------------------------
# AUTH & USER MANAGEMENT
# ------------------------------
from auth import (
    authenticate_user,
    generate_token,
    verify_token,
    create_user,
    delete_user,
    set_user_active,
    update_user_role,
    get_users_for_display,
)

# ------------------------------
# EXPORT UTILITIES
# ------------------------------
from export_excel import df_to_excel_bytes
from export_pdf import df_to_pdf_bytes

# ------------------------------
# EMAIL UTILITIES
# ------------------------------
from email_sender import send_report_email

# Define what is available when importing * from the package
__all__ = [
    # Data loader
    "load_item_and_summary_sales",
    "load_variance_cogs",
    "load_expiry",
    "load_recipes",
    "load_store_mapping",

    # Auth system
    "authenticate_user",
    "generate_token",
    "verify_token",
    "create_user",
    "delete_user",
    "set_user_active",
    "update_user_role",
    "get_users_for_display",

    # Export utilities
    "df_to_excel_bytes",
    "df_to_pdf_bytes",

    # Email
    "send_report_email",
]