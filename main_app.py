"""
main_app.py - Unified multi-dashboard container (clean + security features)

Notes:
- Uses utils/auth.py for authentication
- Adds Reset Password (security question), Change Password in Profile
- Admin-only sign-up via admin user management
- Admin page for Login Logs + "Run Daily Job Now"
"""

import os
import sys
import importlib
import numpy as np
from datetime import datetime
from PIL import Image
import dash
from dash import html, dcc, Input, Output, State, no_update
from dash import dash_table
import dash_bootstrap_components as dbc
from dash import page_container
from flask import Flask
from waitress import serve

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# make project root importable
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Pages (your existing pages)
from pages import dsr_page, daypart_page, sales_page, feedback_page, warehouse_dashboard, menu_mix, item_activity_page
#importlib.reload(warehouse_dashboard)
#print("Functions in warehouse_dashboard:", dir(warehouse_dashboard))
from components.footer import footer

# Auth utils
from utils.auth import (
    authenticate_user,
    generate_token,
    verify_token,
    create_user,
    get_users_for_display,
    set_user_active,
    delete_user,
    update_user_role,
    get_security_question,
    reset_password_with_answer,
    change_password,
    get_login_logs,
    get_recent_login_activity,
    is_admin_user,
)

# Try to import run_now from daily_reports_job (optional)
try:
    from utils.daily_reports_job import run_now as run_daily_job_now
except Exception:
    run_daily_job_now = None


# role mapping
ROLE_PAGES = {
    "admin": {
        "dsr", "daypart", "sales", "feedback",
        "warehouse", "menu-mix",
        "item-activity",
        "admin-users", "admin-logs"
    },
    "manager": {
        "dsr", "daypart", "sales", "menu-mix",
        "item-activity",
        "warehouse"
    },
    "viewer": {"dsr", "sales", "daypart", 
            "item-activity",
               "menu-mix"},
}

app = dash.Dash(
    __name__,
    use_pages=False,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    eager_loading=False,   # <--- SEE ALL CALLBACK ERRORS
)
app.config.suppress_callback_exceptions = True

server = app.server
app.title = "Coffee Island – Unified Analytics (Secure)"
style={"display": "none"}
# ---------------------------
# Login layout (with forgot / signup)
# ---------------------------
def login_layout():
    return html.Div(
        style={
            "height": "100vh",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center",
            "backgroundImage": 'url("/assets/login_page.jpg")',
            "backgroundSize": "cover",
            "backgroundPosition": "center",
        },
        children=[
            html.Div(
                style={"backgroundColor": "rgba(0,0,0,0.55)", "padding": 24, "borderRadius": 12, "width": 380},
                children=[
                    html.H3("Coffee Island – Login", style={"color": "white", "textAlign": "center"}),
                    dbc.Input(id="login-username", placeholder="Username", style={"marginTop": 8}),
                    dbc.Input(id="login-password", placeholder="Password", type="password", style={"marginTop": 8}),
                    dbc.Button("Login", id="login-button", color="primary", style={"width": "100%", "marginTop": 12}),
                    html.Div(id="login-error", style={"color": "#F87171", "marginTop": 8}),
                    html.Div(
                        style={"display": "flex", "justifyContent": "space-between", "marginTop": 8},
                        children=[
                            dbc.Button("Forgot Password", id="forgot-pw-btn", color="link", style={"padding": 0}),
                            html.Span("Sign-up disabled (Admin only)", style={"color": "#E5E7EB", "fontSize": 12}),
                        ],
                    ),
                    # Hidden containers for forgot pwd flow
                    html.Div(id="forgot-pw-area", style={"marginTop": 8}),
                ],
            )
        ],
    )

def welcome_layout():
    bg_color = get_dominant_color("welcome_page.jpg")

    return html.Div(
        style={
            "minHeight": "100vh",
            "width": "100%",
            "position": "relative",
            "overflow": "hidden",
            "backgroundColor": bg_color,
        },
        children=[
            html.Img(
                src="/assets/welcome_page.jpg",
                style={
                    "height": "100%",
                    "width": "100%",
                    "objectFit": "cover",
                    "display": "block",
                },
            ),
        ],
    )

# ---------------------------
# Sidebar + build
# ---------------------------
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "240px",
    "padding": "20px",
    "backgroundColor": "#111827",
    "color": "white",
}

def build_sidebar(role: str, username: str = None):
    allowed = ROLE_PAGES.get(role, set())
    nav_items = []
    if "dsr" in allowed:
        nav_items.append(dbc.NavLink("DSR Dashboard", href="/dsr", active="exact"))
    if "daypart" in allowed:
        nav_items.append(dbc.NavLink("Daypart Dashboard", href="/daypart", active="exact"))
    if "sales" in allowed:
        nav_items.append(dbc.NavLink("Sales Dashboard", href="/sales", active="exact"))
    if "feedback" in allowed:
        nav_items.append(dbc.NavLink("Customer Feedback Dashboard", href="/feedback", active="exact"))
    if "warehouse" in allowed:
        nav_items.append(dbc.NavLink("Warehouse Dashboard", href="/warehouse", active="exact"))
    if "menu-mix" in allowed:
        nav_items.append(dbc.NavLink("Menu Mix Dashboard", href="/menu-mix", active="exact"))
    if "item-activity" in allowed:
        nav_items.append(dbc.NavLink("Item Activity Dashboard", href="/item-activity", active="exact"))
    if "admin-users" in allowed:
        nav_items.append(dbc.NavLink("User Management", href="/admin-users", active="exact"))
    if "admin-logs" in allowed:
        nav_items.append(dbc.NavLink("Login Logs & Jobs", href="/admin-logs", active="exact"))

    # profile + logout
    if username:
        nav_items.append(html.Div(f"Signed in: {username}", style={"fontSize": 12, "marginTop": 12}))
        nav_items.append(dbc.NavLink("Profile / Change Password", href="/profile", active="exact"))
    nav_items.append(dbc.NavLink("Logout", href="/logout", style={"color": "#F87171", "marginTop": 12}))

    return html.Div(
        [
            dcc.Link(
                html.H3(
                    "Coffee Island",
                    style={
                        "color": "#FACC15",
                        "cursor": "pointer",
                        "marginBottom": "10px",
                    },
                ),
                href="/",
                refresh=False,
            ),
            html.Hr(style={"borderColor": "#374151"}),
            dbc.Nav(nav_items, vertical=True, pills=True),
        ],
        id="sidebar",   # ✅ ADD THIS
        style=SIDEBAR_STYLE,
    )

    
def get_dominant_color(image_name):
    image_path = os.path.join(BASE_DIR, "assets", image_name)
    img = Image.open(image_path)
    img = img.resize((50, 50))
    arr = np.array(img).reshape((-1, 3))
    dominant = arr.mean(axis=0).astype(int)
    return f"rgb({dominant[0]}, {dominant[1]}, {dominant[2]})"

# ---------------------------
# User management layout (same capabilities as before)
# ---------------------------
def user_management_layout():
    users = get_users_for_display()
    return html.Div(
        style={"padding": 20},
        children=[
            html.H3("User Management (Admin)"),
            html.Div("Create users, edit roles, activate/deactivate, delete."),
            dbc.Row([
                dbc.Col([
                    html.H5("Create New User"),
                    dbc.Input(id="new-user-username", placeholder="Username", style={"marginTop": 8}),
                    dbc.Input(id="new-user-password", placeholder="Password", type="password", style={"marginTop": 8}),
                    dbc.Input(id="new-user-password2", placeholder="Confirm password", type="password", style={"marginTop": 8}),
                    dbc.Input(id="new-user-security-q", placeholder="Security Question (for reset)", style={"marginTop": 8}),
                    dbc.Input(id="new-user-security-a", placeholder="Answer", type="password", style={"marginTop": 8}),
                    dcc.Dropdown(id="new-user-role", options=[
                        {"label":"Admin","value":"admin"},
                        {"label":"Manager","value":"manager"},
                        {"label":"Viewer","value":"viewer"},
                    ], placeholder="Role", style={"marginTop": 8}),
                    dbc.Button("Create User (Admin only)", id="create-user-btn", color="success", style={"marginTop": 8}),
                    html.Div(id="create-user-msg", style={"marginTop": 8}),
                ], md=4),
                dbc.Col([
                    html.H5("Existing Users"),
                    dash_table.DataTable(
                        id="users-table",
                        data=users,
                        columns=[
                            {"name":"Username","id":"username"},
                            {"name":"Role","id":"role"},
                            {"name":"Active","id":"is_active"},
                            {"name":"Created","id":"created_at"},
                            {"name":"Last Login","id":"last_login"},
                        ],
                        page_size=10,
                        row_selectable="single",
                    ),
                    html.Div(style={"height":8}),
                    dbc.Button("Activate", id="btn-activate-user", color="success", size="sm", className="me-2"),
                    dbc.Button("Deactivate", id="btn-deactivate-user", color="warning", size="sm", className="me-2"),
                    dbc.Button("Delete", id="btn-delete-user", color="danger", size="sm"),
                    html.Div(style={"height":8}),
                    dcc.Dropdown(id="edit-role-dropdown", options=[
                        {"label":"Admin","value":"admin"},
                        {"label":"Manager","value":"manager"},
                        {"label":"Viewer","value":"viewer"},
                    ], placeholder="New role"),
                    dbc.Button("Update Role", id="btn-update-role", color="secondary", size="sm", style={"marginTop":8}),
                    html.Div(id="user-mgmt-msg", style={"marginTop":8}),
                ], md=8),
            ])
        ]
    )

# ---------------------------
# Admin Logs & Jobs layout
# ---------------------------
def admin_logs_layout():
    logs = get_login_logs(limit=200)
    return html.Div(
        style={"padding": 20},
        children=[
            html.H3("Login Activity & Jobs (Admin)"),
            html.Div("Recent login attempts (newest first)."),
            dbc.Button("Run Daily Job Now", id="btn-run-daily-job", color="primary", className="mb-3"),
            html.Div(id="run-job-status", style={"marginTop": 8}),
            dash_table.DataTable(
                id="login-logs-table",
                data=logs,
                columns=[
                    {"name":"Timestamp","id":"timestamp"},
                    {"name":"Username","id":"username"},
                    {"name":"Success","id":"success"},
                    {"name":"Reason","id":"reason"},
                ],
                page_size=20,
                style_table={"overflowX":"auto"},
            ),
            html.Div(style={"height": 10}),
            dbc.Button("Export CSV (Logs)", id="export-logs-csv", color="secondary"),
            dcc.Download(id="download-logs-csv"),
        ]
    )

# ---------------------------
# Profile layout (change password + recent activity)
# ---------------------------
def profile_layout(username):
    recent = get_recent_login_activity(username, limit=50)
    return html.Div(
        style={"padding": 20},
        children=[
            html.H3(f"Profile — {username}"),
            html.H5("Change password"),
            dbc.Input(id="change-old-pw", placeholder="Old password", type="password", style={"marginTop": 8}),
            dbc.Input(id="change-new-pw", placeholder="New password", type="password", style={"marginTop": 8}),
            dbc.Input(id="change-new-pw2", placeholder="Confirm new password", type="password", style={"marginTop": 8}),
            dbc.Button("Change Password", id="btn-change-password", color="primary", style={"marginTop": 8}),
            html.Div(id="change-password-msg", style={"marginTop": 8}),
            html.H5("Recent login activity", style={"marginTop": 20}),
            dash_table.DataTable(
                id="profile-login-activity",
                data=recent,
                columns=[
                    {"name":"Timestamp","id":"timestamp"},
                    {"name":"Success","id":"success"},
                    {"name":"Reason","id":"reason"},
                ],
                page_size=10,
            ),
        ]
    )

# ---------------------------
# Main App Layout
# ---------------------------
app.layout = html.Div(
    [

        # ------------------------------------------------------------
        # URL router
        # ------------------------------------------------------------
        dcc.Location(id="url", refresh=False),

        # Session storage (JWT token)
        dcc.Store(id="session", storage_type="session"),

        # Toast data storage
        dcc.Store(id="toast-data"),

        # ------------------------------------------------------------
        # PAGE-WISE FILTER STATE (CRITICAL)
        # ------------------------------------------------------------
        dcc.Store(id="filters-dsr", storage_type="session"),
        dcc.Store(id="filters-feedback", storage_type="session"),
        dcc.Store(id="filters-menu-mix", storage_type="session"),
        dcc.Store(id="filters-warehouse", storage_type="session"),

        # ------------------------------------------------------------
        # Toast UI
        # ------------------------------------------------------------
        dbc.Toast(
            id="toast",
            is_open=False,
            duration=3000,
            style={
                "position": "fixed",
                "top": 10,
                "right": 10,
                "zIndex": 2000,
            },
        ),

        # ------------------------------------------------------------
        # GLOBAL HIDDEN FILTERS — used across all dashboards
        # ------------------------------------------------------------
        html.Div(
            id="global-hidden-filters",
            style={"display": "none"},
            children=[
                dcc.DatePickerSingle(id="date_from"),
                dcc.DatePickerSingle(id="date_to"),

                dcc.Dropdown(id="f_tab"),
                dcc.Dropdown(id="f_source"),
                dcc.Dropdown(id="f_supercat"),
                dcc.Dropdown(id="f_cat"),
                dcc.Dropdown(id="f_item"),
                dcc.Dropdown(id="f_month"),
                dcc.Dropdown(id="f_day"),
                dcc.Dropdown(id="f_fy"),
                dcc.Dropdown(id="f_week"),

                dcc.Dropdown(id="f_warehouse"),

                dcc.Dropdown(id="f_brand"),
                dcc.Dropdown(id="f_region"),
                dcc.Dropdown(id="f_state"),
                dcc.Dropdown(id="f_city"),
                dcc.Dropdown(id="f_type"),
                dcc.Dropdown(id="f_outlet"),

                dcc.Dropdown(id="brand_filter"),
                dcc.Dropdown(id="region_filter"),
                dcc.Dropdown(id="state_filter"),
                dcc.Dropdown(id="city_filter"),
                dcc.Dropdown(id="type_filter"),
                dcc.Dropdown(id="outlet_filter"),
                dcc.Dropdown(id="source_filter"),
                dcc.Dropdown(id="category_filter"),
                dcc.Dropdown(id="week_filter"),
                dcc.Dropdown(id="day_filter"),
                dcc.Dropdown(id="month_filter"),

                html.Button(id="qr_yesterday"),
                html.Button(id="qr_7"),
                html.Button(id="qr_30"),
                html.Button(id="qr_90"),

                html.Button(id="refresh_button"),
                html.Button(id="reset_filters_btn"),
                dcc.Input(id="search_text"),

                html.Button(id="btn_csv"),
                html.Button(id="btn_excel"),
                dcc.Download(id="download_csv"),
                dcc.Download(id="download_excel"),

                dcc.Interval(
                    id="auto_reload",
                    interval=600000,
                    n_intervals=0,
                ),
            ],
        ),

        # ------------------------------------------------------------
        # MAIN PAGE CONTENT (ALL PAGES)
        # ------------------------------------------------------------
        dcc.Loading(
            id="loader",
            fullscreen=False,
            children=[
                html.Div(id="root"),   # ✅ your router target
            ],
        ),

        # ------------------------------------------------------------
        # GLOBAL FOOTER (DESKTOP ONLY, AUTO-HIDE)
        # ------------------------------------------------------------
        footer(),   # ✅ visible on every page

    ],
    style={"minHeight": "100vh"},
)


# ---------------------------
# Validation layout (preloads ALL pages)
# ---------------------------
app.validation_layout = html.Div([
    login_layout(),
    welcome_layout(),
    user_management_layout(),
    admin_logs_layout(),
    profile_layout("dummy"),

    # All page layouts rendered at least once
    dsr_page.get_layout(),
    daypart_page.get_layout(),
    sales_page.get_layout(),
    feedback_page.get_layout(),
    warehouse_dashboard.get_layout(),
    menu_mix.get_layout(),
    item_activity_page.get_layout(),
    

    # Global filter IDs (ONLY ACTIVE ONES)
    dcc.DatePickerSingle(id="date_from"),
    dcc.DatePickerSingle(id="date_to"),
    dcc.Dropdown(id="f_warehouse"),

    dcc.Dropdown(id="f_tab"),
    dcc.Dropdown(id="f_source"),
    dcc.Dropdown(id="f_supercat"),
    dcc.Dropdown(id="f_cat"),
    dcc.Dropdown(id="f_item"),
    dcc.Dropdown(id="f_month"),
    dcc.Dropdown(id="f_day"),
    dcc.Dropdown(id="f_fy"),
    dcc.Dropdown(id="f_week"),

    dcc.Dropdown(id="brand_filter"),
    dcc.Dropdown(id="region_filter"),
    dcc.Dropdown(id="state_filter"),
    dcc.Dropdown(id="city_filter"),
    dcc.Dropdown(id="type_filter"),
    dcc.Dropdown(id="outlet_filter"),
    dcc.Dropdown(id="source_filter"),
    dcc.Dropdown(id="category_filter"),
    dcc.Dropdown(id="week_filter"),
    dcc.Dropdown(id="day_filter"),

    html.Button(id="refresh_button"),
    html.Button(id="reset_filters_btn"),
    dcc.Input(id="search_text"),

    html.Button(id="btn_csv"),
    html.Button(id="btn_excel"),
    dcc.Download(id="download_csv"),
    dcc.Download(id="download_excel"),

    dcc.Interval(id="auto_reload"),
])

from dash.development.base_component import Component

def walk_layout(item):
    ids = []
    if isinstance(item, Component):
        if hasattr(item, "id") and item.id:
            ids.append(item.id)
        for prop in item.__dict__.values():
            if isinstance(prop, list):
                for c in prop:
                    ids += walk_layout(c)
            else:
                if isinstance(prop, Component):
                    ids += walk_layout(prop)
    return ids

feedback_ids = walk_layout(feedback_page.get_layout())

warehouse_ids = walk_layout(warehouse_dashboard.get_layout())

# ---------------------------
# Root: show login or app (role-aware)
# ---------------------------
@app.callback(
    Output("root", "children"),
    Input("session", "data"),
    Input("url", "pathname"),
)

def show_root(session_data, pathname):

    token = session_data.get("token") if session_data else None
    payload = verify_token(token) if token else None

    # 👇 page-content ALWAYS exists
    page_shell = html.Div(
        [
            # ☰ Toggle button (always visible)
            html.Button(
                "☰",
                id="sidebar-toggle",
                style={
                    "fontSize": "20px",
                    "background": "none",
                    "border": "none",
                    "cursor": "pointer",
                    "marginBottom": "10px",
                },
            ),

            # Page content renders here
            html.Div(id="page-inner"),
        ],
        id="page-content",
        className="with-sidebar",   # ✅ important for CSS
        style={
            "marginLeft": "260px",
            "padding": "20px",
        },
    )

    if not payload:
        return html.Div([
            login_layout(),
            page_shell
        ])

    role = payload.get("role", "viewer")
    username = payload.get("sub")

    return html.Div([
        build_sidebar(role, username),
        page_shell
    ])



@app.callback(
    Output("url", "pathname"),
    Input("session", "data"),
    prevent_initial_call=True
)
def redirect_on_load(session):
    if not session or not session.get("token"):
        return "/login"
    return dash.no_update

# ---------------------------
# Login handler
# ---------------------------
@app.callback(
    Output("session", "data", allow_duplicate=True),
    Output("login-error", "children"),
    Output("toast-data", "data", allow_duplicate=True),
    Output("url", "pathname", allow_duplicate=True),
    Input("login-button", "n_clicks"),
    State("login-username", "value"),
    State("login-password", "value"),
    prevent_initial_call=True,
)
def handle_login(n_clicks, username, password):
    role = authenticate_user(username or "", password or "")
    if not role:
        return {"token": None}, "Invalid username or password.", {"type":"error","msg":"Login failed"}, "/login"
    token = generate_token(username=username, role=role, ttl_hours=12)
    return {"token": token}, "", {"type":"success","msg":f"Welcome {username} ({role})"}, "/"

# ---------------------------
# Forgot password: show question / allow reset
# ---------------------------
@app.callback(
    Output("forgot-pw-area", "children"),
    Input("forgot-pw-btn", "n_clicks"),
    State("login-username", "value"),
    prevent_initial_call=True,
)
def show_forgot_area(n_clicks, username):
    # show input to enter username and fetch question
    if not username:
        return html.Div("Enter username above and click Forgot Password.", style={"color": "#FBBF24"})
    question = get_security_question(username)
    if not question:
        return html.Div("No security question set for this user. Contact admin.", style={"color": "#F87171"})
    return html.Div([
        html.Div(f"Security question: {question}", style={"fontWeight":"600"}),
        dbc.Input(id="pw-reset-answer", placeholder="Answer", type="text", style={"marginTop":8}),
        dbc.Input(id="pw-reset-new", placeholder="New password", type="password", style={"marginTop":8}),
        dbc.Input(id="pw-reset-new2", placeholder="Confirm new password", type="password", style={"marginTop":8}),
        dbc.Button("Reset Password", id="pw-reset-submit", color="primary", style={"marginTop":8}),
        html.Div(id="pw-reset-msg", style={"marginTop":8}),
    ])

@app.callback(
    Output("pw-reset-msg", "children"),
    Output("toast-data", "data", allow_duplicate=True),
    Input("pw-reset-submit", "n_clicks"),
    State("login-username", "value"),
    State("pw-reset-answer", "value"),
    State("pw-reset-new", "value"),
    State("pw-reset-new2", "value"),
    prevent_initial_call=True,
)
def perform_pw_reset(n_clicks, username, answer, new1, new2):
    if not username:
        return "Username missing.", {"type":"error","msg":"Username missing"}
    if not answer:
        return "Answer required.", {"type":"error","msg":"Answer required"}
    if not new1 or not new2 or new1 != new2:
        return "New passwords do not match.", {"type":"error","msg":"Passwords mismatch"}
    ok, msg = reset_password_with_answer(username, answer, new1)
    return msg, {"type":"success" if ok else "error","msg":msg}

# ---------------------------
# Page router (loads page layouts)
# ---------------------------
@app.callback(
    Output("page-inner", "children"),
    Input("url", "pathname"),
    Input("session", "data"),
)

def render_page(pathname, session_data):
    token = session_data.get("token") if session_data and isinstance(session_data, dict) else None
    payload = verify_token(token) if token else None
    if not payload:
        raise dash.exceptions.PreventUpdate

    role = payload.get("role", "viewer")
    username = payload.get("sub")
    allowed = ROLE_PAGES.get(role, set())

    # Home / Welcome page
    if pathname in ("/", "/home", None):
        return welcome_layout()

    if pathname == "/dsr":
        return dsr_page.get_layout() if "dsr" in allowed else html.Div("Access Denied")
    if pathname == "/daypart":
        return daypart_page.get_layout() if "daypart" in allowed else html.Div("Access Denied")
    if pathname == "/sales":
        return sales_page.get_layout() if "sales" in allowed else html.Div("Access Denied")
    if pathname == "/feedback":
        return feedback_page.get_layout() if "feedback" in allowed else html.Div("Access Denied")
    if pathname == "/warehouse":
        return warehouse_dashboard.get_layout() if "warehouse" in allowed else html.Div("Access Denied")
    if pathname == "/menu-mix":
        return menu_mix.get_layout() if "menu-mix" in allowed else html.Div("Access Denied")
    if pathname == "/item-activity":
        return item_activity_page.get_layout() if "item-activity" in allowed else html.Div("Access Denied")
    if pathname == "/admin-users":
        return user_management_layout() if "admin-users" in allowed else html.Div("Access Denied")
    if pathname == "/admin-logs":
        return admin_logs_layout() if "admin-logs" in allowed else html.Div("Access Denied")
    if pathname == "/profile":
        return profile_layout(username)

    return html.Div("404 - Not Found", style={"padding":20})

# ---------------------------
# Logout handler
# ---------------------------
@app.callback(
    Output("session", "data", allow_duplicate=True),
    Output("toast-data", "data", allow_duplicate=True),
    Output("url", "pathname", allow_duplicate=True),
    Input("url", "pathname"),
    prevent_initial_call=True,
)
def handle_logout(pathname):
    if pathname == "/logout":
        return {"token": None}, {"type":"success","msg":"Logged out"}, "/login"
    raise dash.exceptions.PreventUpdate

# ---------------------------
# Toast handler
# ---------------------------
@app.callback(
    Output("toast", "is_open"),
    Output("toast", "header"),
    Output("toast", "children"),
    Output("toast", "icon"),
    Input("toast-data", "data"),
    prevent_initial_call=True,
)
def show_toast(toast_data):
    if not toast_data:
        raise dash.exceptions.PreventUpdate
    typ = toast_data.get("type", "info")
    hdr = typ.title()
    icon = {"success":"success","error":"danger","warning":"warning","info":"info"}.get(typ,"info")
    return True, hdr, toast_data.get("msg",""), icon

@app.callback(Output("toast-data", "data"),
              Input("url", "pathname"))
def debug_ids(path):
    from dash._callback_context import context_value
    layout_ids = []

    def collect_ids(component):
        if hasattr(component, "id") and component.id:
            layout_ids.append(component.id)
        if hasattr(component, "children"):
            if isinstance(component.children, list):
                for child in component.children:
                    collect_ids(child)
            else:
                collect_ids(component.children)
    collect_ids(app.layout)
    return no_update

# ---------------------------
# User management callback (single callback handles actions)
# ---------------------------
@app.callback(
    Output("create-user-msg", "children"),
    Output("toast-data", "data", allow_duplicate=True),
    Output("users-table", "data"),
    Input("create-user-btn", "n_clicks"),
    Input("btn-activate-user", "n_clicks"),
    Input("btn-deactivate-user", "n_clicks"),
    Input("btn-delete-user", "n_clicks"),
    Input("btn-update-role", "n_clicks"),
    Input("users-table", "selected_rows"),
    Input("edit-role-dropdown", "value"),
    Input("new-user-username", "value"),
    Input("new-user-password", "value"),
    Input("new-user-password2", "value"),
    Input("new-user-security-q", "value"),
    Input("new-user-security-a", "value"),
    State("users-table", "data"),
    State("session", "data"),
    prevent_initial_call=True,
)
def manage_users(n_create, n_activate, n_deactivate, n_delete, n_update_role,
                 selected_rows, edit_role_value, new_username, new_pw1, new_pw2,
                 new_sec_q, new_sec_a, table_data, session_data):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate
    trig = ctx.triggered[0]["prop_id"].split(".")[0]

    # ensure admin
    token = session_data.get("token") if session_data else None
    pl = verify_token(token) if token else None
    if not pl or pl.get("role") != "admin":
        return "Only admin can manage users.", {"type":"error","msg":"Not authorized"}, get_users_for_display()

    # selected username
    sel_username = None
    if selected_rows and table_data:
        idx = selected_rows[0]
        if 0 <= idx < len(table_data):
            sel_username = table_data[idx].get("username")

    # Create
    if trig == "create-user-btn":
        if not new_username or not new_pw1 or not new_pw2:
            return "Missing fields", {"type":"error","msg":"Missing fields"}, get_users_for_display()
        if new_pw1 != new_pw2:
            return "Passwords do not match", {"type":"error","msg":"Passwords mismatch"}, get_users_for_display()
        ok, msg = create_user(new_username, new_pw1, role=(edit_role_value or "viewer"), security_question=(new_sec_q or ""), security_answer=(new_sec_a or ""))
        return msg, {"type":"success" if ok else "error","msg":msg}, get_users_for_display()

    if trig == "btn-activate-user":
        if not sel_username: return "Select user", {"type":"error","msg":"No user selected"}, get_users_for_display()
        ok = set_user_active(sel_username, True)
        msg = "Activated" if ok else "Failed"
        return msg, {"type":"success" if ok else "error","msg":msg}, get_users_for_display()

    if trig == "btn-deactivate-user":
        if not sel_username: return "Select user", {"type":"error","msg":"No user selected"}, get_users_for_display()
        ok = set_user_active(sel_username, False)
        msg = "Deactivated" if ok else "Failed"
        return msg, {"type":"success" if ok else "error","msg":msg}, get_users_for_display()

    if trig == "btn-delete-user":
        if not sel_username: return "Select user", {"type":"error","msg":"No user selected"}, get_users_for_display()
        ok = delete_user(sel_username)
        msg = "Deleted" if ok else "Failed"
        return msg, {"type":"success" if ok else "error","msg":msg}, get_users_for_display()

    if trig == "btn-update-role":
        if not sel_username or not edit_role_value:
            return "Select user and role", {"type":"error","msg":"Missing"}, get_users_for_display()
        ok = update_user_role(sel_username, edit_role_value)
        msg = "Role updated" if ok else "Failed"
        return msg, {"type":"success" if ok else "error","msg":msg}, get_users_for_display()

    return "", no_update, get_users_for_display()

# ---------------------------
# Change password (Profile)
# ---------------------------
@app.callback(
    Output("change-password-msg", "children"),
    Output("toast-data", "data", allow_duplicate=True),
    Input("btn-change-password", "n_clicks"),
    State("session", "data"),
    State("change-old-pw", "value"),
    State("change-new-pw", "value"),
    State("change-new-pw2", "value"),
    prevent_initial_call=True,
)
def handle_change_password(n_clicks, session_data, old_pw, new_pw1, new_pw2):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    token = session_data.get("token") if session_data else None
    pl = verify_token(token) if token else None
    if not pl:
        return "Not logged in", {"type":"error","msg":"Session expired"}

    if not old_pw:
        return "Enter old password", {"type":"error","msg":"Old password missing"}

    if not new_pw1 or not new_pw2:
        return "Enter new password twice", {"type":"error","msg":"New password missing"}

    if new_pw1 != new_pw2:
        return "New passwords do not match", {"type":"error","msg":"Passwords mismatch"}

    username = pl.get("sub")
    ok, msg = change_password(username, old_pw, new_pw1)

    return msg, {
        "type": "success" if ok else "error",
        "msg": msg
    }


# ---------------------------
# Admin: run daily job now
# ---------------------------
@app.callback(
    Output("run-job-status", "children"),
    Output("toast-data", "data", allow_duplicate=True),
    Input("btn-run-daily-job", "n_clicks"),
    State("session", "data"),
    prevent_initial_call=True,
)
def run_job_now(n_clicks, session_data):
    token = session_data.get("token") if session_data else None
    pl = verify_token(token) if token else None
    if not pl or pl.get("role") != "admin":
        return "Not authorized", {"type":"error","msg":"Not authorized"}
    if not run_daily_job_now:
        return "Daily job module not available on server.", {"type":"error","msg":"run_daily_job not found"}
    try:
        # run_now should be implemented to run synchronously or spawn a background thread.
        run_daily_job_now()
        return "Daily job executed (check logs).", {"type":"success","msg":"Daily job executed"}
    except Exception as e:
        return f"Error running job: {e}", {"type":"error","msg":f"Job error: {e}"}

# ---------------------------
# Admin: export logs
# ---------------------------
@app.callback(
    Output("download-logs-csv", "data"),
    Input("export-logs-csv", "n_clicks"),
    State("session", "data"),
    prevent_initial_call=True,
)
def export_logs_csv(n_clicks, session_data):
    token = session_data.get("token") if session_data else None
    pl = verify_token(token) if token else None
    if not pl or pl.get("role") != "admin":
        raise dash.exceptions.PreventUpdate
    logs = get_login_logs(limit=5000)
    import io, csv
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["timestamp","username","success","reason"])
    writer.writeheader()
    for r in logs:
        writer.writerow({
            "timestamp": r.get("timestamp"),
            "username": r.get("username"),
            "success": r.get("success"),
            "reason": r.get("reason"),
        })
    return dict(content=buf.getvalue().encode("utf-8"), filename="login_logs.csv", type="text/csv")
# ---------------------------
# Register callbacks for pages (your existing pages should expose register_callbacks)
# ---------------------------
try:
    dsr_page.register_callbacks(app)
except Exception:
    pass

try:
    daypart_page.register_callbacks(app)
except Exception:
    pass

try:
    sales_page.register_callbacks(app)
except Exception:
    pass

try:
    feedback_page.init_callbacks(app)
except Exception as e:
    print("Feedback callback error:", e)

try:
    warehouse_dashboard.register_callbacks(app)
except Exception as e:
    print("Feedback callback error:", e)
    
try:
    menu_mix.register_callbacks(app)
except Exception as e:
    print("Menu Mix callback error:", e)
    
try:
    item_activity_page.register_callbacks(app)
except Exception as e:
    print("Item Consumption callback error:", e)

# ---------------------------
# Run server
# ---------------------------
if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=8050,
        debug=True
    )
