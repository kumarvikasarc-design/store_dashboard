
# --- Auto-inserted to make package imports work ---
import os, sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
# --------------------------------------------------

# main_app.py – Unified multi-dashboard container
# Features:
#   - Login (CSV backend via utils/auth.py)
#   - JWT token stored in session
#   - Role-based dashboards
#   - Admin-only User Management page
#   - Loader animation
#   - Toast messages
#   - Login page with background image
#   - After login: only sidebar + blank page
#   - Proper logout (session clear + redirect + toast)
#   - Full Admin Controls: Create User, Edit Role, Activate/Deactivate, Delete User
#   - Search + Pagination

import os
import sys
import bootstrap
import dash
from dash import html, dcc, Input, Output, State
from dash import dash_table, no_update
import dash_bootstrap_components as dbc

# Ensure project root is importable
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pages import dsr_page, daypart_page, sales_page, expiry_cogs_page
from utils.auth import (
    authenticate_user,
    generate_token,
    verify_token,
    create_user,
    get_users_for_display,
    set_user_active,
    delete_user,
    update_user_role,
)


# -------------------------------
# Role → allowed pages
# -------------------------------
ROLE_PAGES = {
    "admin": {"dsr", "daypart", "sales", "expiry", "admin-users"},
    "manager": {"dsr", "daypart", "sales", "expiry"},
    "viewer": {"dsr", "sales"},
}

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)
server = app.server
app.title = "Coffee Island – Unified Analytics"


# -------------------------------
# LOGIN PAGE UI
# -------------------------------
def login_layout():
    return html.Div(
        style={
            "height": "100vh",
            "width": "100vw",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center",
            "backgroundImage": 'url("/assets/login_page.jpg")',
            "backgroundSize": "cover",
            "backgroundPosition": "center",
        },
        children=[
            html.Div(
                style={
                    "backgroundColor": "rgba(0, 0, 0, 0.55)",
                    "padding": "24px",
                    "borderRadius": "16px",
                    "maxWidth": "380px",
                    "width": "100%",
                    "color": "white",
                    "boxShadow": "0 4px 16px rgba(0,0,0,0.4)",
                },
                children=[
                    html.H3(
                        "Coffee Island – Login",
                        style={"marginBottom": "6px", "textAlign": "center"},
                    ),
                    html.Div(
                        "Enter your credentials to access dashboards.",
                        style={"fontSize": "12px", "color": "#e5e7eb", "marginBottom": "10px", "textAlign": "center"},
                    ),
                    dbc.Input(id="login-username", placeholder="Username", style={"marginBottom": "8px"}),
                    dbc.Input(id="login-password", placeholder="Password", type="password", style={"marginBottom": "8px"}),
                    dbc.Button("Login", id="login-button", color="primary", style={"width": "100%", "marginTop": "4px"}),
                    html.Div(id="login-error", style={"color": "#fecaca", "marginTop": "8px", "fontSize": "12px"}),
                ],
            )
        ],
    )


SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "240px",
    "padding": "20px 10px",
    "backgroundColor": "#111827",
    "color": "white",
}

# -------------------------------
# SIDEBAR
# -------------------------------
def build_sidebar(role: str):
    allowed = ROLE_PAGES.get(role, set())
    nav_items = []

    if "dsr" in allowed:
        nav_items.append(dbc.NavLink("DSR Dashboard", href="/dsr", active="exact"))
    if "daypart" in allowed:
        nav_items.append(dbc.NavLink("Daypart Dashboard", href="/daypart", active="exact"))
    if "sales" in allowed:
        nav_items.append(dbc.NavLink("Sales Dashboard", href="/sales", active="exact"))
    if "expiry" in allowed:
        nav_items.append(dbc.NavLink("COGS + Expiry Dashboard", href="/expiry", active="exact"))
    if "admin-users" in allowed:
        nav_items.append(dbc.NavLink("User Management", href="/admin-users", active="exact"))

    nav_items.append(dbc.NavLink("Logout", href="/logout", style={"color": "#ef4444", "marginTop": "20px"}))

    return html.Div(
        [
            html.H3("Coffee Island", style={"marginBottom": "8px"}),
            html.Hr(style={"borderColor": "#4B5563"}),
            html.P("Analytics Dashboards", style={"fontSize": "12px", "color": "#9CA3AF"}),
            dbc.Nav(nav_items, vertical=True, pills=True),
        ],
        style=SIDEBAR_STYLE,
    )


# -------------------------------
# USER MANAGEMENT UI
# -------------------------------
def user_management_layout():
    df = get_users_for_display()
    return html.Div(
        style={"padding": "16px"},
        children=[
            html.H3("User Management (Admin Only)"),
            html.Div("Create users and manage roles / status.", style={"fontSize": "12px", "marginBottom": "12px"}),

            dbc.Row(
                [
                    # CREATE USER
                    dbc.Col(
                        [
                            html.H5("Create New User"),
                            dbc.Input(id="new-user-username", placeholder="Username", style={"marginBottom": "6px"}),
                            dbc.Input(id="new-user-password", placeholder="Password", type="password", style={"marginBottom": "6px"}),
                            dbc.Input(id="new-user-password2", placeholder="Confirm Password", type="password", style={"marginBottom": "6px"}),
                            dcc.Dropdown(
                                id="new-user-role",
                                options=[
                                    {"label": "Admin", "value": "admin"},
                                    {"label": "Manager", "value": "manager"},
                                    {"label": "Viewer", "value": "viewer"},
                                ],
                                placeholder="Role",
                                style={"marginBottom": "6px"},
                            ),
                            dbc.Button("Create User", id="create-user-btn", color="primary", style={"marginTop": "4px"}),
                            html.Div(id="create-user-msg", style={"color": "#ef4444", "fontSize": "12px", "marginTop": "4px"}),
                        ],
                        width=4,
                    ),

                    # TABLE + ACTIONS
                    dbc.Col(
                        [
                            html.H5("Manage Users"),

                            # Search + Action Buttons
                            html.Div(
                                style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginBottom": "8px"},
                                children=[
                                    dbc.Input(id="user-search", placeholder="Search username / role...", style={"width": "220px"}),

                                    dcc.Dropdown(
                                        id="edit-role-dropdown",
                                        options=[
                                            {"label": "Admin", "value": "admin"},
                                            {"label": "Manager", "value": "manager"},
                                            {"label": "Viewer", "value": "viewer"},
                                        ],
                                        placeholder="New Role",
                                        style={"width": "160px"},
                                    ),

                                    dbc.Button("Update Role", id="btn-update-role", color="secondary", size="sm"),
                                    dbc.Button("Activate", id="btn-activate-user", color="success", size="sm"),
                                    dbc.Button("Deactivate", id="btn-deactivate-user", color="warning", size="sm"),
                                    dbc.Button("Delete", id="btn-delete-user", color="danger", size="sm"),
                                ],
                            ),

                            # Table
                            dash_table.DataTable(
                                id="users-table",
                                data=df.to_dict("records"),
                                columns=[
                                    {"name": "Username", "id": "username"},
                                    {"name": "Role", "id": "role"},
                                    {"name": "Active", "id": "is_active"},
                                ],
                                page_size=10,
                                row_selectable="single",
                                selected_rows=[],
                                style_table={"overflowX": "auto"},
                                style_cell={"fontSize": 11, "padding": "6px 8px", "whiteSpace": "nowrap"},
                                style_header={"backgroundColor": "#f5f5f5", "fontWeight": "600"},
                            ),

                            html.Div("Select a row, then use buttons above.", style={"fontSize": "11px", "marginTop": "4px"}),
                        ],
                        width=8,
                    ),
                ]
            ),
        ],
    )


# -------------------------------
# ROOT LAYOUT
# -------------------------------
app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id="session", storage_type="session"),
        dcc.Store(id="toast-data"),

        # Toast
        dbc.Toast(
            id="toast",
            is_open=False,
            header="",
            children="",
            icon="primary",
            duration=3500,
            dismissable=True,
            style={"position": "fixed", "top": 10, "right": 10, "zIndex": 2000},
        ),

        dcc.Loading(id="loader", type="circle", fullscreen=True, children=html.Div(id="root")),
    ]
)


# -------------------------------
# ROOT SWITCH (LOGIN or APP)
# -------------------------------
@app.callback(
    Output("root", "children"),
    Input("session", "data"),
    Input("url", "pathname"),
)
def show_root(session_data, pathname):

    if pathname in ("/login", "/logout"):
        return login_layout()

    token = session_data.get("token") if session_data else None
    payload = verify_token(token) if token else None

    if not payload:
        return login_layout()

    role = payload.get("role", "viewer")

    return html.Div(
        [
            build_sidebar(role),
            html.Div(id="page-content", style={"marginLeft": "260px", "padding": "20px"}),
        ]
    )


# -------------------------------
# LOGIN HANDLER
# -------------------------------
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
        return (
            {"token": None},
            "Invalid username or password.",
            {"type": "error", "msg": "Login failed – invalid credentials."},
            "/login",
        )

    token = generate_token(username=username, role=role, ttl_hours=12)
    return (
        {"token": token},
        "",
        {"type": "success", "msg": f"Welcome {username} ({role})"},
        "/",
    )


# -------------------------------
# TOAST HANDLER
# -------------------------------
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

    types = {
        "success": ("Success", "success"),
        "error": ("Error", "danger"),
        "warning": ("Warning", "warning"),
        "info": ("Info", "info"),
    }

    hdr, icon = types.get(toast_data.get("type", "info"), ("Info", "info"))

    return True, hdr, toast_data.get("msg", ""), icon


# -------------------------------
# PAGE ROUTING
# -------------------------------
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
    Input("session", "data"),
)
def render_page(pathname, session_data):

    token = session_data.get("token") if session_data else None
    payload = verify_token(token) if token else None

    if not payload:
        return html.Div()

    role = payload.get("role", "viewer")
    allowed = ROLE_PAGES.get(role, set())

    # Empty home page
    if pathname in ("/", "/home", "/login", None):
        return html.Div("")

    if pathname == "/dsr":
        return dsr_page.get_layout() if "dsr" in allowed else html.Div("Access Denied", style={"padding": "40px"})

    if pathname == "/daypart":
        return daypart_page.get_layout() if "daypart" in allowed else html.Div("Access Denied", style={"padding": "40px"})

    if pathname == "/sales":
        return sales_page.get_layout() if "sales" in allowed else html.Div("Access Denied", style={"padding": "40px"})

    if pathname == "/expiry":
        return expiry_cogs_page.get_layout() if "expiry" in allowed else html.Div("Access Denied", style={"padding": "40px"})

    if pathname == "/admin-users":
        return user_management_layout() if "admin-users" in allowed else html.Div("Access Denied", style={"padding": "40px"})

    if pathname == "/logout":
        return html.Div()

    return html.Div("404 – Page Not Found", style={"padding": "40px"})


# -------------------------------
# LOGOUT HANDLER
# -------------------------------
@app.callback(
    Output("session", "data", allow_duplicate=True),
    Output("toast-data", "data", allow_duplicate=True),
    Output("url", "pathname", allow_duplicate=True),
    Input("url", "pathname"),
    prevent_initial_call=True,
)
def handle_logout(pathname):
    if pathname == "/logout":
        return (
            {"token": None},
            {"type": "success", "msg": "Logged out successfully."},
            "/login",
        )
    raise dash.exceptions.PreventUpdate


# -------------------------------
# USER MANAGEMENT CALLBACK
# -------------------------------
@app.callback(
    Output("create-user-msg", "children"),
    Output("toast-data", "data", allow_duplicate=True),
    Output("users-table", "data"),

    Input("create-user-btn", "n_clicks"),
    Input("btn-activate-user", "n_clicks"),
    Input("btn-deactivate-user", "n_clicks"),
    Input("btn-delete-user", "n_clicks"),
    Input("btn-update-role", "n_clicks"),
    Input("user-search", "value"),

    State("new-user-username", "value"),
    State("new-user-password", "value"),
    State("new-user-password2", "value"),
    State("new-user-role", "value"),
    State("edit-role-dropdown", "value"),
    State("users-table", "data"),
    State("users-table", "selected_rows"),
    State("session", "data"),

    prevent_initial_call=True,
)
def manage_users(
    n_create, n_activate, n_deactivate, n_delete, n_update_role, search_value,
    new_username, new_pw1, new_pw2, new_role, edit_role_value,
    table_data, selected_rows, session_data
):

    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trig_id = ctx.triggered[0]["prop_id"].split(".")[0]

    token = session_data.get("token") if session_data else None
    payload = verify_token(token) if token else None
    role = payload.get("role") if payload else None

    # Helper: Filtered table based on search
    def get_filtered_df():
        df_all = get_users_for_display()
        if search_value:
            q = str(search_value).lower()
            df_all = df_all[
                df_all["username"].str.lower().str.contains(q)
                | df_all["role"].str.lower().str.contains(q)
            ]
        return df_all

    # Search → no need for auth
    if trig_id == "user-search":
        df = get_filtered_df()
        return "", no_update, df.to_dict("records")

    # Admin required
    if role != "admin":
        df = get_filtered_df()
        return "Only admin can manage users.", {"type": "error", "msg": "Not authorized."}, df.to_dict("records")

    # Selected user helper
    selected_username = None
    if selected_rows and table_data:
        idx = selected_rows[0]
        if 0 <= idx < len(table_data):
            selected_username = table_data[idx]["username"]

    msg = ""
    toast = None

    # CREATE USER
    if trig_id == "create-user-btn":
        if not new_username or not new_pw1 or not new_pw2 or not new_role:
            msg = "All fields are required."
            toast = {"type": "error", "msg": msg}
        elif new_pw1 != new_pw2:
            msg = "Passwords do not match."
            toast = {"type": "error", "msg": msg}
        else:
            success, msg = create_user(new_username, new_pw1, new_role)
            toast = {"type": "success" if success else "error", "msg": msg}

        df = get_filtered_df()
        return msg, toast, df.to_dict("records")

    # ACTIVATE
    if trig_id == "btn-activate-user":
        if selected_username:
            ok = set_user_active(selected_username, True)
            msg = f"User '{selected_username}' activated." if ok else "User not found."
            toast = {"type": "success" if ok else "error", "msg": msg}
        else:
            msg = "Select a user first."
            toast = {"type": "error", "msg": msg}

        df = get_filtered_df()
        return msg, toast, df.to_dict("records")

    # DEACTIVATE
    if trig_id == "btn-deactivate-user":
        if selected_username:
            ok = set_user_active(selected_username, False)
            msg = f"User '{selected_username}' deactivated." if ok else "User not found."
            toast = {"type": "success" if ok else "error", "msg": msg}
        else:
            msg = "Select a user first."
            toast = {"type": "error", "msg": msg}

        df = get_filtered_df()
        return msg, toast, df.to_dict("records")

    # DELETE
    if trig_id == "btn-delete-user":
        if selected_username:
            ok = delete_user(selected_username)
            msg = f"User '{selected_username}' deleted." if ok else "User not found."
            toast = {"type": "success" if ok else "error", "msg": msg}
        else:
            msg = "Select a user first."
            toast = {"type": "error", "msg": msg}

        df = get_filtered_df()
        return msg, toast, df.to_dict("records")

    # UPDATE ROLE
    if trig_id == "btn-update-role":
        if not selected_username:
            msg = "Select a user first."
            toast = {"type": "error", "msg": msg}
        elif not edit_role_value:
            msg = "Select new role."
            toast = {"type": "error", "msg": msg}
        else:
            ok = update_user_role(selected_username, edit_role_value)
            msg = f"User role updated to {edit_role_value}." if ok else "Failed to update role."
            toast = {"type": "success" if ok else "error", "msg": msg}

        df = get_filtered_df()
        return msg, toast, df.to_dict("records")

    df = get_filtered_df()
    return "", no_update, df.to_dict("records")


# -------------------------------
# REGISTER PAGE CALLBACKS
# -------------------------------
dsr_page.register_callbacks(app)
daypart_page.register_callbacks(app)
sales_page.register_callbacks(app)
expiry_cogs_page.register_callbacks(app)


# -------------------------------
# RUN SERVER
# -------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
