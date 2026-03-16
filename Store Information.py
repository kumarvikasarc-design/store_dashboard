import os
import io
import base64
import pandas as pd
from datetime import datetime
from dash import Dash, html, dcc, Input, Output, State, dash_table, no_update, callback_context
from dash.exceptions import PreventUpdate
from fpdf import FPDF

# === File paths ===
DB_FILE = "stores_db.csv"
EXCEL_FILE = "stores_db.xlsx"

# Initialize empty DB files if missing
if not os.path.exists(DB_FILE):
    df_init = pd.DataFrame(columns=[
        "Store Id", "Outlet Name", "Region", "City",
        "Type", "Area Manager", "Live Date", "Status"
    ])
    df_init.to_csv(DB_FILE, index=False)

if not os.path.exists(EXCEL_FILE):
    pd.read_csv(DB_FILE).to_excel(EXCEL_FILE, index=False)

def load_db():
    try:
        df = pd.read_csv(DB_FILE)
        # Ensure 'Store Id' is integer type for max() calculation later
        df['Store Id'] = pd.to_numeric(df['Store Id'], errors='coerce').fillna(0).astype(int)
        return df
    except Exception:
        # Return an empty dataframe if file is corrupt or empty
        return pd.DataFrame(columns=[
            "Store Id", "Outlet Name", "Region", "City",
            "Type", "Area Manager", "Live Date", "Status"
        ])

def save_db(df):
    df.to_csv(DB_FILE, index=False)
    df.to_excel(EXCEL_FILE, index=False)

REGION_CITY_MAP = {
    "North": ["Gurgaon", "Delhi", "Chandigarh"],
    "South": ["Bangalore", "Chennai", "Hyderabad"],
    "East": ["Kolkata", "Bhagalpur", "Patna"],
    "West": ["Mumbai", "Ahmedabad", "Pune"]
}

REGION_OPTIONS = list(REGION_CITY_MAP.keys())
STATUS_OPTIONS = ["Active", "Inactive"]

MODAL_STYLE = {
    "display": "none",
    "position": "fixed",
    "zIndex": 9999,
    "left": 0,
    "top": 0,
    "width": "100%",
    "height": "100%",
    "overflow": "auto",
    "backgroundColor": "rgba(0,0,0,0.4)"
}

MODAL_CONTENT_STYLE = {
    "backgroundColor": "#fefefe",
    "margin": "10% auto",
    "padding": "20px",
    "border": "1px solid #888",
    "width": "400px",
    "borderRadius": "8px",
    "fontFamily": "Times New Roman",
    "fontSize": "14px"
}

BUTTON_STYLE = {
    "backgroundColor": "#0052cc",
    "color": "white",
    "border": "none",
    "padding": "8px 15px",
    "marginRight": "10px",
    "borderRadius": "5px",
    "fontFamily": "Times New Roman",
    "fontSize": "14px",
    "cursor": "pointer",
}

BUTTON_STYLE_SECONDARY = {
    **BUTTON_STYLE,
    "backgroundColor": "#777",
}

FILTER_STYLE = {
    "width": "150px",
    "display": "inline-block",
    "marginRight": "10px",
    "fontFamily": "Times New Roman",
    "fontSize": "14px",
}

app = Dash(__name__)
server = app.server

def generate_table_columns():
    return [
        {"name": "Store Id", "id": "Store Id", "editable": False},
        {"name": "Outlet Name", "id": "Outlet Name", "editable": False},
        {"name": "Region", "id": "Region", "presentation": "dropdown", "editable": False},
        {"name": "City", "id": "City", "presentation": "dropdown", "editable": False},
        {"name": "Type", "id": "Type", "editable": False},
        {"name": "Area Manager", "id": "Area Manager", "editable": False},
        {"name": "Live Date", "id": "Live Date", "editable": False},
        {"name": "Status", "id": "Status", "presentation": "dropdown", "editable": False},
        {
            "name": "Action",
            "id": "edit",
            "presentation": "markdown"
        }
    ]

def add_edit_button_to_data(df):
    df = df.copy()
    # Only add the edit button if 'edit' column is not already present from previous update/filter
    if 'edit' not in df.columns:
        df['edit'] = ['Edit' for _ in range(len(df))]
    return df

app.layout = html.Div([
    html.H2("Store Information", style={"fontWeight": "bold", "fontSize": "24px", "fontFamily": "Times New Roman"}),

    html.Div([
        html.Button("Add Store", id="add-btn", style=BUTTON_STYLE),
        html.Button("Download Template", id="template-btn", style=BUTTON_STYLE_SECONDARY),
        html.Button("Download Excel", id="excel-btn", style=BUTTON_STYLE_SECONDARY),
        html.Button("Download PDF", id="pdf-btn", style=BUTTON_STYLE_SECONDARY),
        dcc.Upload(id="upload",
                   children=html.Button("Upload Excel", style=BUTTON_STYLE_SECONDARY),
                   multiple=False,
                   style={"display": "inline-block"}),
    ], style={"marginBottom": "15px"}),

    html.Div([
        dcc.Input(id="search-input", type="text", placeholder="Search Store Id or Outlet Name...",
                  style={"marginRight": "10px", "width": "300px", "fontFamily": "Times New Roman", "fontSize": "14px"}),

        dcc.Dropdown(id="filter-region", placeholder="Filter by Region", clearable=True,
                     options=[{"label": r, "value": r} for r in REGION_OPTIONS],
                     style=FILTER_STYLE),

        dcc.Dropdown(id="filter-status", placeholder="Filter by Status", clearable=True,
                     options=[{"label": s, "value": s} for s in STATUS_OPTIONS],
                     style=FILTER_STYLE),

        dcc.Dropdown(id="filter-area-manager", placeholder="Filter by Area Manager", clearable=True,
                     options=[],
                     style={"width": "200px", "display": "inline-block", "marginRight": "10px",
                            "fontFamily": "Times New Roman", "fontSize": "14px"}),

        dcc.Dropdown(id="filter-city", placeholder="Filter by City", clearable=True,
                     options=[],
                     style=FILTER_STYLE),

        html.Button("Reset", id="reset-btn", style=BUTTON_STYLE_SECONDARY),
    ], style={"marginBottom": "20px"}),

    dash_table.DataTable(
        id="table",
        columns=generate_table_columns(),
        data=add_edit_button_to_data(load_db()).to_dict("records"),
        editable=False,
        row_deletable=False,
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={
            'fontFamily': 'Times New Roman',
            'fontSize': '14px',
            'textAlign': 'left',
        },
        dropdown={
            "Region": {"options": [{"label": r, "value": r} for r in REGION_OPTIONS]},
            "Status": {"options": [{"label": s, "value": s} for s in STATUS_OPTIONS]},
        },
        style_header={
            'fontWeight': 'bold',
            'fontFamily': 'Times New Roman',
            'fontSize': '14px',
            'textAlign': 'left',
        },
        markdown_options={"html": True},
    ),

    html.Div(id="modal", style=MODAL_STYLE, children=[
        html.Div(style=MODAL_CONTENT_STYLE, children=[
            html.H3(id="modal-title", style={"fontWeight": "bold", "fontSize": "18px"}),

            html.Label("Outlet Name *"),
            dcc.Input(id="input-outlet", type="text", style={"width": "100%"}),

            html.Label("Region *"),
            dcc.Dropdown(id="input-region", options=[{"label": r, "value": r} for r in REGION_OPTIONS], clearable=False),

            html.Label("City *"),
            dcc.Dropdown(id="input-city", options=[], clearable=False),

            html.Label("Type *"),
            dcc.Input(id="input-type", type="text", style={"width": "100%"}),

            html.Label("Area Manager *"),
            dcc.Input(id="input-manager", type="text", style={"width": "100%"}),

            html.Label("Live Date (YYYY-MM-DD) *"),
            dcc.Input(id="input-date", type="text", placeholder="2025-01-29", style={"width": "100%"}),

            html.Label("Status *"),
            dcc.Dropdown(id="input-status", options=[{"label": s, "value": s} for s in STATUS_OPTIONS], clearable=False),

            html.Div(id="modal-msg", style={"color": "red", "marginTop": "8px"}),

            html.Div([
                html.Button("Save", id="save-btn", n_clicks=0, style=BUTTON_STYLE),
                html.Button("Cancel", id="cancel-btn", n_clicks=0, style={**BUTTON_STYLE_SECONDARY, "marginLeft": "10px"}),
            ], style={"marginTop": "15px", "textAlign": "right"}),
        ]),
    ]),

    html.Div(id="msg", style={"marginTop": "10px", "color": "green", "fontFamily": "Times New Roman", "fontSize": "14px"}),

    dcc.Download(id="download-template"),
    dcc.Download(id="download-excel"),
    dcc.Download(id="download-pdf"),

    dcc.Store(id='edit-store-id', data=None),
])

# Unified callback to handle modal, editing, filtering, and filtering dropdown options
@app.callback(
    Output("modal", "style"),
    Output("modal-title", "children"),
    Output("input-outlet", "value"),
    Output("input-region", "value"),
    Output("input-city", "options"),
    Output("input-city", "value"),
    Output("input-type", "value"),
    Output("input-manager", "value"),
    Output("input-date", "value"),
    Output("input-status", "value"),
    Output("modal-msg", "children"),
    Output("table", "data"),
    Output("msg", "children"),
    Output("edit-store-id", "data"),
    Output("filter-area-manager", "options"),
    Output("filter-city", "options"),
    Output("filter-region", "value"),
    Output("filter-status", "value"),
    Output("filter-area-manager", "value"),
    Output("filter-city", "value"),
    Input("add-btn", "n_clicks"),
    Input("cancel-btn", "n_clicks"),
    Input("table", "active_cell"),
    Input("upload", "contents"),
    Input("save-btn", "n_clicks"),
    Input("search-input", "value"),
    Input("filter-region", "value"),
    Input("filter-status", "value"),
    Input("filter-area-manager", "value"),
    Input("filter-city", "value"),
    Input("input-region", "value"),
    Input("reset-btn", "n_clicks"),
    State("input-outlet", "value"),
    State("input-region", "value"),
    State("input-city", "value"),
    State("input-type", "value"),
    State("input-manager", "value"),
    State("input-date", "value"),
    State("input-status", "value"),
    State("edit-store-id", "data"),
    State("table", "data"),
    prevent_initial_call=True
)
def unified_callback(
    add_click, cancel_click, active_cell, upload_contents, save_clicks,
    search_value, filter_region, filter_status, filter_manager, filter_city,
    modal_region_value, reset_click,
    # State variables for Save/Update
    input_outlet_val, input_region_val, input_city_value, input_type_val, input_manager_val, input_date_val, input_status_val,
    edit_store_id,
    table_data
):
    ctx = callback_context
    if not ctx.triggered:
        raise PreventUpdate
    trig = ctx.triggered[0]["prop_id"].split(".")[0]

    # Load the data
    df = load_db()
    df_current = df.copy() 
    
    # Default outputs (mostly no_update or hidden modal)
    modal_style = {**MODAL_STYLE, "display": "none"}
    modal_msg = ""
    msg = ""
    modal_title = no_update
    
    # Modal input values (use new state vars for save logic)
    input_outlet = no_update
    input_region = no_update
    input_city_options = no_update
    input_city_value_out = no_update
    input_type = no_update
    input_manager = no_update
    input_date = no_update
    input_status = no_update
    
    edit_id = edit_store_id
    updated_data = no_update
    
    # Filter state variables (will be updated by reset/filter change)
    new_filter_region = filter_region
    new_filter_status = filter_status
    new_filter_manager = filter_manager
    new_filter_city = filter_city

    # --- 1. Dynamic Filter Options (must run first to initialize/update options) ---
    
    # Initialize filter options based on the full database
    all_cities = sorted(df["City"].dropna().unique())
    filter_city_options = [{"label": city, "value": city} for city in all_cities]

    all_area_managers = sorted(df["Area Manager"].dropna().unique())
    filter_area_manager_options = [{"label": am, "value": am} for am in all_area_managers]

    # Adjust options and filter values ONLY if a Region is selected
    if new_filter_region:
        # Set City options based on the selected Region
        cities_for_region = REGION_CITY_MAP.get(new_filter_region, [])
        filter_city_options = [{"label": city, "value": city} for city in cities_for_region]

        # Set Area Manager options based on the stores in the selected Region
        df_region = df[df["Region"] == new_filter_region]
        area_managers_for_region = sorted(df_region["Area Manager"].dropna().unique())
        filter_area_manager_options = [{"label": am, "value": am} for am in area_managers_for_region]

        # Reset city filter if currently selected city is not in the new options
        if new_filter_city and new_filter_city not in cities_for_region:
            new_filter_city = None

        # Reset area manager filter if currently selected manager is not in the new options
        if new_filter_manager and new_filter_manager not in area_managers_for_region:
            new_filter_manager = None
            
    # --- 2. Handle Reset Button Click ---
    if trig == "reset-btn" and reset_click:
        new_filter_region = None
        new_filter_status = None
        new_filter_manager = None
        new_filter_city = None
        search_value = "" # Clear search input
        
        # Reset modal input values too (as they are passed as no_update if modal is closed)
        input_outlet = ""
        input_region = None
        input_city_options = []
        input_city_value_out = None
        input_type = ""
        input_manager = ""
        input_date = ""
        input_status = None
        edit_id = None
        
        # Now continue to filtering logic to display full table

    # --- 3. Handle Modal Open/Close (Add/Cancel/Edit) ---
    elif trig == "add-btn":
        modal_style = {**MODAL_STYLE, "display": "block"}
        modal_title = "Add New Store"
        # Clear modal inputs for new entry
        input_outlet, input_region, input_city_value_out, input_type, input_manager, input_date, input_status = "", None, None, "", "", "", None
        input_city_options = []
        edit_id = None

    elif trig == "cancel-btn":
        modal_style = {**MODAL_STYLE, "display": "none"}
        modal_msg = ""
        # Return current state (which flows to filtering step)

    # --- 3. Handle Modal Open/Close (Add/Cancel/Edit) ---
    # ... (add-btn and cancel-btn logic)

    # ... (Lines 437-441: add-btn and cancel-btn logic)
    
    # ... (around line 388, inside the unified_callback function)

    # Handle Edit Button Click
    elif trig == "table":
        # Check if a cell was clicked AND it was the 'edit' column
        if active_cell and active_cell["column_id"] == "edit":
            
            # --- row_idx and row are safely defined here ---
            row_idx = active_cell["row"]
            
            # Use table_data (which is the currently displayed, filtered/searched data)
            if row_idx < len(table_data): 
                row = table_data[row_idx]
                
                modal_style = {**MODAL_STYLE, "display": "block"}
                modal_title = f"Edit Store Id {row['Store Id']}"
                
                # Setup modal inputs with existing data
                input_outlet = row.get("Outlet Name", "")
                input_region = row.get("Region", None)
                input_type = row.get("Type", "")
                input_manager = row.get("Area Manager", "")
                input_date = row.get("Live Date", "")
                input_status = row.get("Status", None)
                edit_id = row.get("Store Id")

                # Update City options and value
                if input_region in REGION_CITY_MAP:
                    input_city_options = [{"label": c, "value": c} for c in REGION_CITY_MAP[input_region]]
                else:
                    input_city_options = []
                input_city_value_out = row.get("City", None)
                
            else:
                # If row index is out of bounds, prevent update
                raise PreventUpdate
                
        # If the trigger was 'table' but not an edit click (e.g., sorting), 
        # the flow simply continues to the filtering logic below without error.

# ... (rest of the unified_callback continues)
        
    # ... (Rest of the callback continues from here)
    # --- 4. Handle City Dropdown Update in Modal (input-region change) ---
    if trig == "input-region":
        modal_style = {**MODAL_STYLE, "display": "block"} # Keep modal open
        if modal_region_value in REGION_CITY_MAP:
            input_city_options = [{"label": c, "value": c} for c in REGION_CITY_MAP[modal_region_value]]
        else:
            input_city_options = []
        input_city_value_out = None # Reset city when region changes
        
        # Set modal inputs back to their current State values
        input_outlet = input_outlet_val
        input_type = input_type_val
        input_manager = input_manager_val
        input_date = input_date_val
        input_status = input_status_val
        
        # Return immediately as the state is only being manipulated for the modal
        return (
            modal_style, modal_title, input_outlet, modal_region_value, # Use modal_region_value for region output
            input_city_options, input_city_value_out, input_type, input_manager,
            input_date, input_status, modal_msg,
            updated_data, msg, edit_id,
            filter_area_manager_options, filter_city_options,
            new_filter_region, new_filter_status,
            new_filter_manager, new_filter_city
        )

    # --- 5. Handle Save Button (Add/Update) ---
    elif trig == "save-btn":
        # FIX: Use the correctly named State variables
        required_fields = [input_outlet_val, input_region_val, input_city_value, input_type_val, input_manager_val, input_date_val, input_status_val]
        
        if not all(required_fields):
            modal_style = {**MODAL_STYLE, "display": "block"}
            modal_msg = "❌ Please fill all required fields."
            edit_id = edit_store_id # Keep the edit ID to continue editing

            # Restore modal inputs to their State values
            input_outlet = input_outlet_val
            input_region = input_region_val
            input_type = input_type_val
            input_manager = input_manager_val
            input_date = input_date_val
            input_status = input_status_val
            input_city_value_out = input_city_value
            
            if input_region_val in REGION_CITY_MAP:
                 input_city_options = [{"label": c, "value": c} for c in REGION_CITY_MAP[input_region_val]]

        else:
            try:
                datetime.strptime(input_date_val, "%Y-%m-%d")
            except ValueError:
                modal_style = {**MODAL_STYLE, "display": "block"}
                modal_msg = "❌ Date must be in YYYY-MM-DD format."
                edit_id = edit_store_id
                
                # Restore modal inputs to their State values
                input_outlet = input_outlet_val
                input_region = input_region_val
                input_type = input_type_val
                input_manager = input_manager_val
                input_date = input_date_val
                input_status = input_status_val
                input_city_value_out = input_city_value
                
                if input_region_val in REGION_CITY_MAP:
                    input_city_options = [{"label": c, "value": c} for c in REGION_CITY_MAP[input_region_val]]
            else:
                # Proceed with Save/Update
                new_row_data = {
                    "Outlet Name": input_outlet_val,
                    "Region": input_region_val,
                    "City": input_city_value,
                    "Type": input_type_val,
                    "Area Manager": input_manager_val,
                    "Live Date": input_date_val,
                    "Status": input_status_val,
                }
                
                if edit_store_id is None: # Add New
                    new_id = df["Store Id"].max() + 1 if not df.empty else 10001
                    new_row_data["Store Id"] = new_id
                    df_new_row = pd.DataFrame([new_row_data])
                    df = pd.concat([df, df_new_row], ignore_index=True)
                    msg = f"✅ Store {new_id} added successfully."
                else: # Update Existing
                    df.loc[df["Store Id"] == edit_store_id, list(new_row_data.keys())] = list(new_row_data.values())
                    msg = f"✅ Store {edit_store_id} updated successfully."

                save_db(df)
                df_current = df # Update df_current to the saved state for filtering
                modal_style = {**MODAL_STYLE, "display": "none"}
                edit_id = None # Clear edit ID after saving

    # --- 6. Handle Upload Excel File ---
    elif trig == "upload":
        if upload_contents:
            content_type, content_string = upload_contents.split(",")
            decoded = base64.b64decode(content_string)
            try:
                # Try to read the uploaded file
                df_new = pd.read_excel(io.BytesIO(decoded))
                required_cols = set(["Store Id", "Outlet Name", "Region", "City", "Type", "Area Manager", "Live Date", "Status"])
                
                # Validation check
                if not required_cols.issubset(df_new.columns):
                    msg = "❌ Excel missing required columns."
                else:
                    # Successful read and validation
                    save_db(df_new)
                    df_current = df_new # Set df_current to the newly uploaded data for filtering
                    msg = "✅ File uploaded and data updated."
            except Exception as e:
                 msg = f"❌ Invalid file uploaded: {e}"
        # No else needed, just fall through to filtering
        
    # --- 7. Filtering the table data based on filters/search (ALWAYS RUN at the end, unless an action returned early) ---
    
    # Apply filtering to df_current, which holds the latest data (initial load, or after save/upload)
    filtered = df_current.copy()
    
    # Apply filters based on the current filter/search state
    if search_value and search_value.strip():
        sv = search_value.strip().lower()
        def matches(row):
            id_match = str(row.get("Store Id", "")).lower()
            name_match = str(row.get("Outlet Name", "")).lower()
            return (sv in id_match) or (sv in name_match)
        filtered = filtered[filtered.apply(matches, axis=1)]

    if new_filter_region:
        filtered = filtered[filtered["Region"] == new_filter_region]
    if new_filter_status:
        filtered = filtered[filtered["Status"] == new_filter_status]
    if new_filter_manager:
        filtered = filtered[filtered["Area Manager"] == new_filter_manager]
    if new_filter_city:
        filtered = filtered[filtered["City"] == new_filter_city]

    # Only update table data if a filter/data change trigger occurred (avoid unnecessary updates)
    if trig in ["search-input", "filter-region", "filter-status", "filter-area-manager", "filter-city", "save-btn", "upload", "reset-btn"]:
        updated_data = add_edit_button_to_data(filtered).to_dict("records")
    elif updated_data is no_update:
        # Initial load or no specific data trigger, just refresh the whole table
        updated_data = add_edit_button_to_data(df).to_dict("records")

    # If modal is closed (not open for edit/add/input-region change), ensure inputs are reset
    if modal_style["display"] == "none" and trig not in ["add-btn", "table", "input-region", "save-btn"]:
        input_outlet = ""
        input_region = None
        input_city_options = []
        input_city_value_out = None
        input_type = ""
        input_manager = ""
        input_date = ""
        input_status = None
        edit_id = None
        
    # Final Return (must return all 20 outputs)
    return (
        modal_style, modal_title, input_outlet, input_region,
        input_city_options, input_city_value_out, input_type, input_manager,
        input_date, input_status, modal_msg,
        updated_data, msg, edit_id,
        filter_area_manager_options, filter_city_options,
        new_filter_region, new_filter_status,
        new_filter_manager, new_filter_city
    )

def df_to_pdf(io, df):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Times", size=12)
    # Filter out the 'edit' column if it exists, as it's a Dash-specific column
    if 'edit' in df.columns:
        df = df.drop(columns=['edit'])
        
    col_names = df.columns.tolist()
    col_width = pdf.w / (len(col_names) + 1)
    row_height = pdf.font_size * 1.5

    # Header
    for col_name in col_names:
        pdf.cell(col_width, row_height, str(col_name), border=1)
    pdf.ln(row_height)

    # Rows
    for _, row in df.iterrows():
        for item in row:
            # Simple check to avoid crashing on long text
            content = str(item) if len(str(item)) < 50 else str(item)[:47] + "..."
            pdf.cell(col_width, row_height, content, border=1)
        pdf.ln(row_height)

    pdf_bytes = pdf.output(dest='S').encode('latin1')  # get PDF as bytes
    io.write(pdf_bytes)  # write bytes to the BytesIO buffer
    io.seek(0)  # rewind to start

# Download Template
@app.callback(
    Output("download-template", "data"),
    Input("template-btn", "n_clicks"),
    prevent_initial_call=True
)
def download_template(n):
    def generate_excel_template(io):
        df = pd.DataFrame({
            "Store Id": ["10001", "10002"],
            "Outlet Name": ["Coffee Island HQ", "Coffee Island GKII"],
            "Region": ["North", "North"],
            "City": ["Gurgaon", "Delhi"],
            "Type": ["BPC", "HSC"],
            "Area Manager": ["Mohammad Sufiyan", "Mohammad Sufiyan"],
            "Live Date": ["2025-01-29", "2025-06-04"],
            "Status": ["Active", "Active"]
        })
        df.to_excel(io, index=False, sheet_name="Stores", engine='openpyxl')
        io.seek(0)

    return dcc.send_bytes(generate_excel_template, "Store_Template.xlsx")

# Download Excel
@app.callback(
    Output("download-excel", "data"),
    Input("excel-btn", "n_clicks"),
    prevent_initial_call=True
)
def download_xlsx(n):
    df = load_db()
    # Drop the internal 'edit' column if it exists
    if 'edit' in df.columns:
        df = df.drop(columns=['edit'])
        
    def to_excel(io):
        df.to_excel(io, index=False, sheet_name="Stores", engine='openpyxl')
        io.seek(0)
    return dcc.send_bytes(to_excel, f"StoreInfo_{datetime.now().strftime('%Y%m%d')}.xlsx")

# Download PDF
@app.callback(
    Output("download-pdf", "data"),
    Input("pdf-btn", "n_clicks"),
    prevent_initial_call=True
)
def download_pdf(n):
    df = load_db()
    def to_pdf(io):
        df_to_pdf(io, df)
    return dcc.send_bytes(to_pdf, f"StoreInfo_{datetime.now().strftime('%Y%m%d')}.pdf")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8053, debug=True)