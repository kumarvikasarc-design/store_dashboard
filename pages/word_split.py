import pandas as pd

# ============================
# File paths
# ============================
input_file = r"C:\Users\ACER\Desktop\address.xlsx"
output_file = r"C:\Users\ACER\Desktop\address_cleaned.xlsx"

# ============================
# Load Excel
# ============================
df = pd.read_excel(input_file)

# ============================
# Remove Pin + 6-digit PIN and anything after
# ============================
df['Address_Clean'] = df['Address'].str.replace(
    r'\s*(Pin[- ]?\d{6}).*',
    '',
    regex=True
).str.strip()

# ============================
# Save output
# ============================
df.to_excel(output_file, index=False)

print("✅ PIN removed successfully!")
print(f"📁 Output saved at: {output_file}")
