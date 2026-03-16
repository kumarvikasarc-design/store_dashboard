import pandas as pd
import glob
import os

path = r"C:\Users\ACER\Desktop\whatsapp_message_report"
files = glob.glob(path + "/*.csv")

dfs = []

for file in files:
    try:
        temp = pd.read_csv(file, sep=None, engine="python", on_bad_lines="skip")
        temp["source_file"] = os.path.basename(file)
        dfs.append(temp)
    except Exception as e:
        print(f"❌ Error reading {file}: {e}")

merged = pd.concat(dfs, ignore_index=True, sort=False)

output_path = os.path.join(path, "merged_output.csv")
merged.to_csv(output_path, index=False)

print(f"✅ Merge complete! Output saved at: {output_path}")
