#!/usr/bin/env python3
"""
Auto-fix script for store_dashboard project:
- Ensures __init__.py exist (EMPTY) in root, pages/, utils/
- Replaces relative imports like:
    from .utils.data_loader import ...
    from ..utils import auth
  -> with absolute imports:
    from utils.data_loader import ...
    from utils import auth
- Ensures main_app.py contains BASE_DIR sys.path insertion (to allow 'from utils...' from main file)
- Backs up files to ./_autofix_backups/<timestamp>/

Run from project root (the directory that contains main_app.py).
"""
import os
import re
import shutil
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(ROOT, "_autofix_backups", datetime.now().strftime("%Y%m%d_%H%M%S"))
os.makedirs(BACKUP_DIR, exist_ok=True)

# files / folders to ensure __init__.py
INIT_PATHS = [
    os.path.join(ROOT, "__init__.py"),
    os.path.join(ROOT, "pages", "__init__.py"),
    os.path.join(ROOT, "utils", "__init__.py"),
]

# file globs to scan for relative imports
SCAN_DIRS = [
    os.path.join(ROOT, "pages"),
    os.path.join(ROOT, "utils"),
    ROOT,
]

# regex patterns to replace:
# 1) from .utils.data_loader import load_x -> from utils.data_loader import load_x
# 2) from ..utils.something import -> from utils.something import
# 3) from .module import X -> from module import X
REL_IMPORT_RE = re.compile(r'from\s+(\.+)([A-Za-z0-9_\.]+)\s+import\s', flags=re.MULTILINE)

# also capture relative "import" (rare): import .module as m    (not valid python normally)
# will not attempt to fix invalid forms

# function to backup file
def backup_file(path):
    if not os.path.exists(path):
        return
    rel = os.path.relpath(path, ROOT)
    dest = os.path.join(BACKUP_DIR, rel)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(path, dest)

# ensure __init__.py empty (create or overwrite)
for p in INIT_PATHS:
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        if os.path.exists(p):
            backup_file(p)
        with open(p, "w", encoding="utf-8") as fh:
            fh.write("# package marker — keep empty\n")
        print(f"[OK] Wrote empty __init__: {p}")
    except Exception as e:
        print(f"[ERR] Could not write __init__ at {p}: {e}")

# helper to replace relative imports in a file content
def fix_relative_imports_in_text(text, fname):
    changed = False

    def repl(m):
        nonlocal changed
        dots = m.group(1)  # one or more dots
        rest = m.group(2)  # module path after dots
        # We drop all leading dots and treat as absolute under project root.
        # e.g. from .utils.data_loader -> from utils.data_loader
        new = f"from {rest} import "
        changed = True
        return new

    new_text = REL_IMPORT_RE.sub(repl, text)

    # Additional fixes: "from . import X" -> "from X import" (if module name follows)
    # Handle: from . import something  --> from something import  (rare)
    new_text2 = re.sub(r'from\s+\.\s+import\s+([A-Za-z0-9_]+)', r'from \1 import ', new_text)
    if new_text2 != new_text:
        new_text = new_text2
        changed = True

    # Fix imports that incorrectly reference 'pages.utils' or 'pages.data_loader' — often from attempts to use relative imports
    # If we see "from pages.utils..." inside pages or root, convert to "from utils..."
    new_text3 = re.sub(r'from\s+pages\.utils\.', 'from utils.', new_text)
    if new_text3 != new_text:
        new_text = new_text3
        changed = True

    # Fix accidental "from .utils import data_loader" -> "from utils import data_loader"
    new_text4 = re.sub(r'from\s+\.utils\b', 'from utils', new_text)
    if new_text4 != new_text:
        new_text = new_text4
        changed = True

    # Remove leading "from ./..." style (unlikely)
    new_text5 = re.sub(r'from\s+\.\/', 'from ', new_text)
    if new_text5 != new_text:
        new_text = new_text5
        changed = True

    # Fix `from .auth import (` inside main_app or others
    new_text6 = re.sub(r'from\s+\.(auth|utils\.(auth))', r'from utils.auth', new_text)
    if new_text6 != new_text:
        new_text = new_text6
        changed = True

    return new_text, changed

# scan and fix files
fixed_files = []
skipped_files = []
for scan_dir in SCAN_DIRS:
    if not os.path.isdir(scan_dir):
        continue
    for root, dirs, files in os.walk(scan_dir):
        # skip backups folder
        if "_autofix_backups" in root:
            continue
        for fn in files:
            if not fn.endswith(".py"):
                continue
            path = os.path.join(root, fn)
            # skip compiled or pip cache
            if any(p in path for p in [os.sep + "__pycache__", os.sep + ".git"]):
                continue
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    content = fh.read()
                new_content, changed = fix_relative_imports_in_text(content, path)

                # Also ensure main_app has BASE_DIR sys.path insertion
                if os.path.basename(path) == "main_app.py":
                    if "BASE_DIR = os.path.dirname(os.path.abspath(__file__))" not in new_content:
                        # add insertion after imports (top of file)
                        header = (
                            "\n# --- Auto-inserted to make package imports work ---\n"
                            "import os, sys\n"
                            "BASE_DIR = os.path.dirname(os.path.abspath(__file__))\n"
                            "if BASE_DIR not in sys.path:\n"
                            "    sys.path.insert(0, BASE_DIR)\n"
                            "# --------------------------------------------------\n\n"
                        )
                        # insert after the first import block: find first occurrence of "import" and insert after that line block
                        lines = new_content.splitlines()
                        insert_at = 0
                        # find first non-shebang line
                        for i, L in enumerate(lines[:40]):
                            if L.strip().startswith("import") or L.strip().startswith("from "):
                                insert_at = i
                                # find end of import block
                        # place header before the first non-import after initial import block
                        # To simplify, if header not present, prepend header
                        new_content = header + new_content
                        changed = True

                if changed:
                    backup_file(path)
                    with open(path, "w", encoding="utf-8") as fh:
                        fh.write(new_content)
                    fixed_files.append(path)
                    print(f"[FIXED] {path}")
            except Exception as e:
                skipped_files.append((path, str(e)))
                print(f"[SKIPPED] {path} -> {e}")

# post-check: ensure main_app imports are absolute
main_app = os.path.join(ROOT, "main_app.py")
if os.path.exists(main_app):
    with open(main_app, "r", encoding="utf-8") as fh:
        mtxt = fh.read()
    # remove any "from .utils" or "from .auth" leftover
    mtxt2 = re.sub(r'from\s+\.(utils|auth|pages)\b', r'from \1', mtxt)
    if mtxt2 != mtxt:
        backup_file(main_app)
        with open(main_app, "w", encoding="utf-8") as fh:
            fh.write(mtxt2)
        fixed_files.append(main_app)
        print(f"[FIXED] main_app.py absolute imports cleaned")

print("\n=== AUTO-FIX SUMMARY ===")
print(f"Backups saved in: {BACKUP_DIR}")
print(f"Files fixed: {len(fixed_files)}")
for f in fixed_files[:50]:
    print(" -", os.path.relpath(f, ROOT))
if skipped_files:
    print("\nFiles skipped / errors:")
    for p, e in skipped_files:
        print(" -", os.path.relpath(p, ROOT), ":", e)

print("\nManual checks recommended for files with complex relative imports or custom package logic.")
