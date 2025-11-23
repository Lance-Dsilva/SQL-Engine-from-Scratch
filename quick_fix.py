#!/usr/bin/env python3
"""
IMMEDIATE FIX for 500MB file upload limit
Run this RIGHT NOW to fix your app

Usage: python QUICK_FIX.py
"""

import os
from pathlib import Path

print("=" * 70)
print(" QUICK FIX - Enable 500MB File Uploads")
print("=" * 70)
print()

# Step 1: Create .streamlit directory
config_dir = Path('.streamlit')
if not config_dir.exists():
    config_dir.mkdir()
    print("✅ Created .streamlit directory")
else:
    print("✅ .streamlit directory exists")

# Step 2: Create config.toml
config_file = config_dir / 'config.toml'

config_content = '''[server]
maxUploadSize = 500
maxMessageSize = 500
enableCORS = false
enableXsrfProtection = true

[browser]
gatherUsageStats = false
'''

with open(config_file, 'w') as f:
    f.write(config_content)

print("✅ Created/Updated .streamlit/config.toml")
print()

# Step 3: Remove problematic line from app.py if it exists
app_file = Path('app.py')
if app_file.exists():
    with open(app_file, 'r') as f:
        content = f.read()
    
    # Remove the problematic st.set_option line
    if "st.set_option('server.maxUploadSize'" in content or 'st.set_option("server.maxUploadSize"' in content:
        print("⚠️  Found problematic st.set_option line in app.py")
        
        # Remove the line
        lines = content.split('\n')
        new_lines = []
        for line in lines:
            if 'st.set_option' in line and 'maxUploadSize' in line:
                # Replace with comment
                indent = len(line) - len(line.lstrip())
                new_lines.append(' ' * indent + '# File upload limit set in .streamlit/config.toml (500MB)')
            else:
                new_lines.append(line)
        
        with open(app_file, 'w') as f:
            f.write('\n'.join(new_lines))
        
        print("✅ Removed st.set_option line from app.py")
        print("✅ Added comment about config.toml instead")
    else:
        print("✅ app.py looks good (no problematic lines)")
else:
    print("⚠️  app.py not found - make sure you're in the project directory")

print()
print("=" * 70)
print(" CONFIGURATION COMPLETE!")
print("=" * 70)
print()
print("📁 File created: .streamlit/config.toml")
print("⚙️  Max upload size: 500 MB")
print()
print("🚀 NEXT STEPS (IMPORTANT!):")
print()
print("   1. STOP your Streamlit app (press Ctrl+C in terminal)")
print()
print("   2. RESTART Streamlit:")
print("      streamlit run app.py")
print()
print("   3. HARD REFRESH your browser:")
print("      • Windows/Linux: Ctrl + Shift + R")
print("      • Mac: Cmd + Shift + R")
print()
print("   4. Try uploading a large file!")
print()
print("=" * 70)
print()
print("💡 TIPS:")
print("   • If still shows 200MB, close ALL browser tabs and restart")
print("   • Try in incognito/private window to avoid cache issues")
print("   • The limit is now 500MB for file uploads")
print()
print("✅ You're all set! Restart Streamlit now.")
print()