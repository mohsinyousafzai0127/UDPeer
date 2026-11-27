"""Starts 4 peers on this computer, each in its own console window.
Works even when the folder path contains spaces (e.g. E:\\5th Semester\\...).
On other systems it prints the 4 commands to run by hand.

Run:  python run_demo.py
"""
import os
import platform
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

ARGS = [["main.py", "1", "5000"]] + \
       [["main.py", str(i), str(5000 + i - 1), "127.0.0.1:5000"] for i in range(2, 5)]

if platform.system() == "Windows":
    for a in ARGS:
        # a list (not one string) means Windows quotes every part correctly
        subprocess.Popen([PY] + a, cwd=HERE, creationflags=subprocess.CREATE_NEW_CONSOLE)
    print("started 4 peers in 4 new windows")
else:
    print("Open 4 terminals in this folder and run, in this order:\n")
    for a in ARGS:
        print("   ", PY, " ".join(a))