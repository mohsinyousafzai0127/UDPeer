"""Creates two random test files of 3 MB: big1.bin and big2.bin.  Run: python make_testfiles.py"""
import os

for name in ("big1.bin", "big2.bin"):
    with open(name, "wb") as f:
        f.write(os.urandom(3 * 1024 * 1024))
    print("created", name)
