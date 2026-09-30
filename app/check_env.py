import sys
import duckdb

print("python:", sys.version.split()[0])
print("duckdb:", duckdb.__version__)
print(duckdb.sql("SELECT 42 AS answer").fetchall())

