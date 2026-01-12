#!/usr/bin/env python
"""
REPL Interface for Custom RDBMS
Run with: python run_repl.py
"""
import sys
import os

# Add the project to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from pesapal_app.rdbms_core import Database, REPL
except ImportError:
    print("Error: Could not import from pesapal_app")
    print("Make sure you're running from the correct directory:")
    print("Current directory:", os.getcwd())
    sys.exit(1)

def main():
    print("=== Pesapal RDBMS REPL ===")
    print("Interactive SQL Interface")
    print("Type 'HELP' for commands, 'EXIT' to quit")
    print("=" * 40)
    
    # Create database instance
    db = Database("pesapal_challenge_db")
    
    # Initialize with sample data
    try:
        # Create users table
        db.execute_sql("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER,
                created_at TEXT
            )
        """)
        print("Created 'users' table")
    except Exception:
        pass  # Table might exist
    
    try:
        # Create products table
        db.execute_sql("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL,
                in_stock BOOLEAN,
                category TEXT
            )
        """)
        print("Created 'products' table")
    except Exception:
        pass
    
    # Start REPL
    repl = REPL(db)
    repl.run()
    
    print("\nGoodbye!")

if __name__ == "__main__":
    main()