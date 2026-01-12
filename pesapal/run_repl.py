#!/usr/bin/env python
"""REPL with file persistence"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pesapal_app.rdbms_core import Database

def main():
    db = Database("pesapal_db")
    
    # Try to load from file
    if os.path.exists("db.pesapal"):
        if db.load_from_file():
            print("✓ Loaded from db.pesapal")
        else:
            print("✗ Could not load, starting fresh")
            # Create sample tables
            try:
                db.execute_sql("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        email TEXT UNIQUE,
                        age INTEGER,
                        created_at TEXT
                    )
                """)
                
                db.execute_sql("""
                    CREATE TABLE IF NOT EXISTS products (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        price REAL,
                        in_stock BOOLEAN,
                        category TEXT
                    )
                """)
            except:
                pass
    else:
        print("✗ No db.pesapal file found, starting fresh")
    
    print("\n" + "="*50)
    print("PESAPALDB REPL with File Persistence")
    print("Database will auto-save to 'db.pesapal'")
    print("Commands: SAVE, LOAD, EXIT, or SQL")
    print("="*50 + "\n")
    
    while True:
        try:
            cmd = input("SQL> ").strip()
            
            if cmd.upper() == 'EXIT':
                # Save before exiting
                db.save_to_file()
                print("Database saved. Goodbye!")
                break
                
            elif cmd.upper() == 'SAVE':
                db.save_to_file()
                
            elif cmd.upper() == 'LOAD':
                db.load_from_file()
                
            elif cmd.upper() == 'HELP':
                print("""
Commands:
  SAVE           - Save database to db.pesapal
  LOAD           - Load database from db.pesapal
  SCHEMA         - Show database schema
  EXIT           - Exit and save
  Any SQL query  - Execute SQL
                """)
                
            elif cmd.upper() == 'SCHEMA':
                schema = db.get_schema()
                print(f"\nDatabase: {schema['name']}")
                for table_name, info in schema['tables'].items():
                    print(f"\n{table_name} ({info['row_count']} rows)")
                    for col in info['columns']:
                        constr = []
                        if col['primary']: constr.append("PK")
                        if col['unique']: constr.append("UNIQUE")
                        if not col['nullable']: constr.append("NOT NULL")
                        constr_str = f" ({', '.join(constr)})" if constr else ""
                        print(f"  {col['name']}: {col['type']}{constr_str}")
                print()
                
            else:
                # Execute SQL
                result = db.execute_sql(cmd)
                if result is not None:
                    if isinstance(result, list):
                        if result:
                            headers = list(result[0].keys())
                            print(" | ".join(headers))
                            print("-" * 40)
                            for row in result:
                                print(" | ".join(str(row.get(h, '')) for h in headers))
                            print(f"\n{len(result)} rows")
                        else:
                            print("Empty result set")
                    else:
                        print(f"Result: {result}")
                
                # Auto-save after each command
                db.save_to_file()
                print("✓ Auto-saved to db.pesapal")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()