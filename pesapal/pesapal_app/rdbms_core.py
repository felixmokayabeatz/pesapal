"""
Simple RDBMS Implementation for Pesapal Challenge
Author: [Your Name]
References: Basic database concepts from CS education, SQLite architecture
"""

import json
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict


class DataType:
    """Supported data types"""
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    REAL = "REAL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    
    @staticmethod
    def validate(data_type: str, value: Any) -> bool:
        if value is None:
            return True
        if data_type == DataType.INTEGER:
            return isinstance(value, int)
        elif data_type == DataType.TEXT:
            return isinstance(value, str)
        elif data_type == DataType.REAL:
            return isinstance(value, (int, float))
        elif data_type == DataType.BOOLEAN:
            return isinstance(value, bool) or value in (0, 1, '0', '1', True, False)
        elif data_type == DataType.DATE:
            return isinstance(value, str)
        return False


class Index:
    """Basic index implementation"""
    def __init__(self, column_name: str):
        self.column_name = column_name
        self.index = defaultdict(list)
    
    def add(self, value: Any, row_id: int):
        self.index[value].append(row_id)
    
    def remove(self, value: Any, row_id: int):
        if value in self.index and row_id in self.index[value]:
            self.index[value].remove(row_id)
    
    def get(self, value: Any) -> List[int]:
        return self.index.get(value, [])


class Column:
    def __init__(self, name: str, data_type: str, 
                 is_primary: bool = False, is_unique: bool = False, 
                 nullable: bool = True):
        self.name = name
        self.data_type = data_type
        self.is_primary = is_primary
        self.is_unique = is_unique
        self.nullable = nullable


class Table:
    def __init__(self, name: str):
        self.name = name
        self.columns: List[Column] = []
        self.rows = []
        self.row_count = 0
        self.indexes: Dict[str, Index] = {}
        self.unique_values: Dict[str, set] = {}
    
    def add_column(self, column: Column):
        if column.is_primary or column.is_unique:
            self.unique_values[column.name] = set()
            self.indexes[column.name] = Index(column.name)
        self.columns.append(column)
    
    def insert(self, values: Dict[str, Any]) -> int:
        # Validate
        row_data = {}
        for col in self.columns:
            if col.name in values:
                if not DataType.validate(col.data_type, values[col.name]):
                    raise ValueError(f"Invalid type for {col.name}")
                if not col.nullable and values[col.name] is None:
                    raise ValueError(f"{col.name} cannot be null")
                row_data[col.name] = values[col.name]
            elif col.is_primary and col.data_type == DataType.INTEGER:
                row_data[col.name] = self.row_count + 1
            else:
                row_data[col.name] = None
        
        # Check unique constraints
        for col in self.columns:
            if col.is_primary or col.is_unique:
                value = row_data[col.name]
                if value in self.unique_values.get(col.name, set()):
                    raise ValueError(f"Duplicate value for {col.name}")
                self.unique_values.setdefault(col.name, set()).add(value)
        
        # Insert
        self.row_count += 1
        self.rows.append(row_data)
        
        # Update indexes
        for col_name, index in self.indexes.items():
            if col_name in row_data:
                index.add(row_data[col_name], self.row_count)
        
        return self.row_count
    
    def select(self, where_clause: Optional[str] = None) -> List[Dict]:
        results = []
        for i, row in enumerate(self.rows, 1):
            if not where_clause or self._evaluate_where(row, where_clause):
                results.append({**row, '_id': i})
        return results
    
    def update(self, values: Dict[str, Any], where_clause: Optional[str] = None) -> int:
        updated = 0
        for i, row in enumerate(self.rows):
            if not where_clause or self._evaluate_where(row, where_clause):
                for col_name, value in values.items():
                    if col_name in row:
                        # Update value
                        old_value = row[col_name]
                        row[col_name] = value
                        
                        # Update indexes
                        if col_name in self.indexes:
                            self.indexes[col_name].remove(old_value, i + 1)
                            self.indexes[col_name].add(value, i + 1)
                
                updated += 1
        return updated
    
    def delete(self, where_clause: Optional[str] = None) -> int:
        indices_to_remove = []
        for i, row in enumerate(self.rows):
            if not where_clause or self._evaluate_where(row, where_clause):
                indices_to_remove.append(i)
        
        for i in sorted(indices_to_remove, reverse=True):
            row = self.rows.pop(i)
            for col_name, index in self.indexes.items():
                if col_name in row:
                    index.remove(row[col_name], i + 1)
        
        self.row_count = len(self.rows)
        return len(indices_to_remove)
    
    def _evaluate_where(self, row: Dict, where_clause: str) -> bool:
        try:
            # Simple evaluator
            expression = where_clause
            for col_name, value in row.items():
                if isinstance(value, str):
                    value_str = f"'{value}'"
                else:
                    value_str = str(value)
                expression = re.sub(rf'\b{col_name}\b', value_str, expression)
            
            expression = expression.replace('=', '==').replace('AND', 'and').replace('OR', 'or')
            return eval(expression, {"__builtins__": {}}, {})
        except:
            return False
    
    def create_index(self, column_name: str):
        if column_name not in self.indexes:
            self.indexes[column_name] = Index(column_name)
            for i, row in enumerate(self.rows, 1):
                if column_name in row:
                    self.indexes[column_name].add(row[column_name], i)


class Database:
    def __init__(self, name: str = "pesapal_db"):
        self.name = name
        self.tables: Dict[str, Table] = {}
    
    def execute_sql(self, sql: str) -> Any:
        sql = sql.strip().upper()
        
        if sql.startswith("CREATE TABLE"):
            return self._parse_create_table(sql)
        elif sql.startswith("INSERT INTO"):
            return self._parse_insert(sql)
        elif sql.startswith("SELECT"):
            return self._parse_select(sql)
        elif sql.startswith("UPDATE"):
            return self._parse_update(sql)
        elif sql.startswith("DELETE"):
            return self._parse_delete(sql)
        elif sql.startswith("DROP TABLE"):
            return self._parse_drop_table(sql)
        elif sql.startswith("CREATE INDEX"):
            return self._parse_create_index(sql)
        else:
            raise ValueError(f"Unsupported SQL: {sql}")
    
    def _parse_create_table(self, sql: str):
        pattern = r'CREATE TABLE (\w+)\s*\((.*)\)'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError("Invalid CREATE TABLE")
        
        table_name = match.group(1)
        columns_sql = match.group(2).strip()
        
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} exists")
        
        columns = []
        for col_def in columns_sql.split(','):
            col_def = col_def.strip()
            parts = col_def.split()
            col_name = parts[0]
            col_type = parts[1].upper()
            
            is_primary = "PRIMARY KEY" in col_def.upper()
            is_unique = "UNIQUE" in col_def.upper()
            nullable = "NOT NULL" not in col_def.upper()
            
            columns.append(Column(col_name, col_type, is_primary, is_unique, nullable))
        
        table = Table(table_name)
        for col in columns:
            table.add_column(col)
        
        self.tables[table_name] = table
        return table
    
    def _parse_insert(self, sql: str) -> int:
        pattern = r'INSERT INTO (\w+)\s*\((.*?)\)\s*VALUES\s*\((.*)\)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        columns = [col.strip() for col in columns_str.split(',')]
        values = self._parse_values(values_str)
        
        row_data = dict(zip(columns, values))
        return self.tables[table_name].insert(row_data)
    
    def _parse_select(self, sql: str) -> List[Dict]:
        pattern = r'SELECT (.*?) FROM (\w+)(?: WHERE (.*))?'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid SELECT")
        
        columns_str = match.group(1)
        table_name = match.group(2)
        where_clause = match.group(3)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        table = self.tables[table_name]
        
        if columns_str == "*":
            return table.select(where_clause)
        else:
            selected = [col.strip() for col in columns_str.split(',')]
            results = table.select(where_clause)
            return [{col: row.get(col) for col in selected} for row in results]
    
    def _parse_update(self, sql: str) -> int:
        pattern = r'UPDATE (\w+) SET (.*?)(?: WHERE (.*))?'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid UPDATE")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        updates = {}
        for assignment in set_clause.split(','):
            col, value = assignment.split('=')
            updates[col.strip()] = self._parse_value(value.strip())
        
        return self.tables[table_name].update(updates, where_clause)
    
    def _parse_delete(self, sql: str) -> int:
        pattern = r'DELETE FROM (\w+)(?: WHERE (.*))?'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DELETE")
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        return self.tables[table_name].delete(where_clause)
    
    def _parse_drop_table(self, sql: str):
        pattern = r'DROP TABLE (\w+)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DROP TABLE")
        
        table_name = match.group(1)
        if table_name in self.tables:
            del self.tables[table_name]
    
    def _parse_create_index(self, sql: str):
        pattern = r'CREATE INDEX \w+ ON (\w+)\s*\((\w+)\)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE INDEX")
        
        table_name = match.group(1)
        column_name = match.group(2)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        self.tables[table_name].create_index(column_name)
    
    def _parse_values(self, values_str: str) -> List[Any]:
        values = []
        current = ""
        in_quotes = False
        
        for char in values_str:
            if char == "'" and not in_quotes:
                in_quotes = True
            elif char == "'" and in_quotes:
                in_quotes = False
            elif char == ',' and not in_quotes:
                values.append(self._parse_value(current.strip()))
                current = ""
                continue
            current += char
        
        if current:
            values.append(self._parse_value(current.strip()))
        return values
    
    def _parse_value(self, value_str: str) -> Any:
        if value_str.upper() == "NULL":
            return None
        elif value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1]
        elif value_str.upper() in ("TRUE", "FALSE"):
            return value_str.upper() == "TRUE"
        elif '.' in value_str:
            try:
                return float(value_str)
            except:
                return value_str
        else:
            try:
                return int(value_str)
            except:
                return value_str
    
    def join(self, table1: str, table2: str, on_clause: str) -> List[Dict]:
        if table1 not in self.tables or table2 not in self.tables:
            raise ValueError("Table(s) not found")
        
        t1 = self.tables[table1]
        t2 = self.tables[table2]
        
        pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        match = re.match(pattern, on_clause)
        if not match:
            raise ValueError("Invalid JOIN")
        
        t1_name, t1_col, t2_name, t2_col = match.groups()
        
        results = []
        for row1 in t1.select():
            for row2 in t2.select():
                if row1.get(t1_col) == row2.get(t2_col):
                    merged = {f"{table1}.{k}": v for k, v in row1.items()}
                    merged.update({f"{table2}.{k}": v for k, v in row2.items()})
                    results.append(merged)
        return results
    
    def get_schema(self) -> Dict:
        return {
            'name': self.name,
            'tables': {
                name: {
                    'columns': [
                        {
                            'name': col.name,
                            'type': col.data_type,
                            'primary': col.is_primary,
                            'unique': col.is_unique,
                            'nullable': col.nullable
                        }
                        for col in table.columns
                    ],
                    'row_count': table.row_count
                }
                for name, table in self.tables.items()
            }
        }


class REPL:
    def __init__(self, db: Database):
        self.db = db
    
    def run(self):
        print(f"Pesapal RDBMS REPL ({self.db.name})")
        print("Type SQL commands or HELP/EXIT")
        
        while True:
            try:
                cmd = input("SQL> ").strip()
                if not cmd:
                    continue
                
                if cmd.upper() == "EXIT":
                    break
                elif cmd.upper() == "HELP":
                    self._show_help()
                elif cmd.upper() == "SCHEMA":
                    self._show_schema()
                else:
                    result = self.db.execute_sql(cmd)
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
                                print("No rows")
                        else:
                            print(f"Result: {result}")
                    
            except Exception as e:
                print(f"Error: {e}")
    
    def _show_help(self):
        print("""
SQL Commands:
  CREATE TABLE name (col TYPE [PRIMARY KEY|UNIQUE|NOT NULL], ...)
  INSERT INTO name (col1, col2) VALUES (val1, val2)
  SELECT * FROM name [WHERE condition]
  UPDATE name SET col=val [WHERE condition]
  DELETE FROM name [WHERE condition]
  DROP TABLE name
  CREATE INDEX idx ON name(col)

Special:
  HELP    - This help
  EXIT    - Quit REPL
  SCHEMA  - Show database schema
        """)
    
    def _show_schema(self):
        schema = self.db.get_schema()
        print(f"Database: {schema['name']}")
        for table_name, info in schema['tables'].items():
            print(f"\n{table_name} ({info['row_count']} rows)")
            for col in info['columns']:
                constr = []
                if col['primary']:
                    constr.append("PK")
                if col['unique']:
                    constr.append("UNIQUE")
                if not col['nullable']:
                    constr.append("NOT NULL")
                constr_str = f" ({', '.join(constr)})" if constr else ""
                print(f"  {col['name']}: {col['type']}{constr_str}")



    def save_to_file(self, filename="db.pesapal"):
        """Save entire database to a file"""
        import pickle
        
        data = {
            'name': self.name,
            'tables': {}
        }
        
        for table_name, table in self.tables.items():
            # Get table data
            table_data = {
                'columns': [],
                'rows': table.rows,
                'row_count': table.row_count
            }
            
            # Save column definitions
            for col in table.columns:
                table_data['columns'].append({
                    'name': col.name,
                    'data_type': col.data_type,
                    'is_primary': col.is_primary,
                    'is_unique': col.is_unique,
                    'nullable': col.nullable
                })
            
            data['tables'][table_name] = table_data
        
        # Save to file
        with open(filename, 'wb') as f:
            pickle.dump(data, f)
        
        print(f"✓ Database saved to {filename}")
    
    def load_from_file(self, filename="db.pesapal"):
        """Load database from file"""
        import pickle
        import os
        
        if not os.path.exists(filename):
            print(f"✗ File {filename} not found")
            return False
        
        try:
            with open(filename, 'rb') as f:
                data = pickle.load(f)
            
            self.name = data['name']
            self.tables = {}
            
            for table_name, table_data in data['tables'].items():
                # Recreate table
                table = Table(table_name)
                
                # Recreate columns
                for col_data in table_data['columns']:
                    column = Column(
                        name=col_data['name'],
                        data_type=col_data['data_type'],
                        is_primary=col_data['is_primary'],
                        is_unique=col_data['is_unique'],
                        nullable=col_data['nullable']
                    )
                    table.add_column(column)
                
                # Restore rows
                table.rows = table_data['rows']
                table.row_count = table_data['row_count']
                
                # Rebuild indexes
                for col in table.columns:
                    if col.is_primary or col.is_unique:
                        table.create_index(col.name)
                
                self.tables[table_name] = table
            
            print(f"✓ Database loaded from {filename}")
            return True
            
        except Exception as e:
            print(f"✗ Error loading database: {e}")
            return False