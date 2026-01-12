# pesapal_app/rdbms_core.py
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
        print(f"DEBUG DataType.validate: Checking type '{data_type}' for value '{value}' (type: {type(value)})")
        
        if value is None:
            print(f"DEBUG DataType.validate: Value is None, returning True")
            return True
        
        # Special handling for INTEGER type
        if data_type == DataType.INTEGER:
            # Allow integers, strings that can be converted to integers, or numeric strings
            if isinstance(value, int):
                print(f"DEBUG DataType.validate: INTEGER - value is int: True")
                return True
            elif isinstance(value, str):
                # Check if it's a string that can be converted to int
                if value.isdigit():
                    print(f"DEBUG DataType.validate: INTEGER - string is digits: True")
                    return True
                # Also check for negative numbers
                if value.startswith('-') and value[1:].isdigit():
                    print(f"DEBUG DataType.validate: INTEGER - string is negative digits: True")
                    return True
                # Check if it's a string representation of a number
                try:
                    int(value)
                    print(f"DEBUG DataType.validate: INTEGER - string can be converted to int: True")
                    return True
                except ValueError:
                    print(f"DEBUG DataType.validate: INTEGER - string cannot be converted to int: False")
                    return False
            elif isinstance(value, float) and value.is_integer():
                print(f"DEBUG DataType.validate: INTEGER - value is float but integer: True")
                return True
            print(f"DEBUG DataType.validate: INTEGER - all checks failed: False")
            return False
        
        elif data_type == DataType.TEXT:
            result = isinstance(value, str)
            print(f"DEBUG DataType.validate: TEXT - is string: {result}")
            return result
        elif data_type == DataType.REAL:
            result = isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit())
            print(f"DEBUG DataType.validate: REAL - is numeric: {result}")
            return result
        elif data_type == DataType.BOOLEAN:
            result = isinstance(value, bool) or value in (0, 1, '0', '1', True, False, 'TRUE', 'FALSE', 'true', 'false')
            print(f"DEBUG DataType.validate: BOOLEAN - is boolean: {result}")
            return result
        elif data_type == DataType.DATE:
            result = isinstance(value, str)
            print(f"DEBUG DataType.validate: DATE - is string: {result}")
            return result
        
        print(f"DEBUG DataType.validate: Unknown data type '{data_type}', returning False")
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
        # Add error tracking for duplicate values
        self.unique_constraints: Dict[str, set] = {}
    
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
                
                # Check unique constraints
                if col.is_unique or col.is_primary:
                    value = values[col.name]
                    if value is not None:
                        # Check if value already exists
                        unique_set = self.unique_values.get(col.name, set())
                        if value in unique_set:
                            raise ValueError(f"Duplicate value '{value}' for {col.name}")
                
                row_data[col.name] = values[col.name]
            elif col.is_primary and col.data_type == DataType.INTEGER:
                # Auto-generate primary key
                next_id = self.row_count + 1
                row_data[col.name] = next_id
            else:
                row_data[col.name] = None
        
        # Check unique constraints (again for auto-generated values)
        for col in self.columns:
            if (col.is_primary or col.is_unique) and col.name in row_data:
                value = row_data[col.name]
                if value is not None:
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
        row_indices_to_update = []
        
        # First, find which rows to update
        for i, row in enumerate(self.rows):
            if not where_clause or self._evaluate_where(row, where_clause):
                row_indices_to_update.append(i)
        
        # Check uniqueness constraints before updating
        for col in self.columns:
            if col.is_unique and col.name in values:
                new_value = values[col.name]
                if new_value is not None:
                    # Check if this value already exists in other rows
                    for i in row_indices_to_update:
                        existing_value = self.rows[i].get(col.name)
                        if existing_value != new_value:  # Only check if value is changing
                            # Look for this value in other rows
                            for j, other_row in enumerate(self.rows):
                                if j not in row_indices_to_update:  # Not in rows being updated
                                    if other_row.get(col.name) == new_value:
                                        raise ValueError(f"Duplicate value '{new_value}' for {col.name}")
        
        # Now perform the updates
        for i in row_indices_to_update:
            row = self.rows[i]
            row_id = i + 1
            
            for col_name, value in values.items():
                if col_name in row:
                    # Store old value for index cleanup
                    old_value = row[col_name]
                    
                    # Update the value
                    row[col_name] = value
                    
                    # Update unique values set
                    if col_name in self.unique_values:
                        if old_value is not None and old_value in self.unique_values[col_name]:
                            self.unique_values[col_name].remove(old_value)
                        if value is not None:
                            self.unique_values[col_name].add(value)
                    
                    # Update indexes
                    if col_name in self.indexes:
                        self.indexes[col_name].remove(old_value, row_id)
                        self.indexes[col_name].add(value, row_id)
            
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
            # Make case-insensitive
            expression = where_clause.upper()
            
            for col_name, value in row.items():
                if isinstance(value, str):
                    value_str = f"'{value}'"
                else:
                    value_str = str(value)
                
                # Replace column name (case-insensitive)
                col_name_upper = col_name.upper()
                expression = re.sub(rf'\b{re.escape(col_name_upper)}\b', value_str, expression)
            
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
        sql = self._clean_sql(sql)  # Clean first
        sql_upper = sql.upper()  # Only uppercase for checking command type
        
        if sql_upper.startswith("CREATE TABLE"):
            return self._parse_create_table(sql)
        elif sql_upper.startswith("ALTER TABLE"):
            return self._parse_alter_table(sql)  # NEW
        elif sql_upper.startswith("INSERT INTO"):
            return self._parse_insert(sql)
        elif sql_upper.startswith("SELECT"):
            return self._parse_select(sql)
        elif sql_upper.startswith("UPDATE"):
            return self._parse_update(sql)
        elif sql_upper.startswith("DELETE"):
            return self._parse_delete(sql)
        elif sql_upper.startswith("DROP TABLE"):
            return self._parse_drop_table(sql)
        elif sql_upper.startswith("CREATE INDEX"):
            return self._parse_create_index(sql)
        else:
            raise ValueError(f"Unsupported SQL: {sql}")
        
    def _parse_alter_table(self, sql: str):
        """Parse ALTER TABLE ADD COLUMN"""
        pattern = r'ALTER TABLE\s+(\w+)\s+ADD COLUMN\s+(\w+)\s+(\w+)'
        match = re.match(pattern, sql, re.IGNORECASE)
        
        if not match:
            raise ValueError(f"Invalid ALTER TABLE: {sql}")
        
        table_name = match.group(1)
        column_name = match.group(2)
        column_type = match.group(3).upper()
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        table = self.tables[table_name]
        
        # Check if column already exists
        for col in table.columns:
            if col.name.lower() == column_name.lower():
                raise ValueError(f"Column {column_name} already exists in {table_name}")
        
        # Create and add column
        new_column = Column(column_name, column_type, False, False, True)
        table.add_column(new_column)
        
        # Add null values to existing rows
        for row in table.rows:
            row[column_name] = None
        
        print(f"✓ Added column '{column_name}' to table '{table_name}'")
        return True
    
    def _parse_create_table(self, sql: str):
        # Improved pattern to handle different CREATE TABLE formats
        pattern = r'CREATE TABLE\s+(\w+)\s*\((.*)\)'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise ValueError(f"Invalid CREATE TABLE syntax: {sql}")
        
        table_name = match.group(1)
        columns_sql = match.group(2).strip()
        
        print(f"DEBUG: Table name: {table_name}")
        print(f"DEBUG: Columns SQL: {columns_sql}")
        
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")
        
        columns = []
        # Split by commas, but handle commas inside parentheses
        column_defs = []
        current_def = ""
        paren_depth = 0
        
        for char in columns_sql:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                column_defs.append(current_def.strip())
                current_def = ""
                continue
            current_def += char
        
        if current_def.strip():
            column_defs.append(current_def.strip())
        
        print(f"DEBUG: Column definitions: {column_defs}")
        
        for col_def in column_defs:
            if not col_def:
                continue
                
            col_def = col_def.strip()
            print(f"DEBUG: Processing column: '{col_def}'")
            
            # Split by spaces, but handle quoted names
            parts = []
            current_part = ""
            in_quotes = False
            in_parentheses = False
            
            for char in col_def:
                if char == '"' or char == "'":
                    in_quotes = not in_quotes
                    current_part += char
                elif char == '(' and not in_quotes:
                    in_parentheses = True
                    current_part += char
                elif char == ')' and not in_quotes:
                    in_parentheses = False
                    current_part += char
                elif char == ' ' and not in_quotes and not in_parentheses:
                    if current_part:
                        parts.append(current_part)
                        current_part = ""
                else:
                    current_part += char
            
            if current_part:
                parts.append(current_part)
            
            print(f"DEBUG: Column parts: {parts}")
            
            if len(parts) < 2:
                raise ValueError(f"Invalid column definition: {col_def}")
            
            col_name = parts[0].strip('"').strip("'")
            
            # Parse data type - handle INT, INTEGER, TEXT, etc.
            col_type = parts[1].upper()
            
            # Standardize data types
            if col_type == "INT":
                col_type = DataType.INTEGER
            elif col_type == "VARCHAR" or col_type.startswith("VARCHAR"):
                col_type = DataType.TEXT
            elif col_type == "FLOAT" or col_type == "DOUBLE":
                col_type = DataType.REAL
            elif col_type == "BOOL":
                col_type = DataType.BOOLEAN
            
            # Check for constraints
            is_primary = False
            is_unique = False
            nullable = True
            
            for i in range(2, len(parts)):
                constraint = parts[i].upper()
                if constraint == "PRIMARY" and i + 1 < len(parts) and parts[i + 1].upper() == "KEY":
                    is_primary = True
                elif constraint == "PRIMARY_KEY":
                    is_primary = True
                elif constraint == "UNIQUE":
                    is_unique = True
                elif constraint == "NOT" and i + 1 < len(parts) and parts[i + 1].upper() == "NULL":
                    nullable = False
                elif constraint == "NOT_NULL":
                    nullable = False
            
            print(f"DEBUG: Creating column: name={col_name}, type={col_type}, "
                  f"primary={is_primary}, unique={is_unique}, nullable={nullable}")
            
            columns.append(Column(col_name, col_type, is_primary, is_unique, nullable))
        
        # Create the table
        table = Table(table_name)
        for col in columns:
            table.add_column(col)
        
        self.tables[table_name] = table
        print(f"✓ Created table '{table_name}' with {len(columns)} columns")
        
        # Return schema info
        return {
            'table': table_name,
            'columns': len(columns),
            'schema': [{'name': col.name, 'type': col.data_type} for col in columns]
        }
    
    def _parse_insert(self, sql: str) -> int:
        # Handle multi-line SQL
        sql = ' '.join(sql.replace('\n', ' ').split())
        
        pattern = r'INSERT INTO (\w+)\s*\((.*?)\)\s*VALUES\s*\((.*)\)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid INSERT: {sql}")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        columns = [col.strip() for col in columns_str.split(',')]
        values = self._parse_values(values_str)
        
        if len(columns) != len(values):
            raise ValueError(f"Column count ({len(columns)}) doesn't match value count ({len(values)})")
        
        row_data = dict(zip(columns, values))
        return self.tables[table_name].insert(row_data)
    
    def _parse_select(self, sql: str) -> List[Dict]:
        # Updated pattern to handle ORDER BY and LIMIT
        pattern = r'SELECT (.*?) FROM (\w+)(?: WHERE (.*?))?(?: ORDER BY (.*?))?(?: LIMIT (\d+))?$'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid SELECT: {sql}")
        
        columns_str = match.group(1)
        table_name = match.group(2)
        where_clause = match.group(3)
        order_by = match.group(4)
        limit_str = match.group(5)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        table = self.tables[table_name]
        
        if columns_str == "*":
            results = table.select(where_clause)
        else:
            selected = [col.strip() for col in columns_str.split(',')]
            results = table.select(where_clause)
            results = [{col: row.get(col) for col in selected} for row in results]
        
        # Apply ORDER BY if specified
        if order_by:
            # Parse order by clause (simple: "column" or "column DESC")
            order_parts = order_by.strip().split()
            column = order_parts[0]
            descending = len(order_parts) > 1 and order_parts[1].upper() == 'DESC'
            
            def sort_key(row):
                value = row.get(column)
                # Handle None values
                if value is None:
                    return '' if not descending else 'ZZZZZZ'
                return str(value)
            
            results.sort(key=sort_key, reverse=descending)
        
        # Apply LIMIT if specified
        if limit_str:
            limit = int(limit_str)
            results = results[:limit]
        
        return results
    
    def _parse_update(self, sql: str) -> int:
        # Clean the SQL first
        sql = self._clean_sql(sql)
        pattern = r'UPDATE (\w+) SET (.*?)(?: WHERE (.*))?$'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid UPDATE: {sql}")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        
        updates = {}
        # Split by comma, but handle commas inside quotes
        assignments = []
        current = ""
        in_quotes = False
        
        for char in set_clause:
            if char == "'" and not in_quotes:
                in_quotes = True
            elif char == "'" and in_quotes:
                in_quotes = False
            elif char == ',' and not in_quotes:
                assignments.append(current.strip())
                current = ""
                continue
            current += char
        
        if current:
            assignments.append(current.strip())
        
        for assignment in assignments:
            if assignment and '=' in assignment:
                parts = assignment.split('=', 1)  # Split only on first '='
                if len(parts) == 2:
                    col = parts[0].strip()
                    value = parts[1].strip()
                    updates[col] = self._parse_value(value)
                else:
                    raise ValueError(f"Invalid assignment: {assignment}")
        
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
    
    def join(self, table1: str, table2: str, on_clause: str, join_type: str = "INNER") -> List[Dict]:
        """
        Perform JOIN operation between two tables
        
        Args:
            table1: First table name
            table2: Second table name
            on_clause: JOIN condition (e.g., "users.id = orders.user_id")
            join_type: Type of join - "INNER", "LEFT", "RIGHT", "FULL", "CROSS"
        
        Returns:
            List of joined rows
        """
        if table1 not in self.tables or table2 not in self.tables:
            raise ValueError("Table(s) not found")
        
        t1 = self.tables[table1]
        t2 = self.tables[table2]
        
        # Parse ON clause
        pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        match = re.match(pattern, on_clause)
        if not match:
            raise ValueError("Invalid JOIN condition. Use format: table1.column = table2.column")
        
        t1_name, t1_col, t2_name, t2_col = match.groups()
        
        results = []
        
        if join_type.upper() == "INNER":
            # INNER JOIN: Only rows with matches in both tables
            for row1 in t1.select():
                for row2 in t2.select():
                    if row1.get(t1_col) == row2.get(t2_col):
                        merged = self._merge_rows(table1, row1, table2, row2)
                        results.append(merged)
        
        elif join_type.upper() == "LEFT":
            # LEFT JOIN: All rows from table1, matched rows from table2
            for row1 in t1.select():
                matched = False
                for row2 in t2.select():
                    if row1.get(t1_col) == row2.get(t2_col):
                        merged = self._merge_rows(table1, row1, table2, row2)
                        results.append(merged)
                        matched = True
                
                # If no match, include row1 with nulls for table2
                if not matched:
                    merged = self._merge_rows(table1, row1, table2, {})
                    results.append(merged)
        
        elif join_type.upper() == "RIGHT":
            # RIGHT JOIN: All rows from table2, matched rows from table1
            for row2 in t2.select():
                matched = False
                for row1 in t1.select():
                    if row1.get(t1_col) == row2.get(t2_col):
                        merged = self._merge_rows(table1, row1, table2, row2)
                        results.append(merged)
                        matched = True
                
                # If no match, include row2 with nulls for table1
                if not matched:
                    merged = self._merge_rows(table1, {}, table2, row2)
                    results.append(merged)
        
        elif join_type.upper() == "FULL":
            # FULL OUTER JOIN: All rows from both tables
            # We'll implement as LEFT JOIN + unmatched RIGHT rows
            left_results = self.join(table1, table2, on_clause, "LEFT")
            
            # Get rows from table2 that don't have matches in table1
            for row2 in t2.select():
                has_match = False
                for result in left_results:
                    if result.get(f"{table2}.{t2_col}") == row2.get(t2_col):
                        has_match = True
                        break
                
                if not has_match:
                    merged = self._merge_rows(table1, {}, table2, row2)
                    results.append(merged)
            
            results.extend(left_results)
        
        elif join_type.upper() == "CROSS":
            # CROSS JOIN: All possible combinations
            for row1 in t1.select():
                for row2 in t2.select():
                    merged = self._merge_rows(table1, row1, table2, row2)
                    results.append(merged)
        
        else:
            raise ValueError(f"Unsupported JOIN type: {join_type}. Use INNER, LEFT, RIGHT, FULL, or CROSS")
        
        return results
    
    def _merge_rows(self, table1: str, row1: Dict, table2: str, row2: Dict) -> Dict:
        """Merge two rows with table prefixes"""
        merged = {}
        
        # Add table1 columns with prefix
        for key, value in row1.items():
            merged[f"{table1}.{key}"] = value
        
        # Add table2 columns with prefix
        for key, value in row2.items():
            merged[f"{table2}.{key}"] = value
        
        return merged
    
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
    def save_to_file(self, filename="db.pesapal"):
        """Save entire database to a file"""
        import pickle
        import os
        
        # Prepare data for saving
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
        try:
            with open(filename, 'wb') as f:
                pickle.dump(data, f)
            print(f"✓ Database saved to {filename}")
            return True
        except Exception as e:
            print(f"✗ Error saving database: {e}")
            return False
    
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
            
            # Clear existing data
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

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL by removing extra whitespace and newlines"""
        # Replace newlines with spaces and normalize whitespace
        sql = ' '.join(sql.replace('\n', ' ').split())
        return sql.strip()

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