"""
Django models that interface with our custom RDBMS
"""
from .rdbms_core import Database, Column, DataType


class RDBMSWrapper:
    """Wrapper to use our RDBMS with Django"""
    _instance = None
    
    @classmethod
    def get_db(cls):
        if cls._instance is None:
            cls._instance = Database("pesapal_db")
            cls._init_sample_data(cls._instance)
        return cls._instance
    
    @classmethod
    def _init_sample_data(cls, db):
        """Initialize with sample tables if they don't exist"""
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
            
            # Insert sample users
            db.execute_sql("INSERT INTO users (name, email, age, created_at) VALUES ('John Doe', 'john@example.com', 25, '2024-01-15')")
            db.execute_sql("INSERT INTO users (name, email, age, created_at) VALUES ('Jane Smith', 'jane@example.com', 30, '2024-01-16')")
            
        except Exception:
            # Tables might already exist
            pass
        
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
            
            # Insert sample products
            db.execute_sql("INSERT INTO products (name, price, in_stock, category) VALUES ('Laptop', 999.99, TRUE, 'Electronics')")
            db.execute_sql("INSERT INTO products (name, price, in_stock, category) VALUES ('Book', 19.99, TRUE, 'Books')")
            db.execute_sql("INSERT INTO products (name, price, in_stock, category) VALUES ('Headphones', 79.99, FALSE, 'Electronics')")
            
        except Exception:
            # Tables might already exist
            pass


# Django model-like classes that use our RDBMS
class User:
    def __init__(self, id=None, name=None, email=None, age=None, created_at=None):
        self.id = id
        self.name = name
        self.email = email
        self.age = age
        self.created_at = created_at
    
    @classmethod
    def objects(cls):
        return UserManager()
    
    def save(self):
        db = RDBMSWrapper.get_db()
        if self.id:
            # Update
            db.execute_sql(f"""
                UPDATE users 
                SET name = '{self.name}', 
                    email = '{self.email}', 
                    age = {self.age or 'NULL'}
                WHERE id = {self.id}
            """)
        else:
            # Insert
            result = db.execute_sql(f"""
                INSERT INTO users (name, email, age, created_at) 
                VALUES ('{self.name}', '{self.email}', {self.age or 'NULL'}, '{self.created_at or '2024-01-01'}')
            """)
            self.id = result
        return self
    
    def __str__(self):
        return f"User: {self.name} ({self.email})"


class UserManager:
    def all(self):
        db = RDBMSWrapper.get_db()
        results = db.execute_sql("SELECT * FROM users")
        return [User(**row) for row in results]
    
    def filter(self, **kwargs):
        db = RDBMSWrapper.get_db()
        where_parts = []
        for key, value in kwargs.items():
            if isinstance(value, str):
                where_parts.append(f"{key} = '{value}'")
            else:
                where_parts.append(f"{key} = {value}")
        
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        sql = f"SELECT * FROM users WHERE {where_clause}"
        results = db.execute_sql(sql)
        return [User(**row) for row in results]
    
    def get(self, **kwargs):
        results = self.filter(**kwargs)
        if results:
            return results[0]
        return None


class Product:
    def __init__(self, id=None, name=None, price=None, in_stock=None, category=None):
        self.id = id
        self.name = name
        self.price = price
        self.in_stock = in_stock
        self.category = category
    
    @classmethod
    def objects(cls):
        return ProductManager()
    
    def __str__(self):
        return f"Product: {self.name} (${self.price})"


class ProductManager:
    def all(self):
        db = RDBMSWrapper.get_db()
        results = db.execute_sql("SELECT * FROM products")
        return [Product(**row) for row in results]
    
    def filter(self, **kwargs):
        db = RDBMSWrapper.get_db()
        where_parts = []
        for key, value in kwargs.items():
            if isinstance(value, str):
                where_parts.append(f"{key} = '{value}'")
            else:
                where_parts.append(f"{key} = {value}")
        
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        sql = f"SELECT * FROM products WHERE {where_clause}"
        results = db.execute_sql(sql)
        return [Product(**row) for row in results]