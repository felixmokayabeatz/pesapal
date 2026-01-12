"""
Django models that interface with our custom RDBMS
"""
from .rdbms_core import Database, Column, DataType


class RDBMSWrapper:
    _instance = None
    
    @classmethod
    def get_db(cls):
        if cls._instance is None:
            cls._instance = Database("pesapal_db")
            
            # Try to load from file
            if not cls._instance.load_from_file():
                print("No db.pesapal file found, creating new database...")
                # Create tables
                cls._create_tables(cls._instance)
                # Save immediately
                cls._instance.save_to_file()
        
        return cls._instance
    
    @classmethod
    def _create_tables(cls, db):
        """Create initial tables"""
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
            print("✓ Created 'users' table")
        except Exception as e:
            print(f"Note: {e}")
        
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
            print("✓ Created 'products' table")
        except Exception as e:
            print(f"Note: {e}")
        
        try:
            # Insert sample users
            db.execute_sql("INSERT INTO users (name, email, age, created_at) VALUES ('John Doe', 'john@example.com', 25, '2024-01-15')")
            db.execute_sql("INSERT INTO users (name, email, age, created_at) VALUES ('Jane Smith', 'jane@example.com', 30, '2024-01-16')")
            print("✓ Added sample users")
        except Exception as e:
            print(f"Note: {e}")
        
        try:
            # Insert sample products
            db.execute_sql("INSERT INTO products (name, price, in_stock, category) VALUES ('Laptop', 999.99, TRUE, 'Electronics')")
            db.execute_sql("INSERT INTO products (name, price, in_stock, category) VALUES ('Book', 19.99, TRUE, 'Books')")
            db.execute_sql("INSERT INTO products (name, price, in_stock, category) VALUES ('Headphones', 79.99, FALSE, 'Electronics')")
            print("✓ Added sample products")
        except Exception as e:
            print(f"Note: {e}")
    
    @classmethod
    def save_db(cls):
        """Save database to file"""
        if cls._instance:
            cls._instance.save_to_file()
            return True
        return False


class User:
    def __init__(self, id=None, ID=None, name=None, NAME=None, email=None, EMAIL=None, 
                 age=None, AGE=None, created_at=None, CREATED_AT=None, _id=None):
        # Handle all possible column name variations
        self.id = id or ID or _id
        self.name = name or NAME
        self.email = email or EMAIL
        self.age = age or AGE
        self.created_at = created_at or CREATED_AT
    
    # ... rest of the class stays the same
    
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


# In the UserManager class, fix the all() method:

class UserManager:
    def all(self):
        db = RDBMSWrapper.get_db()
        results = db.execute_sql("SELECT * FROM users")
        
        # Convert results to User objects, handling all column name variations
        users = []
        for row in results:
            # Create a dictionary with lowercase keys
            user_data = {}
            for key, value in row.items():
                # Convert any key to lowercase for consistency
                lower_key = key.lower().replace('_', '')
                user_data[lower_key] = value
            
            # Also handle the original keys
            user_data.update(row)
            
            try:
                user = User(**user_data)
                users.append(user)
            except Exception as e:
                # Fallback: create User with extracted values
                user = User()
                user.id = row.get('ID') or row.get('id') or row.get('_id')
                user.name = row.get('NAME') or row.get('name')
                user.email = row.get('EMAIL') or row.get('email')
                user.age = row.get('AGE') or row.get('age')
                user.created_at = row.get('CREATED_AT') or row.get('created_at') or row.get('createdat')
                users.append(user)
        
        return users

class Product:
    def __init__(self, id=None, ID=None, name=None, NAME=None, price=None, PRICE=None, 
                 in_stock=None, IN_STOCK=None, category=None, CATEGORY=None, _id=None):
        # Handle all possible column name variations
        self.id = id or ID or _id
        self.name = name or NAME
        self.price = price or PRICE
        self.in_stock = in_stock or IN_STOCK
        self.category = category or CATEGORY
    
    @classmethod
    def objects(cls):
        return ProductManager()
    
    def __str__(self):
        return f"Product: {self.name} (${self.price})"



    
    # ... rest of the methods stay the same

# Also update ProductManager similarly:
class ProductManager:
    def all(self):
        db = RDBMSWrapper.get_db()
        results = db.execute_sql("SELECT * FROM products")
        
        products = []
        for row in results:
            # Create a dictionary with lowercase keys
            product_data = {}
            for key, value in row.items():
                lower_key = key.lower().replace('_', '')
                product_data[lower_key] = value
            
            product_data.update(row)
            
            try:
                product = Product(**product_data)
                products.append(product)
            except Exception as e:
                # Fallback
                product = Product()
                product.id = row.get('ID') or row.get('id') or row.get('_id')
                product.name = row.get('NAME') or row.get('name')
                product.price = row.get('PRICE') or row.get('price')
                product.in_stock = row.get('IN_STOCK') or row.get('in_stock') or row.get('instock')
                product.category = row.get('CATEGORY') or row.get('category')
                products.append(product)
        
        return products