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
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER,
                    created_at TEXT DEFAULT '2024-01-01'
                )
            """)
            print("✓ Created 'users' table")
        except Exception as e:
            print(f"Note: {e}")
        
        try:
            # Create products table
            db.execute_sql("""
                CREATE TABLE IF NOT EXISTS products (
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
        
        # Don't auto-add sample data - let users do it
    
    @classmethod
    def save_db(cls):
        """Save database to file"""
        if cls._instance:
            cls._instance.save_to_file()
            return True
        return False


class UserManager:
    """Manager for User model"""
    
    def __init__(self):
        self.db = RDBMSWrapper.get_db()
    
    def all(self):
        """Get all users"""
        try:
            results = self.db.execute_sql("SELECT * FROM users ORDER BY id")
            users = []
            for row in results:
                # Convert all keys to lowercase for consistency
                user_data = {}
                for key, value in row.items():
                    user_data[key.lower().replace('_', '')] = value
                user_data.update(row)  # Keep original keys too
                
                try:
                    user = User(**user_data)
                    users.append(user)
                except:
                    # Fallback creation
                    user = User()
                    user.id = row.get('ID') or row.get('id') or row.get('_id')
                    user.name = row.get('NAME') or row.get('name')
                    user.email = row.get('EMAIL') or row.get('email')
                    user.age = row.get('AGE') or row.get('age')
                    user.created_at = row.get('CREATED_AT') or row.get('created_at') or row.get('createdat')
                    users.append(user)
            return users
        except Exception as e:
            print(f"Error getting users: {e}")
            return []
    
    def filter(self, **kwargs):
        """Filter users by conditions"""
        try:
            where_parts = []
            for key, value in kwargs.items():
                if isinstance(value, str):
                    where_parts.append(f"{key} = '{value}'")
                elif value is None:
                    where_parts.append(f"{key} IS NULL")
                else:
                    where_parts.append(f"{key} = {value}")
            
            where_clause = " AND ".join(where_parts) if where_parts else "1=1"
            sql = f"SELECT * FROM users WHERE {where_clause} ORDER BY id"
            
            results = self.db.execute_sql(sql)
            users = []
            for row in results:
                user_data = {}
                for key, value in row.items():
                    user_data[key.lower().replace('_', '')] = value
                user_data.update(row)
                
                try:
                    user = User(**user_data)
                    users.append(user)
                except:
                    user = User()
                    user.id = row.get('ID') or row.get('id') or row.get('_id')
                    user.name = row.get('NAME') or row.get('name')
                    user.email = row.get('EMAIL') or row.get('email')
                    user.age = row.get('AGE') or row.get('age')
                    user.created_at = row.get('CREATED_AT') or row.get('created_at') or row.get('createdat')
                    users.append(user)
            return users
        except Exception as e:
            print(f"Error filtering users: {e}")
            return []
    
    def get(self, **kwargs):
        """Get a single user matching conditions"""
        print(f"DEBUG UserManager.get(): kwargs={kwargs}")
        users = self.filter(**kwargs)
        print(f"DEBUG UserManager.get(): found {len(users)} users")
        if users:
            user = users[0]
            print(f"DEBUG UserManager.get(): returning user id={user.id}, name={user.name}")
            return user
        else:
            print(f"DEBUG UserManager.get(): no user found")
            return None


class User:
    """User model"""
    
    def __init__(self, id=None, ID=None, name=None, NAME=None, email=None, EMAIL=None, 
                 age=None, AGE=None, created_at=None, CREATED_AT=None, _id=None):
        # Handle all possible column name variations
        self.id = id or ID or _id
        self.name = name or NAME or ''
        self.email = email or EMAIL or ''
        self.age = age or AGE
        self.created_at = created_at or CREATED_AT or '2024-01-01'
    
    @classmethod
    def objects(cls):
        return UserManager()
    
    def save(self):
        """Save user to database"""
        db = RDBMSWrapper.get_db()
        
        if self.id and self.id != 'None':
            # Update existing user
            try:
                # First check if user exists
                users = User.objects().filter(id=self.id)
                if users:
                    # Update
                    set_parts = []
                    if self.name:
                        set_parts.append(f"name = '{self.name}'")
                    if self.email:
                        set_parts.append(f"email = '{self.email}'")
                    if self.age is not None:
                        set_parts.append(f"age = {self.age}")
                    
                    if set_parts:
                        set_clause = ", ".join(set_parts)
                        sql = f"UPDATE users SET {set_clause} WHERE id = {self.id}"
                        db.execute_sql(sql)
                        RDBMSWrapper.save_db()
                else:
                    # Insert as new
                    self.id = None
                    return self.save()
            except Exception as e:
                print(f"Error updating user: {e}")
                return None
        else:
            # Insert new user
            try:
                # Get next available ID
                result = db.execute_sql("SELECT MAX(id) as max_id FROM users")
                max_id = result[0]['max_id'] if result and result[0].get('max_id') else 0
                new_id = max_id + 1 if max_id else 1
                
                sql = f"""
                    INSERT INTO users (id, name, email, age, created_at) 
                    VALUES ({new_id}, '{self.name}', '{self.email}', 
                            {self.age if self.age is not None else 'NULL'}, 
                            '{self.created_at}')
                """
                db.execute_sql(sql)
                self.id = new_id
                RDBMSWrapper.save_db()
            except Exception as e:
                print(f"Error inserting user: {e}")
                return None
        
        return self
    
    def delete(self):
        """Delete user from database"""
        if self.id:
            db = RDBMSWrapper.get_db()
            try:
                db.execute_sql(f"DELETE FROM users WHERE id = {self.id}")
                RDBMSWrapper.save_db()
                return True
            except Exception as e:
                print(f"Error deleting user: {e}")
                return False
        return False
    
    def __str__(self):
        return f"User: {self.name} ({self.email})"


class ProductManager:
    """Manager for Product model"""
    
    def __init__(self):
        self.db = RDBMSWrapper.get_db()
    
    def all(self):
        """Get all products"""
        try:
            results = self.db.execute_sql("SELECT * FROM products ORDER BY id")
            products = []
            for row in results:
                # Convert all keys to lowercase
                product_data = {}
                for key, value in row.items():
                    product_data[key.lower().replace('_', '')] = value
                product_data.update(row)
                
                try:
                    product = Product(**product_data)
                    products.append(product)
                except:
                    # Fallback
                    product = Product()
                    product.id = row.get('ID') or row.get('id') or row.get('_id')
                    product.name = row.get('NAME') or row.get('name')
                    product.price = row.get('PRICE') or row.get('price')
                    product.in_stock = row.get('IN_STOCK') or row.get('in_stock') or row.get('instock')
                    product.category = row.get('CATEGORY') or row.get('category')
                    products.append(product)
            return products
        except Exception as e:
            print(f"Error getting products: {e}")
            return []
    
    def filter(self, **kwargs):
        """Filter products by conditions"""
        try:
            where_parts = []
            for key, value in kwargs.items():
                if isinstance(value, str):
                    where_parts.append(f"{key} = '{value}'")
                elif value is None:
                    where_parts.append(f"{key} IS NULL")
                else:
                    where_parts.append(f"{key} = {value}")
            
            where_clause = " AND ".join(where_parts) if where_parts else "1=1"
            sql = f"SELECT * FROM products WHERE {where_clause} ORDER BY id"
            
            results = self.db.execute_sql(sql)
            products = []
            for row in results:
                product_data = {}
                for key, value in row.items():
                    product_data[key.lower().replace('_', '')] = value
                product_data.update(row)
                
                try:
                    product = Product(**product_data)
                    products.append(product)
                except:
                    product = Product()
                    product.id = row.get('ID') or row.get('id') or row.get('_id')
                    product.name = row.get('NAME') or row.get('name')
                    product.price = row.get('PRICE') or row.get('price')
                    product.in_stock = row.get('IN_STOCK') or row.get('in_stock') or row.get('instock')
                    product.category = row.get('CATEGORY') or row.get('category')
                    products.append(product)
            return products
        except Exception as e:
            print(f"Error filtering products: {e}")
            return []
    
    def get(self, **kwargs):
        """Get a single product matching conditions"""
        products = self.filter(**kwargs)
        return products[0] if products else None


class Product:
    """Product model"""
    
    def __init__(self, id=None, ID=None, name=None, NAME=None, price=None, PRICE=None, 
                 in_stock=None, IN_STOCK=None, category=None, CATEGORY=None, _id=None):
        # Handle all possible column name variations
        self.id = id or ID or _id
        self.name = name or NAME or ''
        self.price = price or PRICE
        self.in_stock = in_stock or IN_STOCK
        self.category = category or CATEGORY or ''
    
    @classmethod
    def objects(cls):
        return ProductManager()
    
    def save(self):
        """Save product to database"""
        db = RDBMSWrapper.get_db()
        
        if self.id and self.id != 'None':
            # Update existing product
            try:
                # Check if product exists
                products = Product.objects().filter(id=self.id)
                if products:
                    # Update
                    set_parts = []
                    if self.name:
                        set_parts.append(f"name = '{self.name}'")
                    if self.price is not None:
                        set_parts.append(f"price = {self.price}")
                    if self.in_stock is not None:
                        in_stock_val = 'TRUE' if self.in_stock else 'FALSE'
                        set_parts.append(f"in_stock = {in_stock_val}")
                    if self.category:
                        set_parts.append(f"category = '{self.category}'")
                    
                    if set_parts:
                        set_clause = ", ".join(set_parts)
                        sql = f"UPDATE products SET {set_clause} WHERE id = {self.id}"
                        db.execute_sql(sql)
                        RDBMSWrapper.save_db()
                else:
                    # Insert as new
                    self.id = None
                    return self.save()
            except Exception as e:
                print(f"Error updating product: {e}")
                return None
        else:
            # Insert new product
            try:
                # Get next available ID
                result = db.execute_sql("SELECT MAX(id) as max_id FROM products")
                max_id = result[0]['max_id'] if result and result[0].get('max_id') else 0
                new_id = max_id + 1 if max_id else 1
                
                # Handle in_stock conversion
                in_stock_val = 'TRUE' if self.in_stock else 'FALSE'
                
                sql = f"""
                    INSERT INTO products (id, name, price, in_stock, category) 
                    VALUES ({new_id}, '{self.name}', 
                            {self.price if self.price is not None else 'NULL'}, 
                            {in_stock_val}, '{self.category}')
                """
                db.execute_sql(sql)
                self.id = new_id
                RDBMSWrapper.save_db()
            except Exception as e:
                print(f"Error inserting product: {e}")
                return None
        
        return self
    
    def delete(self):
        """Delete product from database"""
        if self.id:
            db = RDBMSWrapper.get_db()
            try:
                db.execute_sql(f"DELETE FROM products WHERE id = {self.id}")
                RDBMSWrapper.save_db()
                return True
            except Exception as e:
                print(f"Error deleting product: {e}")
                return False
        return False
    
    def __str__(self):
        return f"Product: {self.name} (${self.price})"