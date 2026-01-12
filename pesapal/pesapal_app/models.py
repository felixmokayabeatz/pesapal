# pesapal_app/models.py

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
        """Create initial tables - WITH EMAIL UNIQUENESS"""
        print("=== DEBUG: Creating tables with email uniqueness ===")
        
        try:
            # First, drop table if it exists (simple approach)
            try:
                db.execute_sql("DROP TABLE users")
                print("DEBUG: Dropped existing users table")
            except:
                print("DEBUG: No users table to drop")
                pass
            
            # Create users table with UNIQUE constraint on email
            print("DEBUG: Creating users table with UNIQUE email...")
            db.execute_sql("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER,
                    created_at TEXT
                )
            """)
            print("✓ Created 'users' table with UNIQUE email")
            
            # Test insert
            print("DEBUG: Testing insert...")
            result = db.execute_sql("INSERT INTO users (name, email, age, created_at) VALUES ('Test User', 'test@example.com', 25, '2024-01-01')")
            print(f"DEBUG: Insert result: {result}")
            
            # Verify
            users = db.execute_sql("SELECT * FROM users")
            print(f"DEBUG: Users in table: {users}")
            
        except Exception as e:
            print(f"✗ Error creating users table: {e}")
            import traceback
            traceback.print_exc()
        
        try:
            # Same for products
            try:
                db.execute_sql("DROP TABLE products")
                print("DEBUG: Dropped existing products table")
            except:
                print("DEBUG: No products table to drop")
                pass
            
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
            print(f"✗ Error creating products table: {e}")
            import traceback
            traceback.print_exc()
    
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
            print(f"DEBUG UserManager.all(): Got {len(results)} users from database")
            
            users = []
            for i, row in enumerate(results):
                print(f"DEBUG UserManager.all(): Row {i} = {row}")
                
                # Create user with the exact keys from the database
                # First, normalize the keys to match what User.__init__ expects
                user_kwargs = {}
                
                for key, value in row.items():
                    # Store original key
                    user_kwargs[key] = value
                    # Also store uppercase version
                    user_kwargs[key.upper()] = value
                    # Also store lowercase version
                    user_kwargs[key.lower()] = value
                    # Also store version without underscores
                    clean_key = key.lower().replace('_', '')
                    user_kwargs[clean_key] = value
                
                print(f"DEBUG UserManager.all(): User kwargs = {user_kwargs}")
                
                try:
                    user = User(**user_kwargs)
                    users.append(user)
                except Exception as e:
                    print(f"DEBUG UserManager.all(): Error creating user: {e}")
                    # Fallback: create empty user and set attributes manually
                    user = User()
                    
                    # Extract data from row using all possible key variations
                    for key, value in row.items():
                        key_lower = key.lower()
                        if key_lower in ['id', '_id']:
                            user.id = value
                        elif key_lower in ['name', 'fullname']:
                            user.name = value or ''
                        elif key_lower == 'email':
                            user.email = value or ''
                        elif key_lower == 'age':
                            if value is not None and value != '':
                                try:
                                    user.age = int(value)
                                except:
                                    user.age = None
                            else:
                                user.age = None
                        elif key_lower in ['created_at', 'createdat', 'created']:
                            user.created_at = value or '2024-01-01'
                    
                    users.append(user)
                    print(f"DEBUG UserManager.all(): Fallback user: id={user.id}, name={user.name}, email={user.email}, age={user.age}")
            
            return users
        except Exception as e:
            print(f"Error getting users: {e}")
            import traceback
            traceback.print_exc()
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
        
        try:
            # Simple approach: get all and filter
            all_users = self.all()
            print(f"DEBUG: Found {len(all_users)} total users")
            
            for user in all_users:
                match = True
                for key, value in kwargs.items():
                    # Get the attribute (handle case-insensitive)
                    attr_value = None
                    
                    # Try different attribute names
                    for attr_name in [key.lower(), key.upper(), key]:
                        if hasattr(user, attr_name):
                            attr_value = getattr(user, attr_name)
                            break
                    
                    if attr_value is None:
                        match = False
                        break
                    
                    # Compare values
                    if str(attr_value) != str(value):
                        match = False
                        break
                
                if match:
                    print(f"DEBUG: Found matching user: id={user.id}")
                    return user
            
            print(f"DEBUG: No matching user found")
            return None
            
        except Exception as e:
            print(f"DEBUG UserManager.get(): Exception: {e}")
            import traceback
            traceback.print_exc()
            return None

class User:
    """User model"""
    
    def __init__(self, id=None, ID=None, name=None, NAME=None, email=None, EMAIL=None, 
                 age=None, AGE=None, created_at=None, CREATED_AT=None, CREATEDAT=None, 
                 createdat=None, _id=None):
        # Handle all possible column name variations
        self.id = id or ID or _id
        self.name = name or NAME or ''
        self.email = email or EMAIL or ''
        
        # Handle age - convert to int if possible
        age_val = age or AGE
        if age_val is not None and age_val != '':
            try:
                self.age = int(age_val)
            except (ValueError, TypeError):
                self.age = None
        else:
            self.age = None
            
        self.created_at = created_at or CREATED_AT or CREATEDAT or createdat or '2024-01-01'
        
        # Debug - PRINT ALL FIELDS!
        print(f"DEBUG User.__init__(): id={self.id}, name={self.name}, email={self.email}, age={self.age}")
    
    @classmethod
    def objects(cls):
        return UserManager()
    
    def save(self):
        """Save user to database with error handling for unique constraints"""
        db = RDBMSWrapper.get_db()
        
        print(f"DEBUG User.save(): id={self.id}, name={self.name}, email={self.email}, age={self.age}")
        
        # Validate email uniqueness (additional check before trying SQL)
        if self.email:
            try:
                # Check if email already exists for other users
                if self.id:
                    # For updates: check if email exists for other users
                    check_sql = f"SELECT id FROM users WHERE email = '{self.email}' AND id != {self.id}"
                else:
                    # For inserts: check if email exists at all
                    check_sql = f"SELECT id FROM users WHERE email = '{self.email}'"
                
                existing = db.execute_sql(check_sql)
                if existing:
                    raise ValueError(f"Email '{self.email}' is already in use by another user")
            except Exception as check_error:
                # If check fails, we'll let the database enforce it
                print(f"DEBUG: Email check error (will let DB handle): {check_error}")
        
        # Convert ID to string for comparison
        id_str = str(self.id) if self.id is not None else None
        
        try:
            # Always try UPDATE first if we have an ID
            if id_str and id_str.isdigit():
                print(f"DEBUG: Attempting UPDATE for user id={self.id}")
                
                # Build UPDATE SQL
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
                    print(f"DEBUG: UPDATE SQL: {sql}")
                    
                    # Execute update
                    result = db.execute_sql(sql)
                    print(f"DEBUG: UPDATE result: {result}")
                    RDBMSWrapper.save_db()
                    return self
                else:
                    print(f"DEBUG: Nothing to update")
                    return self
                    
        except Exception as e:
            error_msg = str(e)
            print(f"DEBUG: UPDATE failed: {error_msg}")
            
            # Check if it's a unique constraint violation
            if "duplicate" in error_msg.lower() or "unique" in error_msg.lower():
                raise ValueError(f"Email '{self.email}' is already in use. Please use a different email.")
            else:
                # Re-raise other errors
                raise
        
        # If UPDATE failed or no ID, do INSERT
        print(f"DEBUG: Attempting INSERT for user")
        try:
            # Don't include ID - let the table auto-generate it
            age_value = f"{self.age}" if self.age is not None else "NULL"
            sql = f"INSERT INTO users (name, email, age, created_at) VALUES ('{self.name}', '{self.email}', {age_value}, '{self.created_at}')"
            print(f"DEBUG: INSERT SQL: {sql}")
            
            result = db.execute_sql(sql)
            print(f"DEBUG: INSERT returned: {result}")
            
            # Get the new ID
            if isinstance(result, int):
                self.id = result
            else:
                # Try to get the last inserted ID
                max_result = db.execute_sql("SELECT MAX(id) as max_id FROM users")
                if max_result and isinstance(max_result, list) and len(max_result) > 0:
                    self.id = max_result[0].get('max_id', 1)
            
            RDBMSWrapper.save_db()
            print(f"DEBUG: INSERT successful, new id={self.id}")
            return self
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error inserting user: {error_msg}")
            
            # Check if it's a unique constraint violation
            if "duplicate" in error_msg.lower() or "unique" in error_msg.lower():
                raise ValueError(f"Email '{self.email}' is already in use. Please use a different email.")
            else:
                # Re-raise other errors
                raise
    
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