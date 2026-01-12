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
    
    @classmethod
    def check_and_repair_tables(cls):
        """Check if tables have minimum required columns, add if missing"""
        db = cls.get_db()
        
        print("=== Checking and repairing tables ===")
        
        # Check users table
        if 'users' in db.tables:
            table = db.tables['users']
            existing_columns = [col.name.lower() for col in table.columns]
            print(f"DEBUG: Users table columns: {existing_columns}")
            
            # Ensure at least 'id' and 'name' exist
            if 'id' not in existing_columns:
                print("WARNING: Users table missing 'id' column")
                # Can't fix this easily - would need to recreate table
                
            if 'name' not in existing_columns:
                print("Adding 'name' column to users table")
                try:
                    db.execute_sql("ALTER TABLE users ADD COLUMN name TEXT")
                except:
                    print("Could not add name column")
        
        cls.save_db()
        return True
    
    @classmethod
    def fix_duplicate_emails(cls):
        """Fix duplicate emails in the database"""
        db = cls.get_db()
        
        print("=== Fixing duplicate emails ===")
        
        try:
            # Find duplicate emails
            result = db.execute_sql("""
                SELECT email, COUNT(*) as count 
                FROM users 
                WHERE email IS NOT NULL AND email != ''
                GROUP BY email 
                HAVING COUNT(*) > 1
            """)
            
            if not result:
                print("No duplicate emails found")
                return True
            
            print(f"Found {len(result)} email(s) with duplicates:")
            for row in result:
                print(f"  {row['email']}: {row['count']} duplicates")
            
            # Fix each duplicate
            for dup in result:
                email = dup['email']
                count = dup['count']
                
                # Get all users with this email
                users = db.execute_sql(f"SELECT id, name FROM users WHERE email = '{email}' ORDER BY id")
                
                # Keep the first one, modify the rest
                for i, user in enumerate(users):
                    if i == 0:
                        continue  # Keep first one as is
                    
                    user_id = user['id']
                    new_email = f"{email}.{i}"  # Add suffix to make unique
                    
                    print(f"  Changing user {user_id} email from '{email}' to '{new_email}'")
                    
                    # Update the user
                    db.execute_sql(f"UPDATE users SET email = '{new_email}' WHERE id = {user_id}")
            
            cls.save_db()
            print("✓ Fixed duplicate emails")
            return True
            
        except Exception as e:
            print(f"✗ Error fixing duplicate emails: {e}")
            import traceback
            traceback.print_exc()
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
                
                # Clean the row data - remove keys with underscores that might cause issues
                cleaned_row = {}
                for key, value in row.items():
                    # Skip keys that start with underscore (except '_id')
                    if key == '_id':
                        cleaned_row['id'] = value
                    elif not key.startswith('_'):
                        cleaned_row[key] = value
                
                print(f"DEBUG UserManager.all(): Cleaned row: {cleaned_row}")
                
                try:
                    # Pass cleaned row as kwargs
                    user = User(**cleaned_row)
                    users.append(user)
                except Exception as e:
                    print(f"DEBUG UserManager.all(): Error creating user: {e}")
                    # Fallback: create empty user and set attributes manually
                    user = User()
                    
                    # Extract data from row
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
    
    def __init__(self, **kwargs):
            # Set defaults
            self.id = None
            self.name = ''
            self.email = ''
            self.age = None
            self.created_at = '2024-01-01'
            
            # Process kwargs, ignoring unknown ones
            for key, value in kwargs.items():
                key_lower = key.lower().replace('_', '')
                
                if key_lower in ['id', '_id']:
                    if value is not None:
                        try:
                            self.id = int(value)
                        except (ValueError, TypeError):
                            self.id = value
                elif key_lower == 'name':
                    self.name = str(value) if value is not None else ''
                elif key_lower == 'email':
                    self.email = str(value) if value is not None else ''
                elif key_lower == 'age':
                    if value is not None and str(value).strip():
                        try:
                            self.age = int(value)
                        except (ValueError, TypeError):
                            try:
                                self.age = int(float(value))
                            except:
                                self.age = None
                elif key_lower == 'createdat':
                    self.created_at = str(value) if value is not None else '2024-01-01'
            
            # Debug
            print(f"DEBUG User.__init__(): id={self.id}, name={self.name}, email={self.email}, age={self.age}")
    
    @classmethod
    def objects(cls):
        return UserManager()
    
    def save(self):
        """Save user to database - enforce email uniqueness"""
        db = RDBMSWrapper.get_db()
        
        print(f"DEBUG User.save(): id={self.id}, name={self.name}, email={self.email}, age={self.age}")
        
        # First, ensure table has proper structure with UNIQUE email
        self._ensure_table_columns(db)
        
        # Check email uniqueness at application level
        if self.email:
            # Check if email already exists for other users
            if self.id:
                # For updates: check if email exists for other users
                check_sql = f"SELECT id FROM users WHERE email = '{self.email}' AND id != {self.id}"
            else:
                # For inserts: check if email exists at all
                check_sql = f"SELECT id FROM users WHERE email = '{self.email}'"
            
            try:
                existing = db.execute_sql(check_sql)
                if existing:
                    raise ValueError(f"Email '{self.email}' is already in use by another user")
            except Exception as check_error:
                print(f"DEBUG: Email check error: {check_error}")
                # Continue anyway, database constraint will catch it
        
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
                set_parts.append(f"created_at = '{self.created_at}'")
                
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
                import traceback
                traceback.print_exc()
        
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
    
    def _ensure_table_columns(self, db):
        """Ensure the users table has the columns we need with proper constraints"""
        try:
            # Get table schema
            schema = db.get_schema()
            
            if 'users' not in schema['tables']:
                # Table doesn't exist - create it with proper schema including UNIQUE email
                print("DEBUG: Users table doesn't exist, creating with UNIQUE email...")
                db.execute_sql("""
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        email TEXT UNIQUE,
                        age INTEGER,
                        created_at TEXT DEFAULT '2024-01-01'
                    )
                """)
                return
            
            # Check what columns exist and their constraints
            users_columns = schema['tables']['users']['columns']
            existing_columns = {}
            for col in users_columns:
                col_name = col['name'].lower()
                existing_columns[col_name] = {
                    'exists': True,
                    'is_unique': col['unique']
                }
            
            print(f"DEBUG: Existing columns in users table: {existing_columns}")
            
            # Check if email column exists and is UNIQUE
            if 'email' not in existing_columns:
                print("DEBUG: Email column doesn't exist, adding...")
                
                # Try to add column first
                try:
                    db.execute_sql("ALTER TABLE users ADD COLUMN email TEXT")
                    print("DEBUG: Added email column (non-unique)")
                    
                    # Now we need to make it unique by recreating table
                    self._make_email_unique(db)
                    
                except Exception as e:
                    print(f"DEBUG: Could not add email column: {e}")
            else:
                # Email column exists, check if it's UNIQUE
                if not existing_columns['email']['is_unique']:
                    print("DEBUG: Email column exists but is not UNIQUE, fixing...")
                    self._make_email_unique(db)
            
            # Check and add other missing columns
            other_columns = {
                'age': 'INTEGER',
                'created_at': 'TEXT'
            }
            
            for col_name, col_type in other_columns.items():
                if col_name not in existing_columns:
                    print(f"DEBUG: Adding missing column '{col_name}' to users table")
                    try:
                        sql = f"ALTER TABLE users ADD COLUMN {col_name} {col_type}"
                        db.execute_sql(sql)
                        print(f"DEBUG: Added column '{col_name}'")
                    except Exception as e:
                        print(f"DEBUG: Could not add column '{col_name}': {e}")
                        
        except Exception as e:
            print(f"DEBUG: Error ensuring table columns: {e}")
            import traceback
            traceback.print_exc()
    
    def _make_email_unique(self, db):
        """Recreate users table with UNIQUE email constraint"""
        print("DEBUG: Making email column UNIQUE...")
        
        try:
            # Backup existing data
            existing_data = db.execute_sql("SELECT * FROM users")
            print(f"DEBUG: Found {len(existing_data)} users to migrate")
            
            # Create temp table with UNIQUE email
            try:
                db.execute_sql("DROP TABLE users_new")
            except:
                pass
            
            db.execute_sql("""
                CREATE TABLE users_new (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER,
                    created_at TEXT DEFAULT '2024-01-01'
                )
            """)
            
            # Migrate data, handling potential duplicate emails
            duplicate_emails = set()
            migrated_count = 0
            
            for i, user in enumerate(existing_data):
                user_id = user.get('id', user.get('ID', i+1))
                name = user.get('name', user.get('NAME', f'User {user_id}'))
                email = user.get('email', user.get('EMAIL', ''))
                age = user.get('age', user.get('AGE'))
                created_at = user.get('created_at', user.get('CREATED_AT', '2024-01-01'))
                
                # If email is empty, generate a placeholder
                if not email:
                    email = f"user{user_id}@example.com"
                
                # Check if this email already exists in our migration
                if email and email in duplicate_emails:
                    # Generate unique email for duplicate
                    email = f"user{user_id}.dup@example.com"
                
                # Try to insert
                try:
                    age_value = f"{age}" if age is not None else "NULL"
                    sql = f"""
                        INSERT INTO users_new (id, name, email, age, created_at)
                        VALUES ({user_id}, '{name}', '{email}', {age_value}, '{created_at}')
                    """
                    db.execute_sql(sql)
                    migrated_count += 1
                    
                    if email:
                        duplicate_emails.add(email)
                        
                except Exception as e:
                    if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                        # Duplicate email found, generate unique one
                        email = f"user{user_id}.{i}@example.com"
                        sql = f"""
                            INSERT INTO users_new (id, name, email, age, created_at)
                            VALUES ({user_id}, '{name}', '{email}', {age_value}, '{created_at}')
                        """
                        db.execute_sql(sql)
                        migrated_count += 1
                    else:
                        print(f"DEBUG: Error migrating user {user_id}: {e}")
            
            # Replace table
            db.execute_sql("DROP TABLE users")
            db.execute_sql("ALTER TABLE users_new RENAME TO users")
            
            print(f"DEBUG: Successfully migrated {migrated_count} users to table with UNIQUE email")
            
        except Exception as e:
            print(f"DEBUG: Error making email unique: {e}")
            import traceback
            traceback.print_exc()
    
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