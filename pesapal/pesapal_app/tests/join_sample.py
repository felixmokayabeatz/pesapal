from pesapal_app.models import RDBMSWrapper


def ensure_join_sample_data(db):
    """Create sample data for JOIN demonstration if needed"""
    try:
        # Create users table if not exists
        try:
            db.execute_sql("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            
            # Insert sample users
            sample_users = [
                (1, 'Alice Johnson', 'alice@example.com', 25),
                (2, 'Bob Smith', 'bob@example.com', 30),
                (3, 'Charlie Brown', 'charlie@example.com', 35),
                (4, 'Diana Prince', 'diana@example.com', 28)
            ]
            
            for user_id, name, email, age in sample_users:
                try:
                    db.execute_sql(f"""
                        INSERT INTO users (id, name, email, age) 
                        VALUES ({user_id}, '{name}', '{email}', {age})
                    """)
                except:
                    pass  # User might already exist
        except:
            pass  # Table might already exist
        
        # Create orders table if not exists
        try:
            db.execute_sql("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    product_name TEXT,
                    quantity INTEGER,
                    order_date TEXT,
                    total_price REAL
                )
            """)
            
            # Insert sample orders
            sample_orders = [
                (1, 1, 'Laptop', 1, '2024-01-15', 999.99),
                (2, 1, 'Mouse', 2, '2024-01-15', 49.98),
                (3, 2, 'Keyboard', 1, '2024-01-16', 79.99),
                (4, 2, 'Monitor', 1, '2024-01-17', 299.99),
                (5, 5, 'Headphones', 1, '2024-01-18', 149.99)  # user_id=5 doesn't exist in users
            ]
            
            for order_id, user_id, product, quantity, date, price in sample_orders:
                try:
                    db.execute_sql(f"""
                        INSERT INTO orders (id, user_id, product_name, quantity, order_date, total_price) 
                        VALUES ({order_id}, {user_id}, '{product}', {quantity}, '{date}', {price})
                    """)
                except:
                    pass  # Order might already exist
        except:
            pass  # Table might already exist
        
        RDBMSWrapper.save_db()
        
    except Exception as e:
        print(f"Error creating sample data: {e}")