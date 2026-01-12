# pesapal_app/views.py

from django.shortcuts import render, redirect
from django.http import JsonResponse
from .models import User, Product, RDBMSWrapper


def index(request):
    """Home page with database info"""
    db = RDBMSWrapper.get_db()
    schema = db.get_schema()
    
    # Calculate stats
    total_tables = len(schema['tables'])
    total_rows = sum(table['row_count'] for table in schema['tables'].values())
    
    # Get sample data for each table
    for table_name, table_info in schema['tables'].items():
        if table_info['row_count'] > 0:
            try:
                result = db.execute_sql(f"SELECT * FROM {table_name} LIMIT 1")
                if result:
                    table_info['sample_data'] = result[0]
            except:
                table_info['sample_data'] = {}
    
    # Get specific counts
    user_count = schema['tables'].get('users', {}).get('row_count', 0)
    product_count = schema['tables'].get('products', {}).get('row_count', 0)
    
    context = {
        'database_name': schema['name'],
        'tables': schema['tables'],
        'total_tables': total_tables,
        'total_rows': total_rows,
        'user_count': user_count,
        'product_count': product_count,
    }
    return render(request, 'index.html', context)


def users_view(request):
    """List all users"""
    users = User.objects().all()
    
    # Convert to template-friendly format
    user_list = []
    for user in users:
        user_list.append({
            'id': user.id,
            'name': user.name or '',
            'email': user.email or '',
            'age': user.age if user.age not in (None, 'None', '') else '',
            'created_at': user.created_at or ''
        })
    
    # Debug: Print what we're getting
    print(f"DEBUG users_view: Processing {len(user_list)} users")
    for i, user in enumerate(user_list):
        print(f"DEBUG users_view: User {i+1}: id={user['id']}, name={user['name']}, email={user['email']}, age={user['age']}")
    
    # Calculate average age for stats
    ages = [u['age'] for u in user_list if u['age'] and str(u['age']).isdigit()]
    avg_age = sum(int(age) for age in ages) // len(ages) if ages else 0
    
    return render(request, 'users.html', {
        'users': user_list,
        'avg_age': avg_age
    })


def add_user(request):
    """Add a new user"""
    if request.method == 'POST':
        name = request.POST.get('name', '').strip()
        email = request.POST.get('email', '').strip()
        age_str = request.POST.get('age', '').strip()
        
        db = RDBMSWrapper.get_db()
        
        try:
            # Prepare values
            email_value = f"'{email}'" if email else "NULL"
            
            if age_str and age_str.isdigit():
                age_value = int(age_str)
            else:
                age_value = "NULL"
            
            # CRITICAL: Don't include ID, let RDBMS auto-generate it
            # Also use proper SQL formatting
            sql = f"INSERT INTO users (name, email, age, created_at) VALUES ('{name}', {email_value}, {age_value}, '2024-01-15')"
            
            print(f"DEBUG add_user SQL: {sql}")
            result = db.execute_sql(sql)
            print(f"DEBUG add_user result: {result}")
            RDBMSWrapper.save_db()
            
            print(f"✓ User added: name='{name}', email='{email}', age={age_str}")
            return redirect('users')
            
        except Exception as e:
            print(f"✗ Error in add_user: {e}")
            import traceback
            traceback.print_exc()
            return render(request, 'add_user.html', {
                'error': str(e),
                'name': name,
                'email': email,
                'age': age_str
            })
    
    return render(request, 'add_user.html')


def edit_user(request, user_id):
    """Edit a user"""
    print(f"DEBUG: edit_user called with user_id={user_id}")
    
    error_message = None
    
    try:
        # Get the user using the manager
        print(f"DEBUG: Calling User.objects().get(id={user_id})")
        user = User.objects().get(id=user_id)
        
        if not user:
            print(f"DEBUG: User not found")
            return redirect('users')
        
        print(f"DEBUG: Found user: {user.name}, {user.email}, {user.age}")
        
        if request.method == 'POST':
            print(f"DEBUG: POST request received")
            # Update user data
            user.name = request.POST.get('name', '').strip()
            user.email = request.POST.get('email', '').strip()
            
            age_str = request.POST.get('age', '').strip()
            user.age = int(age_str) if age_str and age_str.isdigit() else None
            
            print(f"DEBUG: Updated user data - name={user.name}, email={user.email}, age={user.age}")
            
            # Save the changes
            try:
                user.save()
                print(f"DEBUG: User saved")
                return redirect('users')
            except Exception as save_error:
                print(f"DEBUG: Error saving user: {save_error}")
                error_message = str(save_error)
        
        print(f"DEBUG: Rendering edit form")
        return render(request, 'edit_user.html', {'user': user, 'error': error_message})
        
    except Exception as e:
        print(f"DEBUG: Error editing user: {e}")
        import traceback
        traceback.print_exc()
        return redirect('users')


def delete_user(request, user_id):
    """Delete a user"""
    try:
        # Get the user and delete it
        user = User.objects().get(id=user_id)
        if user:
            user.delete()
    except Exception as e:
        print(f"Error deleting user: {e}")
    
    return redirect('users')


def products_view(request):
    """List all products"""
    products = Product.objects().all()
    
    # Convert to template-friendly format
    product_list = []
    for product in products:
        # Handle in_stock conversion
        in_stock = product.in_stock
        if isinstance(in_stock, str):
            in_stock = in_stock.lower() in ('true', '1', 'yes', 'y')
        elif isinstance(in_stock, int):
            in_stock = bool(in_stock)
        
        product_list.append({
            'id': product.id,
            'name': product.name or '',
            'price': float(product.price) if product.price else 0.0,
            'in_stock': in_stock,
            'category': product.category or ''
        })
    
    return render(request, 'products.html', {
        'products': product_list
    })

def add_product(request):
    """Add a new product"""
    if request.method == 'POST':
        name = request.POST.get('name')
        price = request.POST.get('price')
        in_stock = request.POST.get('in_stock') == 'on'
        category = request.POST.get('category')
        
        db = RDBMSWrapper.get_db()
        
        try:
            # Convert in_stock to SQL boolean
            in_stock_val = 'TRUE' if in_stock else 'FALSE'
            
            # Don't include ID - let it auto-generate
            sql = f"INSERT INTO products (name, price, in_stock, category) VALUES ('{name}', {price}, {in_stock_val}, '{category}')"
            
            print(f"DEBUG add_product: Executing SQL: {sql}")
            db.execute_sql(sql)
            RDBMSWrapper.save_db()
            return redirect('products')
            
        except Exception as e:
            print(f"Error adding product: {e}")
            import traceback
            traceback.print_exc()
            # Return with error
            return render(request, 'add_product.html', {
                'error': str(e),
                'name': name,
                'price': price,
                'category': category
            })
    
    return render(request, 'add_product.html')


def api_query(request):
    """Execute SQL query via API with different formats"""
    if request.method == 'POST':
        query = request.POST.get('query', '')
        format = request.POST.get('format', 'table')
        
        try:
            db = RDBMSWrapper.get_db()
            result = db.execute_sql(query)
            
            # Handle different result types
            if isinstance(result, list):
                # Apply limit for table format
                if format == 'table':
                    result = result[:100]  # Default limit
                
                # Convert to serializable format
                serializable_result = []
                for row in result:
                    if hasattr(row, 'items'):
                        serializable_row = {}
                        for key, value in row.items():
                            serializable_row[str(key)] = str(value) if value is not None else None
                        serializable_result.append(serializable_row)
                    else:
                        serializable_result.append(str(row))
                
                result = serializable_result
            elif result is None:
                result = "Query executed successfully"
            else:
                result = str(result)
            
            return JsonResponse({
                'success': True,
                'result': result,
                'format': format
            })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            })
    
    return JsonResponse({'error': 'POST required'}, status=400)


def api_schema(request):
    """Get database schema via API"""
    db = RDBMSWrapper.get_db()
    schema = db.get_schema()
    return JsonResponse(schema)


def run_join(request):
    """Demonstrate JOIN operation"""
    db = RDBMSWrapper.get_db()
    
    # Create orders table for JOIN demo
    try:
        db.execute_sql("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                order_date TEXT
            )
        """)
        
        # Insert sample orders
        db.execute_sql("INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES (1, 1, 2, '2024-01-20')")
        db.execute_sql("INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES (2, 2, 1, '2024-01-21')")
        
    except Exception:
        pass  # Table might exist
    
    # Perform JOIN
    try:
        results = db.join('users', 'orders', 'users.id = orders.user_id')
        return render(request, 'join_demo.html', {'results': results})
    except Exception as e:
        return render(request, 'join_demo.html', {'error': str(e)})
    


def web_terminal(request):
    """Web-based SQL terminal"""
    db = RDBMSWrapper.get_db()
    
    if request.method == 'POST':
        query = request.POST.get('query', '')
        format = request.POST.get('format', 'table')
        limit = int(request.POST.get('limit', 100))
        
        try:
            # Handle SCHEMA command
            if query.upper() == 'SCHEMA':
                schema = db.get_schema()
                return JsonResponse({
                    'success': True,
                    'result': schema,
                    'format': format
                })
            
            result = db.execute_sql(query)
            
            # Handle different result types
            if isinstance(result, list):
                # Apply limit for table format
                if format == 'table':
                    result = result[:limit]
                
                # Convert to serializable format
                serializable_result = []
                for row in result:
                    if hasattr(row, 'items'):
                        serializable_row = {}
                        for key, value in row.items():
                            serializable_row[str(key)] = str(value) if value is not None else None
                        serializable_result.append(serializable_row)
                    else:
                        serializable_result.append(str(row))
                
                result = serializable_result
            elif result is None:
                result = "Query executed successfully"
            else:
                result = str(result)
            
            return JsonResponse({
                'success': True,
                'result': result,
                'format': format
            })
                
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            })
    
    # GET request - show terminal
    schema = db.get_schema()
    return render(request, 'terminal.html', {'schema': schema})