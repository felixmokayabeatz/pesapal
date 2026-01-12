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


# pesapal_app/views.py - Update add_user and edit_user views

def add_user(request):
    """Add a new user with error handling"""
    error_message = None
    
    if request.method == 'POST':
        name = request.POST.get('name', '').strip()
        email = request.POST.get('email', '').strip()
        age_str = request.POST.get('age', '').strip()
        
        # Basic validation
        if not name:
            error_message = "Name is required."
        elif not email:
            error_message = "Email is required."
        elif '@' not in email:
            error_message = "Please enter a valid email address."
        
        if not error_message:
            try:
                # Create user object
                user = User()
                user.name = name
                user.email = email
                user.age = int(age_str) if age_str and age_str.isdigit() else None
                user.created_at = '2024-01-15'  # Default value
                
                # Save with error handling
                try:
                    user.save()
                    print(f"✓ User added: name='{name}', email='{email}'")
                    return redirect('users')
                except ValueError as e:
                    error_message = str(e)
                except Exception as e:
                    error_message = f"Error saving user: {str(e)}"
                    
            except Exception as e:
                error_message = f"Error creating user: {str(e)}"
        
        # Return with error and form data
        return render(request, 'add_user.html', {
            'error': error_message,
            'name': name,
            'email': email,
            'age': age_str
        })
    
    return render(request, 'add_user.html')


# pesapal_app/views.py - Update edit_user view

def edit_user(request, user_id):
    """Edit a user - handles partial updates"""
    error_message = None
    
    try:
        # Get the user
        user = User.objects().get(id=user_id)
        
        if not user:
            return redirect('users')
        
        if request.method == 'POST':
            # Get form data
            name = request.POST.get('name', '').strip()
            email = request.POST.get('email', '').strip()
            age_str = request.POST.get('age', '').strip()
            
            print(f"DEBUG edit_user: Updating user {user_id}: name='{name}', email='{email}', age='{age_str}'")
            
            # Update only provided fields
            if name:
                user.name = name
            
            if email:
                user.email = email
            
            if age_str:
                try:
                    user.age = int(age_str)
                except ValueError:
                    user.age = None
            
            # Save with error handling
            try:
                user.save()
                print(f"DEBUG: User updated successfully")
                return redirect('users')
            except ValueError as e:
                error_message = str(e)
                print(f"DEBUG: ValueError: {error_message}")
            except Exception as e:
                error_message = f"Error saving user: {str(e)}"
                print(f"DEBUG: Exception: {error_message}")
                import traceback
                traceback.print_exc()
        
        return render(request, 'edit_user.html', {'user': user, 'error': error_message})
        
    except Exception as e:
        print(f"Error editing user: {e}")
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
    """Demonstrate JOIN operations with different types"""
    db = RDBMSWrapper.get_db()
    
    join_type = request.GET.get('type', 'INNER').upper()
    valid_types = ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS']
    
    if join_type not in valid_types:
        join_type = 'INNER'
    
    try:
        results = db.join('users', 'orders', 'users.id = orders.user_id', join_type)
        
        # Calculate statistics
        total_rows = len(results)
        users_with_orders = len(set(r.get('users.id') for r in results if r.get('users.id')))
        orders_with_users = len(set(r.get('orders.id') for r in results if r.get('orders.id')))
        
        return render(request, 'join_demo.html', {
            'results': results,
            'join_type': join_type,
            'total_rows': total_rows,
            'users_with_orders': users_with_orders,
            'orders_with_users': orders_with_users,
            'join_types': valid_types
        })
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