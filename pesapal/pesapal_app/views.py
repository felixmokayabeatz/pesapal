from django.shortcuts import render, redirect
from django.http import JsonResponse
from .models import User, Product, RDBMSWrapper


def index(request):
    """Home page with database info"""
    db = RDBMSWrapper.get_db()
    schema = db.get_schema()
    
    context = {
        'database_name': schema['name'],
        'tables': schema['tables'],
    }
    return render(request, 'index.html', context)


def users_view(request):
    """List all users"""
    users = User.objects().all()
    return render(request, 'users.html', {'users': users})


def add_user(request):
    """Add a new user"""
    if request.method == 'POST':
        name = request.POST.get('name')
        email = request.POST.get('email')
        age = request.POST.get('age')
        
        user = User(name=name, email=email, age=int(age) if age else None)
        user.save()
        
        return redirect('users')
    
    return render(request, 'add_user.html')


def edit_user(request, user_id):
    """Edit a user"""
    users = User.objects().filter(id=user_id)
    if not users:
        return redirect('users')
    
    user = users[0]
    
    if request.method == 'POST':
        user.name = request.POST.get('name')
        user.email = request.POST.get('email')
        user.age = request.POST.get('age')
        user.save()
        return redirect('users')
    
    return render(request, 'edit_user.html', {'user': user})


def delete_user(request, user_id):
    """Delete a user"""
    db = RDBMSWrapper.get_db()
    db.execute_sql(f"DELETE FROM users WHERE id = {user_id}")
    return redirect('users')


def products_view(request):
    """List all products"""
    products = Product.objects().all()
    return render(request, 'products.html', {'products': products})


def add_product(request):
    """Add a new product"""
    if request.method == 'POST':
        name = request.POST.get('name')
        price = request.POST.get('price')
        in_stock = request.POST.get('in_stock') == 'on'
        category = request.POST.get('category')
        
        db = RDBMSWrapper.get_db()
        db.execute_sql(f"""
            INSERT INTO products (name, price, in_stock, category) 
            VALUES ('{name}', {price}, {in_stock}, '{category}')
        """)
        
        return redirect('products')
    
    return render(request, 'add_product.html')


def api_query(request):
    """Execute SQL query via API"""
    if request.method == 'POST':
        query = request.POST.get('query', '')
        try:
            db = RDBMSWrapper.get_db()
            result = db.execute_sql(query)
            return JsonResponse({
                'success': True,
                'result': result
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
        try:
            result = db.execute_sql(query)
            
            # Format result for display
            if isinstance(result, list):
                return JsonResponse({
                    'success': True,
                    'type': 'table',
                    'data': result,
                    'count': len(result)
                })
            else:
                return JsonResponse({
                    'success': True,
                    'type': 'scalar',
                    'data': result
                })
                
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            })
    
    # Get current schema for display
    schema = db.get_schema()
    return render(request, 'terminal.html', {'schema': schema})