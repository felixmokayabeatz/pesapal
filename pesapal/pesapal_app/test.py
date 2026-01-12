# @Felix 2026

def test_db_columns(request):
    """Test what columns exist in users table"""
    from .models import RDBMSWrapper
    
    db = RDBMSWrapper.get_db()
    output = []
    
    
    try:
        users = db.execute_sql("SELECT * FROM users LIMIT 1")
        output.append("<h1>Database Column Test</h1>")
        
        if users:
            user = users[0]
            output.append(f"<h2>First user columns:</h2>")
            output.append("<ul>")
            for key, value in user.items():
                output.append(f"<li><strong>{key}</strong>: {value} (type: {type(value).__name__})</li>")
            output.append("</ul>")
        else:
            output.append("<p>No users in database</p>")
            
        
        output.append("<h2>Test Insert:</h2>")
        try:
            sql = "INSERT INTO users (name, email, age) VALUES ('Test User', 'test@example.com', 30)"
            result = db.execute_sql(sql)
            output.append(f"<p>Insert result: {result}</p>")
            
            
            users = db.execute_sql("SELECT * FROM users ORDER BY id DESC LIMIT 1")
            if users:
                user = users[0]
                output.append(f"<h3>Newly inserted user:</h3>")
                output.append("<ul>")
                for key, value in user.items():
                    output.append(f"<li><strong>{key}</strong>: {value}</li>")
                output.append("</ul>")
                
        except Exception as e:
            output.append(f"<p>Insert error: {e}</p>")
            
    except Exception as e:
        output.append(f"<p>Error: {e}</p>")
    
    from django.http import HttpResponse
    return HttpResponse("<br>".join(output))