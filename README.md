# PesapalDB Custom SQL (Python - Django)

This is a **Simple Custom Relational Database Management System (RDBMS)** written in Python where you can store data in a relational database with custom queries. It comes with a web app written using **Django** for easier execution. Easy to use with examples.

> - Main purpose of this project is to showcase the custom RDBMS and the web app to access it.

It stores data in a file named **db.pesapal** when you start the app.

---

## ⚡ Tech Stack 
- [Python](https://www.python.org/downloads/) — Main programming language
- [Django](https://www.djangoproject.com/) — Backend web framework
- [Requests](https://docs.python-requests.org/) — API calls from Django (lightly used)
- **HTML5** — Markup structure
- **CSS3** — Styling and layout
- **JavaScript** — Client-side interactivity
- [Bootstrap](https://getbootstrap.com/) — Responsive UI components

---

## 📦 Installation & Setup

### 1️⃣ Clone the repository
```bash
git clone https://github.com/felixmokayabeatz/pesapal.git
cd pesapal
# Default Branch (main)
```

---

## 2️⃣ Create and Activate the virtual environment (I will move to Docker soon to make setup easier)

> - For uniformity and easier collaboration use the exact names of the virtual environment names below. If you create with another name add them to gitignore

#### On Windows example
```bash
# Create
python -m venv pesapal_v_env
# Activate
pesapal_v_env\Scripts\activate
```

#### On Linux/macOS
```bash
# Create
python3 -m venv pesapal_v_env

# Activate
source pesapal_v_env/bin/activate
```

---

## 3️⃣ Install the Dependencies/Packages
```bash
pip install -r requirements.txt
```

---

## Running the project
```bash
cd pesapal
python manage.py runserver

# The directory where manage.py is located is where you run the 'python manage.py runserver'.
```

Then visit 👉 http://127.0.0.1:8000 on your browser

---

## You can also use a normal terminal to access the RDBMS (Optional - works exactly like the web version)

> Make sure you are on the root of the project where [run_repl.py](./pesapal/run_repl.py) is located and run it using the following command.
```bash
python run_repl.py
```

---

# Demo gif and screenshots

## 🎥 Demo gif

![App Demo](./assets/gif/pesapalDemo.gif)

## Screenshots

![1](./assets/screenshots/1.png)
![2](./assets/screenshots/2.png)
![3](./assets/screenshots/3.png)
![4](./assets/screenshots/4.png)
![5](./assets/screenshots/5.png)
![6](./assets/screenshots/6.png)

---

## Thank You ✌️