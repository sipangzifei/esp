from flask import Flask
from flask import url_for
from flask import request

from common  import *

app = Flask(__name__)

@app.route('/h')
def h():
    return 'H!'

@app.route('/')
def index():
    return 'Index Page'

@app.route('/hello')
def hello():
    return 'Hello, World'

@app.route('/user/<username>')
def my_profile(username):
    # show the user profile for that user
    return 'User %s' % username

@app.route('/xxx/<yyy>')
def show_zzz_profile(yyy):
    # show the variable
    return 'this is %s' % yyy 

@app.route('/projects/')
def projects():
    return 'The project page'

@app.route('/about')
def about():
    return 'The about page'

@app.route('/login')
def login(): pass


with app.test_request_context():
    print url_for('index')
    print url_for('login')
    print url_for('login', next='/')
    print url_for('my_profile', username='John Doe')

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['the_file']
        f.save('uploaded_file.txt')


