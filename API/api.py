from flask import Flask, jsonify, request, send_file
from flask_restful import Api, Resource, reqparse
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import datetime
from functools import wraps
from use_sql_lite.Data_Base import Data_Base

app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = 'your_secret_key'


# Helper function to generate JWT
def generate_token(username):
    token = jwt.encode({
        'user': username,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    }, app.config['SECRET_KEY'], algorithm="HS256")
    return token


# Decorator to require token for protected routes
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing!'}), 401
        try:
            token = token.split()[1]
            jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        except:
            return jsonify({'message': 'Token is invalid!'}), 401
        return f(*args, **kwargs)

    return decorated


# Регистрация пользователя
class Register(Resource):
    db_name = "../use_sql_lite/grade.db"

    def post(self):
        data = request.get_json()
        username = data['username']
        password = data['password']

        db = Data_Base(self.db_name)
        hashed_password = generate_password_hash(password)

        try:
            db.insert_user(username, hashed_password)  # Вставить пользователя в базу данных
            return jsonify({"message": "User registered successfully!"}), 201
        except Exception as e:
            return jsonify({"message": f"User registration failed: {str(e)}"}), 400


# Авторизация пользователя
class Login(Resource):
    db_name = "../use_sql_lite/grade.db"

    def post(self):
        data = request.get_json()
        username = data['username']
        password = data['password']
        db = Data_Base(self.db_name)
        user = db.get_user(username)

        if user and check_password_hash(user[2], password):
            token = generate_token(username)
            return jsonify({"token": token})
        else:
            return jsonify({"message": "Invalid credentials!"})


# Пример защищенного ресурса
class ProtectedResource(Resource):
    @token_required
    def get(self):
        return jsonify({"message": "This is a protected resource!"})


class Grade_table(Resource):
    db_name = "../use_sql_lite/grade.db"

    def get(self, table_name, number):
        db = Data_Base(self.db_name)
        table = db.get_table(table_name, number)
        table = table.to_dict()
        return jsonify(table)


class Grade_colums_name(Resource):
    db_name = "../use_sql_lite/grade.db"

    def get(self, table_name):
        db = Data_Base(self.db_name)
        colums = db.get_list_table_colums(table_name)
        return jsonify(colums)


class ReceiveJson(Resource):
    db_name = "../use_sql_lite/grade.db"

    def post(self, table_name):
        all_filters = request.get_json()

        db = Data_Base(self.db_name)
        search_table = db.keyword_search(table_name, all_filters)
        search_table = search_table.to_dict()
        response = {
            "status": "success",
            "table": search_table,
            "download_url": ""
        }
        return response, 200


class send_excel_file(Resource):
    db_name = "../use_sql_lite/grade.db"
    download_folder = './downloads'

    def post(self, table_name):
        all_filters = request.get_json()

        db = Data_Base(self.db_name)
        search_table = db.keyword_search(table_name, all_filters)

        excel_filename = './output.xlsx'
        search_table.to_excel(excel_filename, index=False)

        return send_file(excel_filename, as_attachment=True)


class get_unique_elements_in_colums(Resource):
    db_name = "../use_sql_lite/grade.db"

    def get(self, table_name):
        db = Data_Base(self.db_name)
        unique_elements = db.get_unique_elements(table_name)
        return unique_elements


api.add_resource(Register, "/register")
api.add_resource(Login, "/login")
api.add_resource(ProtectedResource, "/protected")
api.add_resource(Grade_table, "/get_table/<table_name>/<int:number>")
api.add_resource(Grade_colums_name, "/get_colum/<table_name>")
api.add_resource(ReceiveJson, "/receive_json/<table_name>")
api.add_resource(send_excel_file, "/send_excel/<table_name>")
api.add_resource(get_unique_elements_in_colums, "/get_unique_elementss/<table_name>")

if __name__ == '__main__':
    app.run(debug=True)
