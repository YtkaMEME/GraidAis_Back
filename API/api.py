import os
from flask import Flask, request, send_file, after_this_request
import zipfile

from flask_restful import Api, Resource
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import datetime
from functools import wraps
import json

from GraidAis_Back.merge_uploads import update_db
from GraidAis_Back.Data_base.Data_Base import Data_Base

app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = '14312412i4rg8ogso8vsdvcs82rkl2rsd'

UPLOAD_FOLDER = '../uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
db_name = "../grade.db"

# Helper function to generate JWT
def generate_token(username):
    token = jwt.encode({
        'user': username,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    }, app.config['SECRET_KEY'], algorithm="HS256")
    return token


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return json.dumps({'message': 'Token is missing!'}, indent = 4), 401
        try:
            token = token.split()[1]
            jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        except:
            print(token)
            return json.dumps({'message': 'Token is invalid!'}, indent = 4), 401
        return f(*args, **kwargs)

    return decorated

class Register(Resource):
    db_name = db_name

    def post(self):
        data = request.get_json()
        username = data['username']
        password = data['password']

        db = Data_Base(self.db_name)
        hashed_password = generate_password_hash(password)

        try:
            db.insert_user(username, hashed_password)
            return json.dumps({"message": "User registered successfully!"}, indent = 4), 201
        except Exception as e:
            return json.dumps({"message": f"User registration failed: {str(e)}"}, indent = 4), 400


class Login(Resource):
    db_name = db_name

    def post(self):
        data = request.get_json()
        username = data['username']
        password = data['password']
        db = Data_Base(self.db_name)
        user = db.get_user(username)

        if user and check_password_hash(user[2], password):
            token = generate_token(username)
            return json.dumps({"token": token}, indent = 4), 200
        else:
            return json.dumps({"message": "Invalid credentials!"}, indent = 4), 400

class ProtectedResource(Resource):
    @token_required
    def get(self):
        return json.dumps({"message": "This is a protected resource!"}, indent = 4), 200


class Grade_table(Resource):
    db_name = db_name

    def get(self, table_name, number):
        db = Data_Base(self.db_name)
        table = db.get_table(table_name, number)
        table = table.to_dict()
        return json.dumps(table, indent = 4)


class Grade_colums_name(Resource):
    db_name = db_name

    def get(self, table_name):
        db = Data_Base(self.db_name)
        colums = db.get_list_table_colums(table_name)
        return json.dumps(colums, indent = 4), 200


class Filter(Resource):
    db_name = db_name

    def post(self, table_name):
        all_filters = request.get_json()
        all_filters = all_filters.get('allFilters', {})
        db = Data_Base(self.db_name)
        search_table = db.keyword_search(table_name, all_filters)
        if "Поиск" in all_filters.keys():
            search_table = db.full_text_search(search_table, all_filters["Поиск"])
        search_table = search_table.to_dict()
        response = {
            "status": "success",
            "table": search_table,
            "download_url": ""
        }
        return response, 200


class send_excel_file(Resource):
    db_name = db_name
    download_folder = './downloads'

    def post(self, table_name):
        all_filters = request.get_json()
        all_filters = all_filters.get('allFilters', {})
        db = Data_Base(self.db_name)
        search_table = db.keyword_search(table_name, all_filters)
        if "Поиск" in all_filters.keys():
            search_table = db.full_text_search(search_table, all_filters["Поиск"])

        excel_filename = './output.xlsx'
        search_table.to_excel(excel_filename, index=False)

        @after_this_request
        def remove_file(response):
            try:
                os.remove(excel_filename)
            except Exception as e:
                print(f"Error removing file: {e}")
            return response

        return send_file(excel_filename, as_attachment=True)


class get_unique_elements_in_colums(Resource):
    db_name = db_name

    def get(self, table_name):
        db = Data_Base(self.db_name)
        unique_elements = db.get_unique_elements(table_name)
        return unique_elements, 200

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)


class update_refresh_db(Resource):
    def post(self):
        if 'files' not in request.files:
            return {"error": "Нет файлов в запросе"}, 400

        files = request.files.getlist('files')
        saved_files = []
        file_names = []
        update_mode = eval(request.form.get('updateMode', None))

        unzip_folder = os.path.join(app.config['UPLOAD_FOLDER'], 'unzipped')
        os.makedirs(unzip_folder, exist_ok=True)

        try:
            for file in files:
                if file.filename == '':
                    return {"error": "Один из файлов не имеет имени"}, 400

                filename = secure_filename(file.filename)
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

                file.save(file_path)

                if filename.endswith('.zip'):
                    try:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            bad_file = zip_ref.testzip()
                            if bad_file:
                                return {"error": f"Архив {filename} повреждён, ошибка в файле {bad_file}"}, 400

                            for member in zip_ref.infolist():
                                if member.filename.startswith('__MACOSX') or member.filename.endswith('.DS_Store'):
                                    continue

                                zip_ref.extract(member, unzip_folder)

                        for root, dirs, extracted_files in os.walk(unzip_folder):
                            for extracted_file in extracted_files:
                                extracted_file_path = os.path.join(root, extracted_file)

                                if extracted_file.lower().endswith('.csv') or extracted_file.lower().endswith('.xlsx'):
                                    file_names.append(extracted_file_path)
                                    saved_files.append(extracted_file_path)
                                else:
                                    return {"error": f"Неподдерживаемый тип файла: {extracted_file}"}, 400

                    except zipfile.BadZipFile:
                        return {"error": f"Файл {filename} не является допустимым ZIP архивом"}, 400
                    except Exception as e:
                        return {"error": f"Ошибка при разархивировании {filename}: {str(e)}"}, 500
                    finally:
                        os.remove(file_path)

                else:
                    file_names.append(file_path)
                    saved_files.append(file_path)

            update_db(file_names, update_mode)

        except Exception as e:
            return {"error": f"Ошибка: {str(e)}"}, 500

        @after_this_request
        def remove_files(response):

            try:

                for file in saved_files:

                    if os.path.exists(file):
                        os.remove(file)

                for root, dirs, files in os.walk(unzip_folder):

                    for file in files:
                        os.remove(os.path.join(root, file))

            except Exception as e:

                print(f"Error removing file: {e}")

            return response

        return {"message": "База данных успешно обновлена"}, 200

api.add_resource(update_refresh_db, "/api/upload_files")
api.add_resource(Register, "/api/register")
api.add_resource(Login, "/api/login")
api.add_resource(ProtectedResource, "/api/protected")
api.add_resource(Grade_table, "/api/get_table/<table_name>/<int:number>")
api.add_resource(Grade_colums_name, "/api/get_colum/<table_name>")
api.add_resource(Filter, "/api/receive_json/<table_name>")
api.add_resource(send_excel_file, "/api/send_excel/<table_name>")
api.add_resource(get_unique_elements_in_colums, "/api/get_unique_elementss/<table_name>")

if __name__ == '__main__':
    app.run(debug=True, port=5003)
