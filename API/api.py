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

from GraidAis_Back.config import SECRET_KEY, DB_NAME
from GraidAis_Back.API.merge_uploads import update_db
from GraidAis_Back.Data_base.DataBase import DataBase

app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = SECRET_KEY

upload_folder = '../uploads'
app.config['UPLOAD_FOLDER'] = upload_folder

class Requests(Resource):
    db_name = DB_NAME
    download_folder = './downloads'


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
            return json.dumps({'message': 'Token is invalid!'}, indent = 4), 401
        return f(*args, **kwargs)

    return decorated

class Register(Requests):

    def post(self):
        data = request.get_json()
        username = data['username']
        password = data['password']

        db = DataBase(self.db_name)
        hashed_password = generate_password_hash(password)

        try:
            db.insert_user(username, hashed_password)
            return json.dumps({"message": "User registered successfully!"}, indent = 4), 201
        except Exception as e:
            return json.dumps({"message": f"User registration failed: {str(e)}"}, indent = 4), 400

class Login(Requests):

    def post(self):
        data = request.get_json()
        username = data['username']
        password = data['password']
        db = DataBase(self.db_name)
        user = db.get_user(username)

        if user and check_password_hash(user[2], password):
            token = generate_token(username)
            return json.dumps({"token": token}, indent = 4), 200
        else:
            return json.dumps({"message": "Invalid credentials!"}, indent = 4), 400

class ProtectedResource(Requests):
    @token_required
    def get(self):
        return json.dumps({"message": "This is a protected resource!"}, indent = 4), 200

class GradeTable(Requests):

    def get(self, table_name, number):
        db = DataBase(self.db_name)
        table = db.get_table(table_name, number)
        table = table.to_dict()
        return json.dumps(table, indent = 4)

class GradeColumsName(Requests):

    def get(self, table_name):
        db = DataBase(self.db_name)
        colums = db.get_list_table_colums(table_name)
        return json.dumps(colums, indent = 4), 200

class Filter(Requests):

    def post(self, table_name, number):
        all_filters = request.get_json()
        all_filters = all_filters.get('allFilters', {})
        db = DataBase(self.db_name)
        search_table = db.filers(table_name, all_filters)
        if "Поиск" in all_filters.keys():
            search_table = db.full_text_search(search_table, all_filters["Поиск"])
        search_table = search_table.head(100)
        search_table = search_table.to_dict()
        response = {
            "status": "success",
            "table": search_table,
            "download_url": ""
        }
        return response, 200

import tempfile

class SendExcelFile(Requests):

    def post(self, table_name):
        full_request = request.get_json()
        all_filters = full_request.get('allFilters', {})
        all_samples = full_request.get('selectedCheckboxes', {})

        db = DataBase(self.db_name)

        search_table = db.filers(table_name, all_filters)

        selected_columns = [column for column, is_selected in all_samples.items() if is_selected]
        search_table = search_table[selected_columns]

        if "Поиск" in all_filters.keys():
            search_table = db.full_text_search(search_table, all_filters["Поиск"])

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            search_table.to_excel(tmp.name, index=False)
            excel_filename = tmp.name

        @after_this_request
        def remove_file(response):
            try:
                os.remove(excel_filename)
            except Exception as e:
                print(f"Error removing file: {e}")
            return response

        return send_file(excel_filename, as_attachment=True, add_etags=False)


class GetUniqueElementsInColums(Requests):

    def post(self, table_name):
        full_request = request.get_json()
        colums = full_request.get('columsDrop', [])
        db = DataBase(self.db_name)
        unique_elements = db.get_unique_elements(table_name, colums)
        return unique_elements, 200

class UpdateRefreshDb(Requests):
    def post(self):

        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)

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

            update_db(file_names, update_mode, self.db_name)

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

api.add_resource(UpdateRefreshDb, "/api/upload_files")
api.add_resource(Register, "/api/register")
api.add_resource(Login, "/api/login")
api.add_resource(ProtectedResource, "/api/protected")
api.add_resource(GradeTable, "/api/get_table/<table_name>/<int:number>")
api.add_resource(GradeColumsName, "/api/get_colum/<table_name>")
api.add_resource(Filter, "/api/receive_json/<table_name>/<int:number>")
api.add_resource(SendExcelFile, "/api/send_excel/<table_name>")
api.add_resource(GetUniqueElementsInColums, "/api/get_unique_elements/<table_name>")

if __name__ == '__main__':
    app.run(debug=True, port=5003)
