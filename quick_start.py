from werkzeug.security import generate_password_hash

from GraidAis_Back.Data_base.DataBase import DataBase

db_name = "./grade.db"
db = DataBase(db_name)

db.create_users_table()

hashed_password = generate_password_hash("admin")
username = "admin"

db.insert_user(username, hashed_password)