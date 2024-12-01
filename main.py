import pandas as pd
from merge_uploads import merge_uploads
from Data_base.Data_Base import Data_Base
from werkzeug.security import generate_password_hash, check_password_hash


def main(paths, name_db, new_base):
    db = Data_Base(name_db)
    all_data = None
    if new_base == "True":
        all_data = merge_uploads(paths)
    else:
        old_data = db.get_table("people")
        marged_uploads = merge_uploads(paths)
        all_data = pd.concat([marged_uploads, old_data], ignore_index=True)

    db.create_db(all_data, "people")
    db.close()

    return


# main(["./uploads/Upload_3.csv", "./uploads/Upload_2.csv"], "grade.db", True)
