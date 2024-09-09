import pandas as pd

from merge_uploads import merge_uploads
from use_sql_lite.Data_Base import Data_Base


def main(paths, name_db, new_base):
    db = Data_Base(name_db)
    all_data = None
    if not new_base:
        old_data = db.get_table("people")
        marged_uploads = merge_uploads(paths)
        all_data = pd.concat([marged_uploads, old_data], ignore_index=True)
    else:
        all_data = merge_uploads(paths)

    db.create_db(all_data, "people")
    db.close()

    return


main(["../uploads/Upload_3.csv", "../uploads/Upload_2.csv"], "grade.db", True)
