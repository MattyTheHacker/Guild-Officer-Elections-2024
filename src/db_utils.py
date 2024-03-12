import sqlite3
import traceback
import sys
import os
import json

def put_specific_data_into_db(dataset, table_name, date_generated, cur, conn):
    cur.execute("SELECT name FROM " + table_name)
    deps_in_db = [dep[0] for dep in cur.fetchall()]

    deps = {}
    for dep in dataset["Items"]:
        deps[dep["Name"]] = (dep["Voters"], dep["Eligible"])
    
    cur.execute("SELECT * FROM " + table_name)

    column_command = "ALTER TABLE " + table_name + " ADD COLUMN '" + date_generated + "' INTEGER"
    cur.execute(column_command)
    conn.commit()

    for dep in deps:
        if dep not in deps_in_db:
            print("[INFO] Inserting " + dep + " into database...")
            row_command = "INSERT INTO " + table_name + " (name, eligible) VALUES ('" + dep + "', " + str(deps[dep][1]) + ")"
            input_data_command = "UPDATE " + table_name + " SET '" + date_generated + "' = " + str(deps[dep][0]) + " WHERE name = '" + dep + "'"
            try:
                cur.execute(row_command)
                cur.execute(input_data_command)
                conn.commit()
            except sqlite3.Error as er:
                print('SQLite error: %s' % (' '.join(er.args)))
                print("Exception class is: ", er.__class__)
                print('SQLite traceback: ')
                exc_type, exc_value, exc_tb = sys.exc_info()
                print(traceback.format_exception(exc_type, exc_value, exc_tb))
        else:
            print("[INFO] Updating " + dep + " in database...")
            input_data_command = "UPDATE " + table_name + " SET '" + date_generated + "' = " + str(deps[dep][0]) + " WHERE name = '" + dep + "'"
            try:
                cur.execute(input_data_command)
                conn.commit()
            except sqlite3.Error as er:
                print('SQLite error: %s' % (' '.join(er.args)))
                print("Exception class is: ", er.__class__)
                print('SQLite traceback: ')
                exc_type, exc_value, exc_tb = sys.exc_info()
                print(traceback.format_exception(exc_type, exc_value, exc_tb))



def save_to_db(data, date_generated):
    # we're going to have a separate table for different sets of data:
    # departments, year, type (UG, PGR, PGT) etc...

    db_file_path = "../data/db/all_data.db"

    if not os.path.exists(db_file_path):
        print("[ERROR] Database file does not exist. A new one will be created...")

    conn = sqlite3.connect(db_file_path)
    cur = conn.cursor()

    tables = ["department_data", "sex_data", "year_data", "type_data","small_groups_data","large_groups_data", "societies_data","medium_groups_data", "college_data", "associations_data"]

    for table in tables:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='" + table + "'")

        if cur.fetchone() is None:
            print("[ERROR] Table " + table + " does not exist. Creating it now...")
            cur.execute("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, eligible INTEGER)")            


    for dataset in data["Groups"]:
        if "Department" in dataset["Name"]:
            table_name = "department_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Year of study" in dataset["Name"]:
            table_name = "year_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Student type" in dataset["Name"]:
            table_name = "type_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Sex" in dataset["Name"]:
            table_name = "sex_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Large Groups" in dataset["Name"]:
            table_name = "large_groups_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Small Groups" in dataset["Name"]:
            table_name = "small_groups_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Medium Group" in dataset["Name"]:
            table_name = "medium_groups_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Societies" in dataset["Name"]:
            table_name = "societies_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "College" in dataset["Name"]:
            table_name = "college_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)

        elif "Associations" in dataset["Name"]:
            table_name = "associations_data"
            put_specific_data_into_db(dataset, table_name, date_generated, cur, conn)


    conn.commit()

    cur.execute("SELECT * FROM department_data")

    print(cur.fetchall())

    conn.close()

def save_consolidated_to_db():
    db_file_path = "../data/db/all_data.db"

    if not os.path.exists(db_file_path):
        print("[ERROR] Database file does not exist. A new one will be created...")

    conn = sqlite3.connect(db_file_path)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS consolidated_society_data")

    # get a list of data file names
    data_files = os.listdir("../data/json/raw")
    data_groups = ["Societies", "Associations", "Small Groups", "Medium Groups", "Large Groups", "MedSoc Societies and Sports Clubs", "Volunteering Groups"]
    group_names = []
    most_recent_pull = data_files[-1]

    with open("../data/json/raw/" + most_recent_pull, "r") as file:
        data = json.load(file)

        for group in data["Groups"]:
            if group["Name"] in data_groups:
                for item in group["Items"]:
                    group_names.append(item["Name"])

    # remove dupes
    group_names = list(set(group_names))

    # create a new table for the consolidated society data, each column should be a society, with rows for each vote count
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='consolidated_society_data';")

    if cur.fetchone() is None:
        print("[ERROR] Table consolidated_society_data does not exist. Creating it now...")
        create_table_command = "CREATE TABLE consolidated_society_data (\
                                dt TEXT NOT NULL, \
                                name TEXT NOT NULL,\
                                votes INTEGER NOT NULL,\
                                PRIMARY KEY (dt, name));"
        cur.execute(create_table_command)

    # now we have a table with columns for each society, we can add the data
    for file in data_files:
        with open("../data/json/raw/" + file, "r") as file:
            data = json.load(file)
            for group in data["Groups"]:
                if group["Name"] in data_groups:
                    for item in group["Items"]:
                        society_name = item["Name"]
                        if society_name in group_names:
                            try:
                                # check if the society has already been added to the database
                                cur.execute("SELECT * FROM consolidated_society_data WHERE name = '%s'" % society_name)
                                if cur.fetchone() is not None:
                                    print("[INFO] " + society_name + " already exists in the database. Skipping...")
                                    continue

                                date_generated = os.path.basename(file.name).split(".")[0]

                                print("[INFO] Inserting " + society_name + " into database...")

                                society_name = society_name.replace("'", "''")

                                insert_data_command = "INSERT INTO consolidated_society_data (dt, name, votes) VALUES ('%s', '%s', %d);" % (date_generated, society_name, item["Voters"])

                                
                                cur.execute(insert_data_command)
                                conn.commit()
                            except sqlite3.Error as er:
                                print('SQLite error: %s' % (' '.join(er.args)))
                                print('The command was: ' + insert_data_command)
                                print("Exception class is: ", er.__class__)
                                print('SQLite traceback: ')
                                exc_type, exc_value, exc_tb = sys.exc_info()
                                print(traceback.format_exception(exc_type, exc_value, exc_tb))
                                exit(1)

    conn.close()



if __name__ == "__main__":
    save_consolidated_to_db()