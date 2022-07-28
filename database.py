from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
import logger
import pandas as pd

logs = logger.my_logs('database.py')


class cassandra_db:
    def __init__(self):
        """
        Constructor to obtain the cassandra_db attributes from the credentials file and to initialize the connector, keyspace name and table name to None
        """
        f = open('cassandra_credentials.txt')
        f.seek(0)
        self._file_address = f.readline()[:-1]
        self._id = f.readline()[:-1]
        self.__secret = f.readline()[:]
        f.close()
        self.session = None
        self.table = None
        self.keyspace = None

    def connect_db(self):
        """
        Function to create a new cassandra session.
        :return: None
        """
        try:
            cloud_config = {'secure_connect_bundle': self._file_address}
            auth = PlainTextAuthProvider(self._id, self.__secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth, protocol_version=4)
            session = cluster.connect()
            session.default_timeout = 60
            self.session = session
            logs.log_info("The connection to the cassandra driver has been established !!")
        except Exception as e:
            logs.log_error('Error in connecting to the cassandra database:', str(e))
            raise Exception('Error in connecting to the cassandra database:\n' + str(e))

    def check_null_connection(self):
        """
        To check if the connection has been established or not.
        :return: None
        """
        if self.session is None:
            logs.log_warning(
                "The connection has not been established yet. Creating the connection by calling connect_db()...")
            self.connect_db()

    def establish_keyspace(self, ksname):
        """
        Function to check the presence of the given keyspace and make it the current keyspace.
        :param ksname: Name of the keyspace
        :return: None
        """
        try:
            self.check_null_connection()
            r = self.session.execute("select * from system_schema.keyspaces;")
            logs.log_info("Establishing keyspace...")
            for i in r:
                d = i[0]
                if d == ksname.strip().lower():
                    self.session.execute("use {}".format(ksname.strip().lower()))
                    self.keyspace = ksname.strip().lower()
                    logs.log_info("The keyspace {} selected to be used.".format(ksname.strip().lower()))
                    return
            else:
                logs.log_warning("Keyspace {} not found !!".format(ksname))
                logs.log_info(
                    "To create the keyspace {}, use the UI of DataStax Cassandra website. Creating the keyspace from commandline is giving issues.".format(
                        ksname))
                raise Exception("Keyspace {} not found !!".format(ksname) +
                                "\nTo create the keyspace {}, use the UI of DataStax Cassandra website. Creating the keyspace from commandline is giving issues.".format(
                                    ksname))
        except Exception as e:
            logs.log_error("Error in searching the keyspace: ", str(e))
            raise Exception("Error in searching the keyspace: \n" + str(e))

    def check_null_keyspace(self):
        """
        To check if the keyspace has been selected or not.
        :return: None
        """
        if self.keyspace is None:
            logs.log_warning(
                "The keyspace has not been selected yet. Select the keyspace using establish_keyspace() function.")
            raise Exception(
                "The keyspace has not been selected yet. Select the keyspace using establish_keyspace() function.")

    def list_tables(self):
        """
        Function to list the table names present in the selected keyspace
        :return: list object containing table names as string
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            query = "select * from system_schema.tables where keyspace_name = '{}';".format(self.keyspace)
            rows = self.session.execute(query)
            table_list = []
            for i in rows:
                table_list.append(i[1])
            return table_list
        except Exception as e:
            logs.log_error("Error in finding the list of tables.", str(e))
            raise Exception("Error in finding the list of tables.\n" + str(e))

    def drop_table(self, tname):
        """
        Function to drop the given table from the database.
        :param tname: table name
        :return: None
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            query = "Drop table IF EXISTS {}.{} ;".format(self.keyspace, tname)
            self.session.execute(query)
            logs.log_info("Table {} deleted/does not exists from the keyspace {}".format(tname, self.keyspace))
            return
        except Exception as e:
            logs.log_error("Error in deleting the table {}:".format(tname), str(e))
            raise Exception("Error in deleting the table {}: \n".format(tname) + str(e))

    def establish_table(self, tname, columns):
        """
        Function to create/set table in an already selected keyspace using the attributes passed.
        \n WARNING: This function drops one of the tables if the total number of tables is above 50 in the current keyspace
        \n REQUIRED: Exactly one atribute needs to be denoted as primary key in the columns arguement
        \n example columns value: "id int PRIMARY KEY, name text, roll_no int, height int"
        :param columns: string containing the information on column name and column type in the format "coloumn1_name coloumn1_type, coloumn2_name coloumn2_type, ...".
        :param tname: name of the table
        :return: None
        """
        try:
            table_list = self.list_tables()
            if tname in table_list:
                logs.log_info("Table {} already exists. Setting the current table to {}...".format(tname, tname))
                self.table = tname
                return
            if len(table_list) >= 50:
                logs.log_warning("Too many tables in the database !!!.")
                logs.log_warning("Deleting the table {}".format(table_list[0]))
                self.drop_table(table_list[0])
            query = "CREATE TABLE " + tname.strip() + " (" + columns + " );"
            self.session.execute(query)
            logs.log_info(
                "Table {} created with attributes {}. Setting the current table to {}...".format(tname, columns, tname))
            self.table = tname
        except Exception as e:
            logs.log_error("Error in creating table {}".format(tname), str(e))
            raise Exception("Error in creating table {}: \n".format(tname) + str(e))

    def check_null_table(self):
        """
        To check if the table has been selected or not.
        :return: None
        """
        if self.table is None:
            logs.log_warning(
                "The table has not been selected yet. Select the table using establish_table() function.")
            raise Exception(
                "The table has not been selected yet. Select the table using establish_table() function.")

    def get_col_names(self):
        """
        Function to get the column names of the selected table.
        :return: list of column names
        """
        query = "select column_name from system_schema.columns where keyspace_name = '{}' AND table_name = '{}' ALLOW FILTERING".format(
            self.keyspace, self.table)
        rows = self.session.execute(query)
        return [i[0] for i in rows]

    def add_single_data(self, values):
        """
        Function to add a singe data instance.
        :param values: Values to be updated along with thier column name in a dictionary
        :return: None
        """
        try:
            logs.log_info("Begin to add single data instance to the table {}.{}".format(self.keyspace, self.table))
            col_names = self.get_col_names()
            if not all(i in col_names for i in list(values.keys())):
                logs.log_warning("Invalid Column names passed.")
                raise Exception("Invalid Column names passed")
            col_format = ""
            for i in values.keys():
                col_format = col_format + i + ", "
            val_format = str(values.values())[13:-2]
            query = "Insert Into {}({}) VALUES ({});".format(self.table, col_format[:-2], val_format)
            self.session.execute(query)
            logs.log_info("Single data instance added to the table {}".format(self.table))
        except Exception as e:
            logs.log_error("Error in single entry to the table: ", str(e))
            raise Exception("Error in single entry to the table: \n" + str(e))

    def add_batch_data(self, df):
        """
        Function to add multiple records at once from the supplied dataframe.
        :pram df: batch data in pandas dataframe format
        :return: None
        """
        try:
            logs.log_info("Begin to add batch data instances to the table {}.{}".format(self.keyspace, self.table))
            col_names = self.get_col_names()
            if not all(i in col_names for i in list(df.columns)):
                logs.log_warning("Invalid Column names passed.")
                raise Exception("Invalid Column names passed")
            col_format = ""
            for i in df.columns:
                col_format = col_format + i + ", "
            query = "insert into {}({}) VALUES (".format(self.table, col_format[:-2]) + ('%s, ' * len(df.columns))[
                                                                                        :-2] + " );"
            batch = BatchStatement()
            for i in range(len(df)):
                batch.add(query, [j for j in df.iloc[i]])
            self.session.execute(batch)
            logs.log_info("Batch data records added to the table {}".format(self.table))
        except Exception as e:
            logs.log_error("Error in loading Batch data records: ", str(e))
            raise Exception("Error in loading Batch data records: \n" + str(e))

    def read_table(self, query):
        """
        Function to execute Read operation (select operation) on the selected table and return the result
        :param query: select query in CQL
        :return: pandas dataframe consisting the results
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            self.check_null_table()
            rows = self.session.execute(str(query))
            col = rows.column_names
            df = pd.DataFrame(columns=col)
            for i in rows:
                df.loc[len(df)] = [i[j] for j in range(len(col))]
            logs.log_info("Returned search result for CQL query: " + query)
            return df
        except Exception as e:
            logs.log_error("Error in reading the table with the given CQL query.", str(e))
            raise Exception("Error in reading the table with the given CQL query." + str(e))

    def update_table_values(self, uid, values, primary_key):
        """
        Function to update the values of the records recognised by their unique ids
        :param uid: values of the primary_key in list format
        :param values: list of key value pair of the attributes and values to be updated corresponding to the uid
        :param primary_key: column name of the primary_key
        :return:None
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            self.check_null_table()
            if type(uid) != list:
                uid = [uid]
            l = len(uid)
            for i in range(l):
                query = "UPDATE {}.{} SET ".format(self.keyspace, self.table)
                update = ""
                for j in range(len(values[i].keys())):
                    dval = ("'" + str(list(values[i].values())[j]) + "'") if (
                            type(list(values[i].values())[j]) == str) else (str(list(values[i].values())[j]))
                    update = update + str(list(values[i].keys())[j]) + "= " + dval + ", "
                query = query + update[:-2] + " WHERE {} = ".format(primary_key)
                query = query + (("'" + uid[i] + "'") if type(uid[i]) == str else str(uid[i])) + ';'
                self.session.execute(query)
            logs.log_info("Updated the table {}".format(self.table))

        except Exception as e:
            logs.log_error("Error in updating the table: ", str(e))
            raise Exception("Error in updating the table: \n", str(e))

    def alter_table_column_names(self, up_col):
        """
        Function to update the column names of the table. Can only rename the primary key column names. Columns which are not primary key cannot be renamed via CQL.
        :param up_col: dictionary containing updated names of the columns in the format {'old_col_name':'new_col_name'}
        :return: None
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            self.check_null_table()
            qr1 = "ALTER TABLE {}.{} RENAME ".format(self.keyspace, self.table)
            for i in range(len(up_col)):
                query = qr1 + str(list(up_col.keys())[i]) + " TO " + str(list(up_col.values())[i]) + ";"
                self.session.execute(query)
            logs.log_info("Column names of the table {} updated".format(self.table))
        except Exception as e:
            logs.log_error("Error in updating column names: ", str(e))
            raise Exception("Error in updating the column names \n: ", str(e))

    def alter_table_add_column(self, add_col):
        """
        Function to add the supplied column
        :param add_col: dictionary containing new column names and thier type in the format {'new_col_name':'new_col_type'}
        :return: None
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            self.check_null_table()
            qr1 = "ALTER TABLE {}.{} ADD ".format(self.keyspace, self.table)
            for i in range(len(add_col)):
                query = qr1 + str(list(add_col.keys())[i]) + " " + str(list(add_col.values())[i]) + ";"
                self.session.execute(query)
            logs.log_info("New Column(s) added to the table {}".format(self.table))
        except Exception as e:
            logs.log_error("Error in adding new columns : ", str(e))
            raise Exception("Error in adding new columns \n: ", str(e))

    def alter_table_drop_column(self, col_name):
        """
        Function to drop columns.
        :param col_name: list of column names to be droped
        :return: None
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            self.check_null_table()
            qr1 = "ALTER TABLE {}.{} DROP ".format(self.keyspace, self.table)
            if type(col_name) == str:
                query = qr1 + str(col_name) + ";"
                self.session.execute(query)
            else:
                for i in range(len(col_name)):
                    query = qr1 + str(col_name[i]) + ";"
                    self.session.execute(query)
            logs.log_info("Dropped Column(s) from the table {}".format(self.table))
        except Exception as e:
            logs.log_error("Error in Dropping columns : ", str(e))
            raise Exception("Error in Dropping columns \n: ", str(e))

    def delete_rows(self, uid, primary_key):
        """
        Function to delete records of the given id values.
        :param uid: list containing ids of the records deleted
        :param primary_key: column name which is the id of the table (primary key)
        :return: None
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            self.check_null_table()
            qr1 = "DELETE FROM {}.{} WHERE {} = ".format(self.keyspace, self.table, primary_key)
            if type(uid) != list:
                query = qr1 + (("'" + uid + "'") if type(uid) == str else str(uid))
                self.session.execute(query)
            else:
                for i in range(len(uid)):
                    query = qr1 + (("'" + uid[i] + "'") if type(uid[i]) == str else str(uid[i]))
                    self.session.execute(query)
            logs.log_info("Row(s) deleted from tabel {}".format(self.table))
            return
        except Exception as e:
            logs.log_error("Error in deleting rows : ", str(e))
            raise Exception("Error in deleting rows \n: ", str(e))

    def delete_data(self, uid, attributes, primary_key):
        """
        Function to delete the data from the given attributes of the selected records based on their primary key
        :param uid: values of the primary keys from where data needs to be deleted in list format
        :param attributes: list of column names from where data needs to be deleted
        :param primary_key: column name which is the id of the table (primary key)
        :return: None
        """
        try:
            self.check_null_connection()
            self.check_null_keyspace()
            self.check_null_table()
            if type(attributes) != list:
                attributes = [attributes]
                if type(uid) != list:
                    uid = [uid]
            l = len(uid)
            for i in range(l):
                qr1 = "DELETE "
                att = ""
                for j in range(len(attributes)):
                    att = att + str(attributes[j]) + ", "
                qr2 = qr1 + att[:-2] + " FROM {}.{} WHERE {} = ".format(self.keyspace, self.table, primary_key)
                query = qr2 + (("'" + uid[i] + "'") if type(uid[i]) == str else str(uid[i]))
                self.session.execute(query)
            logs.log_info("Deleted some data points from the table {}".format(self.table))
        except Exception as e:
            logs.log_error("Error in deleting the given data points from the table: ", str(e))
            raise Exception("Error in deleting the given data points from the table: \n", str(e))
