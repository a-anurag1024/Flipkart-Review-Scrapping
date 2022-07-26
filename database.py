from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logger

logs = logger.my_logs('database.py')


class cassandra_db:
    def __init__(self):
        """
        Constructor to obtain the cassandra_db attributes from the credentials file and to initialize the connector, keyspace name and table name to None
        """
        f = open('cassandra_credentials.txt')
        f.seek(0)
        self.file_address = f.readline()[:-1]
        self.id = f.readline()[:-1]
        self.secret = f.readline()[:]
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
            cloud_config = {'secure_connect_bundle': self.file_address}
            auth = PlainTextAuthProvider(self.id, self.secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth, protocol_version=4)
            session = cluster.connect()
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
            logs.log_info("Table {} created with attributes {}. Setting the current table to {}...".format(tname, columns, tname))
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
