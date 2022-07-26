from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logger

logs = logger.my_logs('database.py')


class cassandra_db:
    def __init__(self):
        """
        Constructor to obtain the cassandra_db attributes from the credentials file
        """
        f = open('cassandra_credentials.txt')
        f.seek(0)
        self.file_address = f.readline()[:-1]
        self.id = f.readline()[:-1]
        self.secret = f.readline()[:]
        f.close()

    def connect_db(self):
        """
        Function to create a new cassandra session.
        :return: A cassandra session object
        """
        try:
            cloud_config = {'secure_connect_bundle': self.file_address}
            auth = PlainTextAuthProvider(self.id, self.secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth)
            session = cluster.connect()
            return session
        except Exception as e:
            logs.log_error('Error in connecting to the cassandra database:',str(e))
            raise Exception('Error in connecting to the cassandra database:\n' + str(e))



