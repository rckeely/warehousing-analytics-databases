import psycopg2

#-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #--
#
# DBConnection Class
#
#-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #--
class DBConnection():
    """Class to handle the database connection"""
    def __init__(self,
                 dbname="company_db",
                 user="postgres",
                 host="localhost"):
        """Constructor initialises connection and cursor"""
        self.configure_conn(dbname, user, host)
        self.connect()
        self.open_cursor()

    def __del__(self):
        """Destructor to tidy up if required"""
        if self.cur is not None:
            self.close_cursor()
        if self.conn is not None:
            self.disconnect()

    def connect(self):
        """Connect to configured host"""
        conn_str = f"dbname={self.dbname} user={self.user} host={self.host}"
        self.conn = psycopg2.connect(conn_str)

    def configure_conn(self, dbname, user, host):
        """Configure connection"""
        self.dbname=dbname
        self.user=user
        self.host=host

    def disconnect(self, close_cursor_b=False):
        """Close connection to database"""
        if close_cursor_b:
            self.close_cursor
        self.conn.close()
        self.conn = None

    def is_connected(self):
        """Check if connection is active"""
        if self.conn is None:
            return False
        else:
            return True

    def open_cursor(self):
        """Open database cursor"""
        self.cur = self.conn.cursor()

    def close_cursor(self):
        """Close database cursor"""
        self.cur.close()
        self.cur = None

    def select_query(self, query):
        """Execute SELECT query"""
        self.cur.execute(query)
        colnames = [desc[0] for desc in self.cur.description]
        return colnames, self.cur.fetchall()

    def insert_query(self, query, var_tuple):
        """Execute INSERT query"""
        self.cur.execute(query, var_tuple)
        self.conn.commit()

    def _delete_query(self, query):
        """Execute DELETE query :: Testing only"""
        # Not a fan of providing direct delete methods, testing only...
        self.cur.execute(query)
        self.conn.commit()

if __name__ == "__main__":
    conn = DBConnection()
    conn.connect()
    conn.disconnect()
