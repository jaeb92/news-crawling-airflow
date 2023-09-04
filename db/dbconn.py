import yaml
import sys
import psycopg2
import psycopg2.extras

from datetime import datetime

with open('db/config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
   

class Singleton(type):
    _intances = {}
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._intances:
            cls._intances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._intances[cls].__init__(*args, **kwargs)
            
        return cls._intances[cls]
    

class Database(metaclass=Singleton):
    
    def __init__(self) -> None:
        self.db = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            dbname=config['database'],
            user =config['user'],
            password=config['password']
        )

        self.cursor = self.db.cursor()
    
    def get_conn(self):
        return self.db
    
    def close(self):
        self.cursor.close()
        self.db.close()
    
    def create_table(self, ddl_file: str):
        with open(ddl_file, 'r') as f:
            ddl = f.read()
            
        self.execute(ddl)
        self.commit()
        
    def table_schema(self, table: str):
        """
        return tuple of table column name, column type and column maximum length

        Args:
            table (str): table name

        Returns:
            tuple: tuple of column name, column type, column maximum length 
        """
        res = self.select(f"\
            select column_name, data_type, character_maximum_length \
            from information_schema.columns \
            where table_catalog = \'{config['database']}\' and table_name = \'{table}\' order by ordinal_position")
        return res

    def get_table_column_name(self, table):
        return [info[0] for info in self.table_schema(table)]
        
    def execute(self, query: str, vars=None):
        self.cursor.execute(query=query, vars=vars)
        
    def fetch(self):
        """
        return query result 

        Returns:
            list: result of query (ex. select * from test -> [(col1, col2), (col1, col2), ... ])
        """
        
        return self.cursor.fetchall()
        
    def commit(self):
        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
    
    def delete(self, q):
        self.execute(q)
        self.commit()
        
    def select(self, q):
        self.execute(q)
        row = self.fetch()
        self.commit()
        return row
    
    def insert(self, q: str, v: tuple):
        self.execute(query=q, vars=v)
        self.commit()
    
    def insert_bulk(self, q: str, arg: list):
        psycopg2.extras.execute_values(cur=self.cursor, sql=q, argslist=arg)
        self.commit()
        
        
if __name__ == '__main__':
    db = Database()
    db.insert_bulk()
    # cols = db.table_schema('news')
    # col = ','.join(col[0] for col in cols)
    # templates = ','.join('%s' for i in range(len(cols)))
    # query = f"insert into news ({col}) values ({templates})"
    # values = ('sample news2', '1aweoifjawe', 'oiwajef', 'awioefjawe', datetime.now(), 'test')

