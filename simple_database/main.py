import os
import json
import datetime

from config import BASE_DB_FILE_PATH
from exceptions import ValidationError


LIST_OF_DB_INSTANCES = []

def create_database(db_name):
    if not os.path.exists(BASE_DB_FILE_PATH):
        os.makedirs(BASE_DB_FILE_PATH)
    
    if db_name in os.listdir(BASE_DB_FILE_PATH):
        raise ValidationError(
            'Database with name "{}" already exists.'.format(db_name))

    db_path = BASE_DB_FILE_PATH + db_name
    return Database(db_name, db_path)


def connect_database(db_name):
    for db_instance in LIST_OF_DB_INSTANCES:
        if db_instance.db_name == db_name:
            return db_instance
    raise ValidationError('Given database does not exist.')


def get_data(db_path):
    """Open database file and parse json into python dictionary"""
    with open(db_path) as db_json:
        try:
            data = json.load(db_json)
        except ValueError:
            data = ''
    return data
    
    
def update_data(db_path, new_data):
    """Replace data in db_path with given new_data"""
    open(db_path, 'w').close()
    with open(db_path, 'a') as db:
        updated_data = json.dumps(new_data)
        db.write(updated_data)    
    
    
def format_date(date):
    return '{}-{}-{}'.format(date.year, date.month, date.day)


class Database(object):
    
    def __init__(self, db_name, db_path):
        self.db_name = db_name
        self.db_path = db_path
        self.tables = []
        with open(self.db_path, 'a') as db:
            db.write('{}')
        LIST_OF_DB_INSTANCES.append(self)
        
    def create_table(self, table_name, columns=None):
        table_obj = Table(table_name, columns, self.db_path)
        setattr(self, table_name, table_obj)
        self.tables.append(table_name)
    
    def show_tables(self):
        return self.tables
    
    
class Table(object):
    
    def __init__(self, table_name, columns, db_path):
        self.name = table_name
        self.columns = columns
        self.db_path = db_path
        db_data = get_data(db_path)
        db_data[table_name] = []
        update_data(self.db_path, db_data)
        
    def insert(self, *args):
        # Validate amount of fields
        if len(args) != len(self.columns):
            raise ValidationError('Invalid amount of field')
        
        # Validate types of given args
        for index, value in enumerate(args):
            type_given = type(value).__name__
            type_expected = self.columns[index]['type']
            if type_given != type_expected:
                raise ValidationError(
                    'Invalid type of field "{}": Given "{}", expected "{}"'.format(
                        self.columns[index]['name'], type_given, type_expected))
                
        # Build new object with given data
        obj = {}
        for index, value in enumerate(args):
            if isinstance(value, datetime.date):
                value = format_date(value)
            obj[self.columns[index]['name']] = value
        
        data = get_data(self.db_path)
        data[self.name].append(obj)
        update_data(self.db_path, data)

    def count(self):
        data = get_data(self.db_path)
        return len(data[self.name])
        
    def query(self, **kwargs):
        data = get_data(self.db_path)
        for key, value in kwargs.items():
            result = [row for row in data[self.name] if row[key] == value]
        
        rows = []
        for row_dict in result:
            row = Row(row_dict)
            rows.append(row)

        count = 0
        while len(rows) > count:
            yield rows[count]
            count += 1
            
            
    def all(self):
        data = get_data(self.db_path)
        result = [row for row in data[self.name]]

        rows = []
        for row_dict in result:
            row = Row(row_dict)
            rows.append(row)
        
        count = 0
        while len(rows) > count:
            yield rows[count]
            count += 1
        
    def describe(self):
        return self.columns
        
        
class Row(object):
    
    def __init__(self, row_data):
        for attr_key, attr_value in row_data.items():
            setattr(self, attr_key, attr_value)
