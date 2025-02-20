from typing import NamedTuple

# Input dictionary
data = {
    'go_methods': {
        'Column_names': ['order_method_code', 'order_method_type'],
        'data_type': {
            'order_method_code': 'bigint',
            'order_method_type': 'varchar(255)'
        },
        'merge_column': [],
        'partition_on': [],
        'masking_column': [],
        'partition_column': []
    }
}

# Mapping of SQL types to Python types
sql_to_python = {
    'bigint': int,
    'varchar(255)': str
}

# Extract column names and their data types
columns = data['go_methods']['Column_names']
column_types = data['go_methods']['data_type']

# Convert dictionary to a list of tuples (name, type)
fields = [(col, sql_to_python[column_types[col]]) for col in columns]

# Dynamically create the NamedTuple class
ExampleRow = NamedTuple('ExampleRow', fields)

# Test the class
row = ExampleRow(order_method_code=123, order_method_type="Online")
print(row)
print(row.order_method_code, type(row.order_method_code))
print(row.order_method_type, type(row.order_method_type))
