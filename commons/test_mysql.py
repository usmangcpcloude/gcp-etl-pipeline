# Define column names and types
col_names = "['id', 'retailer_code', 'product_number', 'order_method_code', 'sale_date', 'quantity', 'unit_price', 'unit_sale_price']"
col_types = "{'id': 'bigint', 'retailer_code': 'bigint', 'product_number': 'bigint', 'order_method_code': 'bigint', 'sale_date': 'date', 'quantity': 'bigint', 'unit_price': 'bigint', 'unit_sale_price': 'bigint'}"

# Convert col_names and col_types to Python objects
import ast

# Parse column names
col_names_list = ast.literal_eval(col_names)

# Parse column types
col_types_dict = ast.literal_eval(col_types)

# Function to convert Beam Row object to a dictionary dynamically
def row_to_dict(row):
    return {name: convert_type(row[i], col_types_dict[name]) for i, name in enumerate(col_names_list)}

# Function to convert types based on col_types
def convert_type(value, col_type):
    if col_type == 'bigint':
        return int(value)
    elif col_type == 'date':
        return str(value)  # Adjust as needed for date formatting
    elif col_type == 'float':
        return float(value)
    else:
        return str(value)

# Define Parquet schema dynamically
import pyarrow

parquet_schema = pyarrow.schema([
    (name, pyarrow.int64() if col_type == 'bigint' else pyarrow.string() if col_type == 'string' else pyarrow.float64() if col_type == 'float' else pyarrow.string())
    for name, col_type in col_types_dict.items()
])