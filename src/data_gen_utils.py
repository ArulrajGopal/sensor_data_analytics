import duckdb
from config import *


def model_data_gen(file_num, rows):

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/Model/ds_{rows}_rows_file_{file_num}.csv'

    duckdb.execute(f"""
        COPY (
            SELECT  
                t.row_id AS model_id,
                SUBSTR('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 1, 1 + (RANDOM() * 25)::INT) AS model_name,
                CAST(NOW() - INTERVAL (RANDOM() * 365) DAY AS TIMESTAMP) AS __insert_time
            FROM generate_series(1, {rows}) t(row_id)
        )
        TO '{output_path}'
        (FORMAT CSV, HEADER TRUE, DELIMITER ',')
    """)
    
    print(f"Completed file {file_num}: {output_path}")
    return output_path




def variant_data_gen(file_num, rows):

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/Variant/ds_{rows}_rows_file_{file_num}.csv'

    duckdb.execute(f"""
        COPY (
            SELECT  
                t.row_id as variant_id,
                SUBSTR('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 1, 1 + (RANDOM() * 20)::INT) AS variant_name,
                (1 + (RANDOM() * 99)::INT) AS model_id,
                CAST(NOW() - INTERVAL (RANDOM() * 365) DAY AS TIMESTAMP) AS __insert_time
            FROM generate_series(1, {rows}) t(row_id)
        )
        TO '{output_path}'
        (FORMAT CSV, HEADER TRUE, DELIMITER ',')
    """)
    
    print(f"Completed file {file_num}: {output_path}")
    return output_path




def options_data_gen(file_num, rows):

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/Options/ds_{rows}_rows_file_{file_num}.csv'

    duckdb.execute(f"""
        COPY (
            SELECT  
                t.row_id as option_id,
                SUBSTR('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 1, 1 + (RANDOM() * 20)::INT) AS option_name,
                SUBSTR('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 1, 1 + (RANDOM() * 150)::INT) AS option_description,
                CAST(NOW() - INTERVAL (RANDOM() * 365) DAY AS TIMESTAMP) AS __insert_time

            FROM generate_series(1, {rows}) t(row_id)
        )
        TO '{output_path}'
        (FORMAT CSV, HEADER TRUE, DELIMITER ',')
    """)
    
    print(f"Completed file {file_num}: {output_path}")
    return output_path


