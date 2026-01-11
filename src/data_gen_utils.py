import duckdb

def model_data_gen(file_num, rows):

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/ds_model_{rows}_rows_file_{file_num}.csv'

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

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/ds_variant_{rows}_rows_file_{file_num}.csv'

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

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/ds_options_{rows}_rows_file_{file_num}.csv'

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


def vehile_instance_data_gen(file_num, rows):

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/ds_vehile_instance_{rows}_rows_file_{file_num}.csv'

    duckdb.execute(f"""
        COPY (
            SELECT  
                t.row_id as vehicle_id,
                (1 + (RANDOM() * 40000000)::INT) AS owner_id,
                (1 + (RANDOM() * 99)::INT) AS model_id,
                (1 + (RANDOM() * 499)::INT) AS variant_id,
                (1 + (RANDOM() * 999)::INT) AS option_a,
                (1 + (RANDOM() * 999)::INT) AS option_b,
                (1 + (RANDOM() * 999)::INT) AS option_c,
                (1 + (RANDOM() * 999)::INT) AS option_d,
                CAST(NOW() - INTERVAL (RANDOM() * 365) DAY AS TIMESTAMP) AS __insert_time

            FROM generate_series(1, {rows}) t(row_id)
        )
        TO '{output_path}'
        (FORMAT CSV, HEADER TRUE, DELIMITER ',')
    """)
    
    print(f"Completed file {file_num}: {output_path}")
    return output_path


def unit_a_data_gen(file_num, rows):

    output_path = f'C:/Users/Arulraj Gopal/OneDrive/Desktop/data/ds_unit_a_data_{rows}_rows_file_{file_num}.csv'

    duckdb.execute(f"""
        COPY (
            SELECT  
                t.row_id as record_id,
                (1 + (RANDOM() * 39999999)::INT) AS vehicle_id,
                   

                CAST(NOW() - INTERVAL (RANDOM() * 365) DAY AS TIMESTAMP) AS __insert_time

            FROM generate_series(1, {rows}) t(row_id)
        )
        TO '{output_path}'
        (FORMAT CSV, HEADER TRUE, DELIMITER ',')
    """)
    
    print(f"Completed file {file_num}: {output_path}")
    return output_path



