import pandas as pd
import pyodbc
import logging
import traceback

def get_cursor_data_dw_mssql():
    """Get cursor for MSSQL database data warehouse"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    ####"Sửa lại đoạn này giốn conn string nãy tui gửi nhé, với chỉnh lại username và password là thinhnd và DfL6iFnuCm#@wwE"
    MSSQL_SERVER = 'tcp:future-retail.database.windows.net'
    MSSQL_DW_DATABASE = 'FutureRetailDW'
    MSSQL_USERNAME = 'thinhnd'    
    MSSQL_PASSWORD = 'DfL6iFnuCm#@wwE'    
    CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=' + MSSQL_SERVER + ',1433;Database=' + MSSQL_DW_DATABASE + ';Uid=' + MSSQL_USERNAME + ';Pwd=' + MSSQL_PASSWORD + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    try:
        print(CONNECTION_STRING)
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        logging.info('Connected to MSSQL database')
        return cursor
    except Exception as e:
        logging.error('Error connecting to MSSQL database')
        logging.error(traceback.format_exc())

def load_dim_city_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    print(mssql_cursor)
    dw_table_name = "dbo.DimCity"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        city_key = row['CityKey']
        state_key = row['StateKey']
        city_name = row['CityName']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (CityKey, StateKey, CityName) 
            VALUES (?, ?, ?)""",
            city_key,
            state_key,
            city_name
        )
        mssql_cursor.commit()
    mssql_cursor.close()
    

def load_dim_customer_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.DimCustomer"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        customer_key = row['CustomerKey']
        customer_fname = row['Customer First Name']
        customer_lname = row['Customer Last Name']
        phone = row['Phone']
        city_key = row['CityKey']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (CustomerKey, CustomerFirstName, CustomerLastName, Phone, CityKey) 
            VALUES (?, ?, ?, ?, ?)""",
            customer_key,
            customer_fname,
            customer_lname,
            phone,
            city_key
        )
        mssql_cursor.commit()
    mssql_cursor.close()

def load_dim_date_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.DimDate"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        order_date_key = row['DateKey']
        order_date = row['Date']
        day = row['Day']
        weekday_name = row['WeekDayName']
        month = row['Month']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (OrderDateKey, OrderDate, Day, WeekdayName, Month) 
            VALUES (?, ?, ?, ?, ?)""",
            order_date_key,
            order_date,
            day,
            weekday_name,
            month
        )
        mssql_cursor.commit()

def load_dim_order_status_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.DimOrderStatus"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        order_status_key = row['OrderStatusKey']
        order_status_name = row['OrderStatusName']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (OrderStatusKey, OrderStatusName) 
            VALUES (?, ?)""",
            order_status_key,
            order_status_name,
        )
        mssql_cursor.commit()

def load_dim_return_reason_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.DimReturnReason"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        return_reason_key = row['ReturnReasonKey']
        return_reason_name = row['ReturnReasonName']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (ReturnReasonKey, ReturnReasonName) 
            VALUES (?, ?)""",
            return_reason_key,
            return_reason_name,
        )
        mssql_cursor.commit()

def load_dim_sale_agents_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.DimSaleAgent"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        sale_agent_key = row['SaleAgentKey']
        sale_agent_name = row['SaleAgentName']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (SaleAgentKey, SaleAgentName) 
            VALUES (?, ?)""",
            sale_agent_key,
            sale_agent_name,
        )
        mssql_cursor.commit()

def load_dim_state_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.DimState"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        state_key = row['StateKey']
        state_name = row['State']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (StateKey, StateName) 
            VALUES (?, ?)""",
            state_key,
            state_name,
        )
        mssql_cursor.commit()


def load_dim_product_catalog_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.DimProductCatalog"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        product_catalog_key = row['ProductCatalogKey']
        product_catalog_name = row['ProductCatalogName']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (ProductCatalogKey, ProductCatalogName) 
            VALUES (?, ?)""",
            product_catalog_key,
            product_catalog_name,
        )
        mssql_cursor.commit()

def load_fact_order_to_mssql(df_load: pd.DataFrame):
    mssql_cursor = get_cursor_data_dw_mssql()
    dw_table_name = "dbo.FactOrder"

    mssql_cursor.execute(f"""DELETE FROM {dw_table_name}""")
    mssql_cursor.commit()

    ####""Sửa lại là tên cột trong df_load mà ông muốn load vào bảng trong DW VÀ SỬA LẠI CÂU QUERY Ở DƯỚI NHA"
    for _, row in df_load.iterrows():
        order_id = row['Order ID']
        order_date_key = row['DateKey']
        customer_key = row['CustomerKey']
        product_catalog_key = row['ProductCatalogKey']
        sale_agent_key = row['SaleAgentKey']
        state_key = row['StateKey']
        order_status_key = row['OrderStatusKey']
        return_reason_key = row['ReturnReasonKey']
        quantity = row['Quantity Ordered']
        manufacture_price = row['Manufacturer Price']
        sale_price = row['Sale Price']
        total_profit = row['Total Profit (GMROI)']
        
        mssql_cursor.execute(f"""
            INSERT INTO {dw_table_name} 
            (OrderId, OrderDateKey, CustomerKey, ProductCatalogKey
            , SaleAgentKey, StateKey, OrderStatusKey, ReturnReasonKey
            , Quantity, ManufacturePrice, SalePrice, TotalProfit) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            int(order_id), int(order_date_key), int(customer_key), int(product_catalog_key),
            int(sale_agent_key), int(state_key), int(order_status_key), int(return_reason_key),
            int(quantity), int(manufacture_price), int(sale_price), int(total_profit)
        )
        mssql_cursor.commit()
