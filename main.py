import pandas as pd
import pyodbc

# Connection parameters (modify according to your setup)
server = 'localhost'
database = 'NAMA DATABASE KALIAN'
username = 'SA'
password = 'PASSWORD DATABASE KALIAN.'
driver = '{ODBC Driver 17 for SQL Server}'

# Connect to SQL Server
conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
)
cursor = conn.cursor()

# Define table creation scripts (as before)
create_tables_script = """
-- Drop tables if they exist
IF OBJECT_ID('Employee', 'U') IS NOT NULL DROP TABLE Employee;
IF OBJECT_ID('Productivity', 'U') IS NOT NULL DROP TABLE Productivity;
IF OBJECT_ID('MarketingResearchExpenses', 'U') IS NOT NULL DROP TABLE MarketingResearchExpenses;
IF OBJECT_ID('IncomeAndProfit', 'U') IS NOT NULL DROP TABLE IncomeAndProfit;
IF OBJECT_ID('FinanceDataAndAsset', 'U') IS NOT NULL DROP TABLE FinanceDataAndAsset;
IF OBJECT_ID('Companies', 'U') IS NOT NULL DROP TABLE Companies;
IF OBJECT_ID('Subsector', 'U') IS NOT NULL DROP TABLE Subsector;

-- Create tables
CREATE TABLE Subsector (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE Companies (
    id VARCHAR(255) PRIMARY KEY,
    year INT,
    name VARCHAR(255),
    subsector_id VARCHAR(255),
    FOREIGN KEY (subsector_id) REFERENCES Subsector(id)
);

CREATE TABLE FinanceDataAndAsset (
    id VARCHAR(255) PRIMARY KEY,
    total_asset DECIMAL(20, 2),
    fixed_asset DECIMAL(20, 2),
    cost_of_goods_sold DECIMAL(20, 2),
    operating_expense DECIMAL(20, 2),
    general_administrative_expense DECIMAL(20, 2),
    FOREIGN KEY (id) REFERENCES Companies(id)
);

CREATE TABLE IncomeAndProfit (
    id VARCHAR(255) PRIMARY KEY,
    sales_revenue DECIMAL(20, 2),
    operating_profit_margin DECIMAL(20, 2),
    operating_profit_margin_ratio DECIMAL(10, 4),
    FOREIGN KEY (id) REFERENCES Companies(id)
);

CREATE TABLE MarketingResearchExpenses (
    id VARCHAR(255) PRIMARY KEY,
    advertising_expenses DECIMAL(20, 2),
    rnd_expenses DECIMAL(20, 2),
    FOREIGN KEY (id) REFERENCES Companies(id)
);

CREATE TABLE Productivity (
    id VARCHAR(255) PRIMARY KEY,
    return_on_asset DECIMAL(10, 4),
    operational_efficiency DECIMAL(10, 4),
    sales_growth DECIMAL(10, 4),
    FOREIGN KEY (id) REFERENCES Companies(id)
);

CREATE TABLE Employee (
    id VARCHAR(255) PRIMARY KEY,
    number_employee INT,
    FOREIGN KEY (id) REFERENCES Companies(id)
);
"""

# Execute table creation script
cursor.execute(create_tables_script)
conn.commit()

# Load data from the dataset
df = pd.read_excel('Manufacturing Indonesia Dataset.xlsx')


def safe_float_conversion(value):
    """Converts a value to float if possible, otherwise returns None and logs invalid cases."""
    try:
        if pd.isna(value) or value == '' or value is None:
            return None
        if isinstance(value, str):
            value = value.replace(',', '')
        return float(value)
    except (ValueError, TypeError):
        print(f"Warning: Invalid value for conversion to float: {value}")
        return None

# Track unique subsectors
unique_subsectors = {}

# Insert data into tables with unique Subsector handling
for index, row in df.iterrows():
    company_id = index  # Using numeric index directly as the ID

    # Handle unique Subsector insertion
    subsector_name = row['Subsector']
    if subsector_name not in unique_subsectors:
        subsector_id = len(unique_subsectors)  # Generate unique numeric ID for each new subsector
        unique_subsectors[subsector_name] = subsector_id
        cursor.execute("""
            INSERT INTO Subsector (id, name)
            VALUES (?, ?)
            """, subsector_id, subsector_name if pd.notna(subsector_name) else None)
    else:
        subsector_id = unique_subsectors[subsector_name]

    # Insert into Companies table
    cursor.execute("""
        INSERT INTO Companies (id, year, name, subsector_id)
        VALUES (?, ?, ?, ?)
        """, company_id, int(row['Year']) if pd.notna(row['Year']) else None, row['Firm'] if pd.notna(row['Firm']) else None, subsector_id)

    # Insert into FinanceDataAndAsset table
    total_asset = safe_float_conversion(row['Total Asset ( IDR)'])
    fixed_asset = safe_float_conversion(row['PPE (Fixed Asset) IDR'])
    cost_of_goods_sold = safe_float_conversion(row['Cost of Goods Sold (COGS) (IDR)'])
    operating_expense = safe_float_conversion(row['Operating Expense (IDR)'])
    general_administrative_expense = safe_float_conversion(row['General and Administrative Expense (IDR)'])

    cursor.execute("""
        INSERT INTO FinanceDataAndAsset (id, total_asset, fixed_asset, cost_of_goods_sold, operating_expense, general_administrative_expense)
        VALUES (?, ?, ?, ?, ?, ?)
        """, company_id, total_asset, fixed_asset, cost_of_goods_sold, operating_expense, general_administrative_expense)

    # Insert into IncomeProfit table
    sales_revenue = safe_float_conversion(row['Sales Revenues (IDR)'])
    operating_profit_margin = safe_float_conversion(row['Operating Profit Margin (IDR)'])
    operating_profit_margin_ratio = safe_float_conversion(row['Operating Profit Margin Ratio'])

    cursor.execute("""
        INSERT INTO IncomeAndProfit (id, sales_revenue, operating_profit_margin, operating_profit_margin_ratio)
        VALUES (?, ?, ?, ?)
        """, company_id, sales_revenue, operating_profit_margin, operating_profit_margin_ratio)

    # Insert into MarketingResearchExpenses table
    advertising_expenses = safe_float_conversion(row['Advertising Expense (IDR)'])
    rnd_expenses = safe_float_conversion(row['R&D Expenses (IDR)'])

    cursor.execute("""
        INSERT INTO MarketingResearchExpenses (id, advertising_expenses, rnd_expenses)
        VALUES (?, ?, ?)
        """, company_id, advertising_expenses, rnd_expenses)

    # Insert into Productivity table
    return_on_asset = safe_float_conversion(row['Return on Asset (ROA)'])
    operational_efficiency = safe_float_conversion(row['Operational Efficiency'])
    sales_growth = safe_float_conversion(row['Sales Growth'])

    cursor.execute("""
        INSERT INTO Productivity (id, return_on_asset, operational_efficiency, sales_growth)
        VALUES (?, ?, ?, ?)
        """, company_id, return_on_asset, operational_efficiency, sales_growth)

    # Insert into Employee table
    number_employee = int(row['Number of Employees']) if pd.notna(row['Number of Employees']) else None

    cursor.execute("""
        INSERT INTO Employee (id, number_employee)
        VALUES (?, ?)
        """, company_id, number_employee)

conn.commit()


# Example query to confirm data insertion
cursor.execute("SELECT * FROM Companies")
for row in cursor.fetchall():
    print(row)

# Close the connection
cursor.close()
conn.close()
