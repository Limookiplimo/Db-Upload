from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from sqlalchemy import text
import csv
import os

# Load environment variables from the .env file
load_dotenv()

# Database configuration setup using environment variables
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": os.getenv("DB_PORT")
}

# URL encoding for username and password to handle special characters
encoded_username = quote_plus(db_config["user"])
encoded_password = quote_plus(db_config["password"])

# Creating the SQLAlchemy database URL
SQLALCHEMY_DATABASE_URL = f'mysql+mysqlconnector://{encoded_username}:{encoded_password}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def db_connection():
    """
    Provides a session to interact with the database.
    Ensures that the connection is properly closed after usage.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_table(db: Session):
    """
    Creates the 'tenants' table if it doesn't already exist.
    """
    create_table_query = text("""
            create table if not exists tenants (
                shop_id int primary key,
                shop_name VARCHAR(100),
                user_name VARCHAR(100),
                user_email VARCHAR(100),
                user_phone VARCHAR(100));
            """)
    
    # Logging table creation
    print("Executing table creation query...")
    db.execute(create_table_query)
    db.commit()
    print("Table creation committed.")


def insert_data(db: Session, data):
    """
    Inserts or updates data in the 'tenants' table.
    If the primary key (shop_id) already exists, it updates the record.
    """
    insert_query = text("""
        insert into tenants (shop_id, shop_name, user_name, user_email, user_phone)
        values (:shop_id, :shop_name, :user_name, :user_email, :user_phone)
        on duplicate key update
        shop_name=values(shop_name),
        user_name=values(user_name),
        user_email=values(user_email),
        user_phone=values(user_phone);
    """)
    
    # Logging insertion
    print(f"Inserting/updating record for shop_id: {data['shop_id']}")
    db.execute(insert_query, data)
    db.commit()
    print(f"Data for shop_id {data['shop_id']} committed.")


def read_csv_file(file_path):
    """
    Reads a CSV file and returns a list of dictionaries where each dictionary represents a row.
    """
    data = []
    print(f"Reading data from CSV file: {file_path}")
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            data.append(row)
    print(f"Finished reading CSV file: {file_path}")
    return data


def main():
    # Establishing database connection
    db = next(db_connection())
    
    # Create the table if it doesn't exist
    print(f"Creating tenants table")
    create_table(db)
    print(f"Tenants table created successfully")

    # Define the path to the CSV file
    csv_file_path = 'some_users.csv'
    
    # Read the CSV data
    csv_data = read_csv_file(csv_file_path)

    # Insert the CSV data into the database
    print(f"Inserting data into tenants")
    for row in csv_data:
        insert_data(db, row)

    print(f"Data from {csv_file_path} has been successfully inserted into the database.")


if __name__ == "__main__":
    # Run the main process
    main()
