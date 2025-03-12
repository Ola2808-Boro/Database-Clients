import logging
import os

import mysql
import mysql.connector
import mysql.connector as connector
from dotenv import load_dotenv
from mysql.connector import MySQLConnection
from mysql.connector.pooling import MySQLConnectionPool

load_dotenv()

logging.basicConfig(
    filename="logs.info",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def connect_to_mysql_pooling():
    try:
        db_config = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": "localhost",
        }
        cnxpooling = MySQLConnectionPool(pool_name="pool_a", pool_size=3, **db_config)
        logging.info(f"Connected to the MySQL")
        return cnxpooling
    except mysql.connector.Error as e:
        logging.info(f"Failed to connect to MySQL")
        return None


def connect_to_mysql():
    try:
        connection = connector.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host="localhost",
        )
        logging.info(f"Connected to the MySQL")
        return connection

    except mysql.connector.Error as e:
        logging.info(f"Failed to connect to MySQL")
        return None


def create_db(connection: MySQLConnection):
    try:
        create_table = """
            CREATE DATABASE IF NOT EXISTS little_lemon_db
        """
        cursor = connection.cursor()
        cursor.execute(create_table)
    except mysql.connector.Error as e:
        logging.error(f"Failed to connect to database: {e}")
    finally:
        cursor.close()


def use_db(connection: MySQLConnection):
    try:
        use_db = """
            USE  little_lemon_db
        """
        cursor = connection.cursor()
        cursor.execute(use_db)
        logging.info("Using database little_lemon_db")
    except mysql.connector.Error as e:
        logging.error(f"Failed to use database: {e}")
    finally:
        cursor.close()


def create_tables(connection: MySQLConnection):
    create_menuitem_table = """CREATE TABLE IF NOT EXISTS MenuItems(
            ItemID INT AUTO_INCREMENT,
            NAME VARCHAR(255),
            Type VARCHAR(255),
            PRICE INT,
            PRIMARY KEY(ItemID)
        )
    """
    create_menu_table = """CREATE TABLE IF NOT EXISTS Menu(
        MenuID INT,
        ItemID INT,
        Cuisine VARCHAR(255),
        PRIMARY KEY(MenuID,ItemID)
        );
    """
    create_booking_table = """CREATE TABLE IF NOT EXISTS Bookings (
        BookingID INT AUTO_INCREMENT,
        TableNo INT,
        GuestFirstName VARCHAR(100) NOT NULL,
        GuestLastName VARCHAR(100) NOT NULL,
        BookingSlot TIME NOT NULL,
        EmployeeID INT,
        PRIMARY KEY (BookingID)
        );"""
    create_orders_table = """CREATE TABLE IF NOT EXISTS Orders (
        OrderID INT,
        TableNo INT,
        MenuID INT,
        BookingID INT,
        BillAmount INT,
        Quantity INT,
        PRIMARY KEY (OrderID,TableNo)
        );"""
    create_employees_table = """CREATE TABLE IF NOT EXISTS Employees (
        EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
        Name VARCHAR (255),
        Role VARCHAR (100),
        Address VARCHAR (255),
        Contact_Number INT,
        Email VARCHAR (255),
        Annual_Salary VARCHAR (100)
        );"""

    cursor = connection.cursor()
    cursor.execute(create_menuitem_table)
    cursor.execute(create_menu_table)
    cursor.execute(create_booking_table)
    cursor.execute(create_orders_table)
    cursor.execute(create_employees_table)
    logging.info(f"End creating database")


def insert_data(connection: MySQLConnection):
    insert_menuitems = """INSERT IGNORE INTO MenuItems (ItemID, Name, Type, Price)
        VALUES
        (1, 'Olives','Starters',5),
        (2, 'Flatbread','Starters', 5),
        (3, 'Minestrone', 'Starters', 8),
        (4, 'Tomato bread','Starters', 8),
        (5, 'Falafel', 'Starters', 7),
        (6, 'Hummus', 'Starters', 5),
        (7, 'Greek salad', 'Main Courses', 15),
        (8, 'Bean soup', 'Main Courses', 12),
        (9, 'Pizza', 'Main Courses', 15),
        (10, 'Greek yoghurt','Desserts', 7),
        (11, 'Ice cream', 'Desserts', 6),
        (12, 'Cheesecake', 'Desserts', 4),
        (13, 'Athens White wine', 'Drinks', 25),
        (14, 'Corfu Red Wine', 'Drinks', 30),
        (15, 'Turkish Coffee', 'Drinks', 10),
        (16, 'Turkish Coffee', 'Drinks', 10),
        (17, 'Kabasa', 'Main Courses', 17);
        """

    insert_menu = """INSERT IGNORE INTO Menu (MenuID,ItemID,Cuisine)
        VALUES
        (1, 1, 'Greek'),
        (1, 7, 'Greek'),
        (1, 10, 'Greek'),
        (1, 13, 'Greek'),
        (2, 3, 'Italian'),
        (2, 9, 'Italian'),
        (2, 12, 'Italian'),
        (2, 15, 'Italian'),
        (3, 5, 'Turkish'),
        (3, 17, 'Turkish'),
        (3, 11, 'Turkish'),
        (3, 16, 'Turkish');
        """

    insert_bookings = """INSERT IGNORE INTO Bookings (BookingID, TableNo, GuestFirstName, 
        GuestLastName, BookingSlot, EmployeeID)
        VALUES
        (1, 12, 'Anna','Iversen','19:00:00',1),
        (2, 12, 'Joakim', 'Iversen', '19:00:00', 1),
        (3, 19, 'Vanessa', 'McCarthy', '15:00:00', 3),
        (4, 15, 'Marcos', 'Romero', '17:30:00', 4),
        (5, 5, 'Hiroki', 'Yamane', '18:30:00', 2),
        (6, 8, 'Diana', 'Pinto', '20:00:00', 5);
        """

    insert_orders = """INSERT IGNORE INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
        VALUES
        (1, 12, 1, 1, 2, 86),
        (2, 19, 2, 2, 1, 37),
        (3, 15, 2, 3, 1, 37),
        (4, 5, 3, 4, 1, 40),
        (5, 8, 1, 5, 1, 43);
        """

    insert_employees = """INSERT IGNORE INTO employees (EmployeeID, Name, Role, Address, Contact_Number, Email, Annual_Salary)
        VALUES
        (01,'Mario Gollini','Manager','724, Parsley Lane, Old Town, Chicago, IL',351258074,'Mario.g@littlelemon.com','$70,000'),
        (02,'Adrian Gollini','Assistant Manager','334, Dill Square, Lincoln Park, Chicago, IL',351474048,'Adrian.g@littlelemon.com','$65,000'),
        (03,'Giorgos Dioudis','Head Chef','879 Sage Street, West Loop, Chicago, IL',351970582,'Giorgos.d@littlelemon.com','$50,000'),
        (04,'Fatma Kaya','Assistant Chef','132  Bay Lane, Chicago, IL',351963569,'Fatma.k@littlelemon.com','$45,000'),
        (05,'Elena Salvai','Head Waiter','989 Thyme Square, EdgeWater, Chicago, IL',351074198,'Elena.s@littlelemon.com','$40,000'),
        (06,'John Millar','Receptionist','245 Dill Square, Lincoln Park, Chicago, IL',351584508,'John.m@littlelemon.com','$35,000');
        """

    cursor = connection.cursor()

    cursor.execute(insert_menuitems)
    connection.commit()

    cursor.execute(insert_menu)
    connection.commit()

    cursor.execute(insert_bookings)
    connection.commit()

    cursor.execute(insert_orders)
    connection.commit()

    cursor.execute(insert_employees)
    connection.commit()

    logging.info(f"End inserting database")


def peakhours_stored_procedure(connection: MySQLConnection | MySQLConnectionPool):
    peakhours = """
        CREATE PROCEDURE IF NOT EXISTS PeakHours()
        BEGIN
            SELECT HOUR(BookingSlot) as Hour, COUNT(*) as Num_of_bookings 
            FROM Bookings 
            GROUP BY HOUR(BookingSlot) 
            ORDER BY Num_of_bookings DESC;
        END ;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(peakhours)
        cursor.callproc("PeakHours")
        results = next(cursor.stored_results())
        dataset = results.fetchall()
        logging.info(f"Columns: {results.column_names}")
        for data in dataset:
            logging.info(f"{data}")
    except mysql.connector.Error as e:
        logging.error(f"Problem with call stored procedure: {e}")
    finally:
        cursor.close()


def gueststatus_stored_procedure(connection: MySQLConnection | MySQLConnectionPool):
    peakhours = """
        CREATE PROCEDURE IF NOT EXISTS GuestStatus()
        BEGIN
            SELECT 
                CONCAT(GuestFirstName,' ',GuestLastName) as GuestFullName,
                CASE
                    WHEN ROLE ='Manager or Assistant Manager' THEN 'Ready to pay'
                    WHEN ROLE ='Head Chef' THEN 'Ready to serve'
                    WHEN ROLE ='Assistant Chef' THEN 'Preparing Order'
                    WHEN ROLE ='Preparing Order' THEN 'Order served'
                    ELSE NULL
                END AS Status
                FROM Bookings LEFT JOIN employees ON employees.EmployeeID=Bookings.EmployeeID;
        END;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(peakhours)
        cursor.callproc("GuestStatus")
        results = next(cursor.stored_results())
        dataset = results.fetchall()
        logging.info(f"Columns: {results.column_names}")
        for data in dataset:
            logging.info(f"{data}")
    except mysql.connector.Error as e:
        logging.error(f"Problem with call stored procedure: {e}")
    finally:
        cursor.close()


def main(pooling=True):
    if pooling:
        pool = connect_to_mysql_pooling()
        connection = pool.get_connection()
    else:
        connection = connect_to_mysql()

    if connection:
        create_db(connection=connection)
        use_db(connection=connection)
        create_tables(connection=connection)
        insert_data(connection=connection)
        peakhours_stored_procedure(connection=connection)
        gueststatus_stored_procedure(connection=connection)
        connection.close()


main()
