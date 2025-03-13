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

def connect_to_mysql_pooling_b():
    try:
        db_config = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": "localhost",
        }
        cnxpooling = MySQLConnectionPool(pool_name="pool_b", pool_size=2, **db_config)
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


def peakhours_stored_procedure(connection: MySQLConnection):
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

def insert_guests(pooling):
    insert_guest1=""" INSERT IGNORE INTO Bookings (TableNo, GuestFirstName, 
        GuestLastName, BookingSlot, EmployeeID)
        VALUES (8,'Anees','Java','18:00',6);
    """
    insert_guest2=""" INSERT IGNORE INTO Bookings (TableNo, GuestFirstName, 
        GuestLastName, BookingSlot, EmployeeID)
        VALUES (5,'Bald','Vin','19:00',6);
    """
    insert_guest3=""" INSERT IGNORE INTO Bookings (TableNo, GuestFirstName, 
        GuestLastName, BookingSlot, EmployeeID)
        VALUES(12,'Jay','Kon','19:30',6)
        ;
    """
    insert_queries=[insert_guest1,insert_guest2,insert_guest3]
    for i,query in enumerate(insert_queries):
        try:
            connection=pooling.get_connection()
        except mysql.connector.Error as e:
            logging.error(f"Problem with creating connection with pooling_b: {e}")
            pooling.add_connection()
            connection=pooling.get_connection()
        finally:
            logging.info(f'Adding guest: {i}')
            use_db = """
                USE  little_lemon_db
                """
            cursor = connection.cursor()
            cursor.execute(use_db)
            cursor.execute(query)
            connection.commit()
            connection.close()

def dispaly_bookings(pooling):
    connection=pooling.get_connection()
    cursor=connection.cursor(buffered=True)
    query="""
        SELECT BookingSlot,
        CONCAT(GuestFirstName, ' ', GuestLastName) AS GuestFullName,
        Name,
        Role
        FROM
        Bookings NATUTAL JOIN Employees
        ORDER BY BookingSlot ASC
        LIMIT 3
    """
    cursor.execute(query)
    results=cursor.fetchall()
    logging.info(f'Succesfully display 3 bookings')
    for i,result in enumerate(results):
        booking_slot,guest_name, name,role=result
        logging.info(f'Guest: {i} \n Booking slot: {booking_slot} \n guest name: {guest_name}\n assigned to: {name} [{role}] ')
    cursor.close()
    connection.close()
    


def create_basic_sales_report_procedure(pooling):
    basic_sales_report_procedure="""
        CREATE PROCEDURE IF NOT EXISTS Basic_Sales_Report()
        BEGIN
            SELECT SUM(BillAmount) AS Total_sales,
            AVG(BillAmount) AS Average_sale,
            MIN(BillAmount) AS Minimum_bill_paid,
            MAX(BillAmount) AS Maximum_bill_paid
            FROM Orders;
        END;
    """
    connection=pooling.get_connection()
    cursor=connection.cursor()
    cursor.execute(basic_sales_report_procedure)
    cursor.callproc('Basic_Sales_Report')
    results=next(cursor.stored_results())
    dataset=results.fetchall()
    sum_bills,avg_bills,min_bill,max_bill=dataset[0]
    logging.info(f'Total sales: {sum_bills}, avg sale: {avg_bills}, min bill paid: {min_bill}, max bill paid: {max_bill}')
    cursor.close()
    connection.close()
    
def create_report(pooling):
    query_managers="""
        SELECT Name, EmployeeID FROM employees WHERE Role='Manager';
    """
    hightest_salary="""
        SELECT Name, Role FROM employees ORDER BY Annual_Salary LIMIT 1;
    """
    number_of_guests="""
        SELECT COUNT(*) as Num_of_guests FROM Bookings WHERE BookingSlot BETWEEN '18:00' AND '20:00';
    """
    
    guest_waiting = """
        SELECT BookingID, CONCAT(GuestFirstName, ' ', GuestLastName) AS GuestFullName
        FROM Bookings 
        ORDER BY BookingSlot;
     """
    connection=pooling.get_connection()
    cursor=connection.cursor()
    cursor.execute(query_managers)
    query_managers_results=cursor.fetchall()
    logging.info(f'The name and EmployeeID of the Little Lemon manager: {query_managers_results}')
    
    cursor.execute(hightest_salary)
    hightest_salary_results=cursor.fetchall()
    logging.info(f'The name and role of the employee who receives the highest salary: {hightest_salary_results}')
    
    cursor.execute(number_of_guests)
    number_of_guests_results=cursor.fetchall()
    logging.info(f'The number of guests booked between 18:00 and 20:00: {number_of_guests_results}')
    
    cursor.execute(guest_waiting)
    guest_waiting_results=cursor.fetchall()
    logging.info(f'All guests waiting to be seated: {guest_waiting_results}')
    cursor.close()
    connection.close()

def main(pooling=True,task=3):
    if task==3:
        pooling_b=connect_to_mysql_pooling_b()
        insert_guests(pooling=pooling_b)
        create_report(pooling=pooling_b)
        create_basic_sales_report_procedure(pooling=pooling_b)
        dispaly_bookings(pooling=pooling_b)
    else:
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
