import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Database connection 
DB_PARAMS = {
    "dbname": "mydatabase",
    "user": "admin",
    "password": "admin123",
    "host": "127.0.0.1",
    "gssencmode": "disable"
}

def get_connection():
    return psycopg2.connect(**DB_PARAMS)

def fetch_keys(cur, query):
    cur.execute(query)
    return [row[0] for row in cur.fetchall()]

#Dim Population  

def populate_date_dim(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM DateDim")
    count = cur.fetchone()[0]
    if count > 0:
        print("DateDim already has", count, "rows.")
        cur.close()
        return
    start_date = datetime(2014, 1, 1)
    end_date = datetime(2024, 12, 31)
    delta = end_date - start_date
    insert_query = """
        INSERT INTO DateDim (DateKey, FullDate, Day, Month, MonthName, Quarter, Year, WeekNumber, DayOfWeek)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows_inserted = 0
    for i in range(delta.days + 1):
        current_date = start_date + timedelta(days=i)
        date_key = int(current_date.strftime("%Y%m%d"))
        day = current_date.day
        month = current_date.month
        month_name = current_date.strftime("%B")
        quarter = (month - 1) // 3 + 1
        year = current_date.year
        week_number = int(current_date.strftime("%U"))  
        day_of_week = current_date.strftime("%A")
        cur.execute(insert_query, (date_key, current_date.date(), day, month, month_name, quarter, year, week_number, day_of_week))
        rows_inserted += 1
        if rows_inserted % 100 == 0:
            conn.commit()
    conn.commit()
    cur.close()
    print(f"Inserted {rows_inserted} rows into DateDim.")

def populate_movie_dim(conn, num_movies=100):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM MovieDim")
    count = cur.fetchone()[0]
    if count > 0:
        print("MovieDim already has", count, "rows.")
        cur.close()
        return
    genres = ['Action', 'Adventure', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 'Romance']
    languages = ['English', 'Spanish', 'French', 'German', 'Chinese']
    insert_query = """
        INSERT INTO MovieDim (Title, Genre, Language, ReleaseYear, Country, DirectorName)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for _ in range(num_movies):
        title = fake.sentence(nb_words=3).rstrip('.')
        genre = random.choice(genres)
        language = random.choice(languages)
        release_year = random.randint(2000, 2024)
        country = fake.country()
        director_name = fake.name()
        cur.execute(insert_query, (title, genre, language, release_year, country, director_name))
    conn.commit()
    cur.close()
    print(f"Inserted {num_movies} rows into MovieDim.")

def populate_cinema_dim(conn, num_cinemas=50):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM CinemaDim")
    count = cur.fetchone()[0]
    if count > 0:
        print("CinemaDim already has", count, "rows.")
        cur.close()
        return
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ']
    insert_query = """
        INSERT INTO CinemaDim (CinemaName, Address, City, State, Country, HallNumber, HallCapacity)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for _ in range(num_cinemas):
        cinema_name = fake.company() + " Cinema"
        address = fake.address().replace("\n", ", ")
        idx = random.randrange(len(cities))
        city = cities[idx]
        state = states[idx]
        country = "USA"
        hall_number = random.randint(1, 10)
        hall_capacity = random.randint(50, 300)
        cur.execute(insert_query, (cinema_name, address, city, state, country, hall_number, hall_capacity))
    conn.commit()
    cur.close()
    print(f"Inserted {num_cinemas} rows into CinemaDim.")

def populate_customer_dim(conn, num_customers=500):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM CustomerDim")
    count = cur.fetchone()[0]
    if count > 0:
        print("CustomerDim already has", count, "rows.")
        cur.close()
        return
    genders = ['Male', 'Female']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    insert_query = """
        INSERT INTO CustomerDim (Gender, AgeGroup, City, OtherCustomerAttrib, BirthDate)
        VALUES (%s, %s, %s, %s, %s)
    """
    for _ in range(num_customers):
        gender = random.choice(genders)
        birth_year = random.randint(1960, 2010)
        birth_date = datetime(birth_year, random.randint(1, 12), random.randint(1, 28)).date()
        age_group = "Unknown"  
        city = random.choice(cities)
        other_attrib = fake.word()
        cur.execute(insert_query, (gender, age_group, city, other_attrib, birth_date))
    conn.commit()
    cur.close()
    print(f"Inserted {num_customers} rows into CustomerDim.")

def populate_promotion_dim(conn, num_promotions=20):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM PromotionDim")
    count = cur.fetchone()[0]
    if count > 0:
        print("PromotionDim already has", count, "rows.")
        cur.close()
        return
    insert_query = """
        INSERT INTO PromotionDim (Description, DiscountAmt, StartDate, EndDate, ActiveFlag)
        VALUES (%s, %s, %s, %s, %s)
    """
    for _ in range(num_promotions):
        description = fake.catch_phrase()
        discount_amt = round(random.uniform(1, 10), 2)
        start_date = fake.date_between(start_date='-5y', end_date='today')
        end_date = start_date + timedelta(days=random.randint(30, 180))
        active_flag = random.choice(['Y', 'N'])
        cur.execute(insert_query, (description, discount_amt, start_date, end_date, active_flag))
    conn.commit()
    cur.close()
    print(f"Inserted {num_promotions} rows into PromotionDim.")

def populate_payment_dim(conn, num_payments=5):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM PaymentDim")
    count = cur.fetchone()[0]
    if count > 0:
        print("PaymentDim already has", count, "rows.")
        cur.close()
        return
    payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Online Payment', 'Mobile Payment']
    browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera', None]  
    device_systems = ['Windows', 'macOS', 'Linux', 'Android', 'iOS']
    insert_query = """
        INSERT INTO PaymentDim (PaymentMethod, BrowserName, DeviceSystem)
        VALUES (%s, %s, %s)
    """
    for _ in range(num_payments):
        payment_method = random.choice(payment_methods)
        browser = random.choice(browsers)
        device = random.choice(device_systems)
        cur.execute(insert_query, (payment_method, browser, device))
    conn.commit()
    cur.close()
    print(f"Inserted {num_payments} rows into PaymentDim.")

#Fact Table Population 

def populate_fact_table(conn, num_rows=1000000):
    cur = conn.cursor()
    
    # Fetch keys
    date_keys = fetch_keys(cur, "SELECT DateKey FROM DateDim")
    movie_keys = fetch_keys(cur, "SELECT MovieKey FROM MovieDim")
    cinema_keys = fetch_keys(cur, "SELECT CinemaKey FROM CinemaDim")
    customer_keys = fetch_keys(cur, "SELECT CustomerKey FROM CustomerDim")
    promotion_keys = fetch_keys(cur, "SELECT PromotionKey FROM PromotionDim")
    payment_keys = fetch_keys(cur, "SELECT PaymentKey FROM PaymentDim")

    # Check Dim tables for data
    if not date_keys:
        raise Exception("DateDim is empty!")
    if not movie_keys:
        raise Exception("MovieDim is empty!")
    if not cinema_keys:
        raise Exception("CinemaDim is empty!")
    if not customer_keys:
        raise Exception("CustomerDim is empty!")
    if not payment_keys:
        raise Exception("PaymentDim is empty!")
    
    insert_query = """
        INSERT INTO FactTicketSales 
        (DateKey, MovieKey, CinemaKey, CustomerKey, PromotionKey, PaymentKey,
         TicketQuantity, TicketPrice, SeatRow, SeatNumber, DiscountAmount, ScreeningTime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Define screening time
    possible_hours = list(range(10, 24)) + [0]
    
    for i in range(num_rows):
        date_key = random.choice(date_keys)
        movie_key = random.choice(movie_keys)
        cinema_key = random.choice(cinema_keys)
        customer_key = random.choice(customer_keys)
        promotion_key = random.choice(promotion_keys) if promotion_keys and random.random() < 0.3 else None
        payment_key = random.choice(payment_keys)
        ticket_quantity = random.randint(1, 5)
        ticket_price = round(random.uniform(5, 15), 2)
        seat_row = fake.random_letter().upper()
        seat_number = str(random.randint(1, 20))
        discount_amount = round(random.uniform(0, 5), 2)
        hour = random.choice(possible_hours)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        screening_time = f"{hour:02d}:{minute:02d}:{second:02d}"
        
        cur.execute(insert_query, (date_key, movie_key, cinema_key, customer_key, promotion_key, payment_key,
                                     ticket_quantity, ticket_price, seat_row, seat_number, discount_amount, screening_time))
        
        if (i + 1) % 10000 == 0:
            conn.commit()
            print(f"{i+1} fact rows inserted")
    conn.commit()
    cur.close()
    print("Finished inserting fact table rows.")

def main():
    conn = None
    try:
        conn = get_connection()
        print("Connected to database.")
        
        # Populate 
        populate_date_dim(conn)
        populate_movie_dim(conn)
        populate_cinema_dim(conn)
        populate_customer_dim(conn)
        populate_promotion_dim(conn)
        populate_payment_dim(conn)
        
        # Populate
        populate_fact_table(conn, num_rows=1000000)
        
    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()
