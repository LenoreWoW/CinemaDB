-- Pivot Table Part 2

/* Query 1: Produce a pivot table for total sales, with two dimensions, namely,
   (i) the years of the transactions, and (ii) the genders of the customers. */
SELECT
  d.Year,
  SUM(CASE WHEN c.Gender = 'Male' THEN f.TicketQuantity * f.TicketPrice ELSE 0 END) AS MaleSales,
  SUM(CASE WHEN c.Gender = 'Female' THEN f.TicketQuantity * f.TicketPrice ELSE 0 END) AS FemaleSales
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
GROUP BY d.Year
ORDER BY d.Year;

/* Query 2: Produce a pivot table for total sales, with two dimensions, namely,
   (i) the months of the transactions with ROLLUP to years, and
   (ii) whether or not the transactions are made online or offline. */
SELECT
  COALESCE(CAST(d.Year AS VARCHAR), 'Total') AS Year,
  COALESCE(CAST(d.Month AS VARCHAR), 'Total') AS Month,
  CASE
    WHEN p.BrowserName IS NOT NULL THEN 'Online'
    ELSE 'Offline'
  END AS Channel,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN PaymentDim p ON f.PaymentKey = p.PaymentKey
GROUP BY ROLLUP(d.Year, d.Month, CASE WHEN p.BrowserName IS NOT NULL THEN 'Online' ELSE 'Offline' END)
ORDER BY d.Year, d.Month, Channel;

/* Query 3: Produce a pivot table for total sales, with two dimensions, namely,
   (i) the genres of the movies, and (ii) whether the movies are shown on weekdays or weekends. */
SELECT
  m.Genre,
  CASE
    WHEN d.DayOfWeek IN ('Saturday', 'Sunday') THEN 'Weekend'
    ELSE 'Weekday'
  END AS DayType,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN MovieDim m ON f.MovieKey = m.MovieKey
JOIN DateDim d ON f.DateKey = d.DateKey
GROUP BY m.Genre,
         CASE WHEN d.DayOfWeek IN ('Saturday', 'Sunday') THEN 'Weekend' ELSE 'Weekday' END
ORDER BY m.Genre, DayType;

/* Query 4: Produce a pivot table on total sales in 2018, with two dimensions, namely,
   (i) the genders of the customers, and (ii) the types of promotions (if any) associated with the transactions. */
SELECT
  c.Gender,
  COALESCE(p.Description, 'No Promotion') AS PromotionType,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
LEFT JOIN PromotionDim p ON f.PromotionKey = p.PromotionKey
WHERE d.Year = 2018
GROUP BY c.Gender, COALESCE(p.Description, 'No Promotion')
ORDER BY c.Gender, PromotionType;

/* Query 5: Produce a pivot table on total sales in 2018, with two dimensions, namely,
   (i) the genders of the customers, and (ii) the numbers of tickets bought in each transaction. */
SELECT
  c.Gender,
  f.TicketQuantity,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
WHERE d.Year = 2018
GROUP BY c.Gender, f.TicketQuantity
ORDER BY c.Gender, f.TicketQuantity;

/* Query 6: Produce a pivot table on the total number of tickets sold in 2018, with two dimensions, namely,
   (i) the genders of the customers, and (ii) whether the movie is shown in the morning, in the afternoon, or at night.
   (Morning: before 12:00, Afternoon: 12:00-16:59, Night: 17:00 and later) */
SELECT
  c.Gender,
  CASE
    WHEN f.ScreeningTime < '12:00:00' THEN 'Morning'
    WHEN f.ScreeningTime < '17:00:00' THEN 'Afternoon'
    ELSE 'Night'
  END AS TimeOfDay,
  SUM(f.TicketQuantity) AS TotalTicketsSold
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
WHERE d.Year = 2018
GROUP BY c.Gender,
         CASE
           WHEN f.ScreeningTime < '12:00:00' THEN 'Morning'
           WHEN f.ScreeningTime < '17:00:00' THEN 'Afternoon'
           ELSE 'Night'
         END
ORDER BY c.Gender, TimeOfDay;

/* Query 7: Produce a pivot table on total sales from 2015 to 2018 for movies directed by Mohamed Khan, with two dimensions, namely,
   (i) the years of transactions, and (ii) the states in which the cinemas are located. */
SELECT
  d.Year,
  cin.State,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CinemaDim cin ON f.CinemaKey = cin.CinemaKey
JOIN MovieDim m ON f.MovieKey = m.MovieKey
WHERE d.Year BETWEEN 2015 AND 2018
  AND m.DirectorName = 'Mohamed Khan'
GROUP BY d.Year, cin.State
ORDER BY d.Year, cin.State;

/* Query 8: Produce a pivot table on total sales for movies where Omar Sharif was cast in, with two dimensions, namely,
   (i) genres of the movies, and (ii) the genders of customers. */
SELECT
  m.Genre,
  c.Gender,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN MovieDim m ON f.MovieKey = m.MovieKey
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
JOIN MovieCastDim mc ON m.MovieKey = mc.MovieKey
WHERE mc.ActorName = 'Omar Sharif'
GROUP BY m.Genre, c.Gender
ORDER BY m.Genre, c.Gender;

/* Query 9: Produce a pivot table on total sales for offline transactions in 2018, with two dimensions, namely,
   (i) the states in which the cinemas are located, and (ii) whether the movie is shown in a small size, mid-size, or large size hall.
   (Small: HallCapacity < 50, Mid-size: BETWEEN 50 AND 150, Large: > 150) */
SELECT
  cin.State,
  CASE
    WHEN cin.HallCapacity < 50 THEN 'Small'
    WHEN cin.HallCapacity BETWEEN 50 AND 150 THEN 'Mid-size'
    ELSE 'Large'
  END AS HallSize,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CinemaDim cin ON f.CinemaKey = cin.CinemaKey
JOIN PaymentDim p ON f.PaymentKey = p.PaymentKey
WHERE d.Year = 2018
  AND p.BrowserName IS NULL
GROUP BY cin.State,
         CASE
           WHEN cin.HallCapacity < 50 THEN 'Small'
           WHEN cin.HallCapacity BETWEEN 50 AND 150 THEN 'Mid-size'
           ELSE 'Large'
         END
ORDER BY cin.State, HallSize;

/* Query 10: Produce a pivot table on total sales from 2015 to 2018, with two dimensions, namely,
   (i) the genders of the customers, and (ii) the ages of the customers at the time of ticket purchase,
   with ROLLUP to age groups. (Age groups: 0-10, 11-20, 21-30, 31-40, 41-50, 51+) */
SELECT
  c.Gender,
  CASE
    WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 0 AND 10 THEN '0-10'
    WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 11 AND 20 THEN '11-20'
    WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 21 AND 30 THEN '21-30'
    WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 31 AND 40 THEN '31-40'
    WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 41 AND 50 THEN '41-50'
    ELSE '51+'
  END AS AgeGroup,
  SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales
FROM FactTicketSales f
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
WHERE d.Year BETWEEN 2015 AND 2018
GROUP BY ROLLUP (
    c.Gender,
    CASE
      WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 0 AND 10 THEN '0-10'
      WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 11 AND 20 THEN '11-20'
      WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 21 AND 30 THEN '21-30'
      WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 31 AND 40 THEN '31-40'
      WHEN EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) BETWEEN 41 AND 50 THEN '41-50'
      ELSE '51+'
    END
)
ORDER BY c.Gender, AgeGroup;


--Part 3 


/* Query 11: For each city, rank the cinemas in the city in descending order of total sales in 2018. */
SELECT
   cin.City,
   cin.CinemaName,
   SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales,
   RANK() OVER (PARTITION BY cin.City ORDER BY SUM(f.TicketQuantity * f.TicketPrice) DESC) AS RankInCity
FROM FactTicketSales f
JOIN CinemaDim cin ON f.CinemaKey = cin.CinemaKey
JOIN DateDim d ON f.DateKey = d.DateKey
WHERE d.Year = 2018
GROUP BY cin.City, cin.CinemaName
ORDER BY cin.City, RankInCity;

/* Query 12: For each director, rank his/her movies in descending order of total sales for customers with ages under 40 (at the time of ticket purchases). */
SELECT
   m.DirectorName,
   m.Title,
   SUM(f.TicketQuantity * f.TicketPrice) AS TotalSales,
   RANK() OVER (PARTITION BY m.DirectorName ORDER BY SUM(f.TicketQuantity * f.TicketPrice) DESC) AS MovieRank
FROM FactTicketSales f
JOIN MovieDim m ON f.MovieKey = m.MovieKey
JOIN DateDim d ON f.DateKey = d.DateKey
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
WHERE EXTRACT(YEAR FROM age(d.FullDate, c.BirthDate)) < 40
GROUP BY m.DirectorName, m.Title
ORDER BY m.DirectorName, MovieRank;

/* Query 13: Consider the online transactions made with various browsers, for cinemas in different states. For each city, rank the browsers in descending order of the total numbers of transactions made. */
SELECT
   cin.City,
   p.BrowserName,
   COUNT(*) AS TransactionCount,
   RANK() OVER (PARTITION BY cin.City ORDER BY COUNT(*) DESC) AS BrowserRank
FROM FactTicketSales f
JOIN PaymentDim p ON f.PaymentKey = p.PaymentKey
JOIN CinemaDim cin ON f.CinemaKey = cin.CinemaKey
WHERE p.BrowserName IS NOT NULL
GROUP BY cin.City, p.BrowserName
ORDER BY cin.City, BrowserRank;

/* Query 14: Find the top 10 movies in 2018 (in terms of the total number of tickets sold) for male and female customers, respectively. */
WITH RankedMovies AS (
  SELECT
     c.Gender,
     m.Title,
     SUM(f.TicketQuantity) AS TotalTicketsSold,
     RANK() OVER (PARTITION BY c.Gender ORDER BY SUM(f.TicketQuantity) DESC) AS RankByGender
  FROM FactTicketSales f
  JOIN DateDim d ON f.DateKey = d.DateKey
  JOIN MovieDim m ON f.MovieKey = m.MovieKey
  JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
  WHERE d.Year = 2018
  GROUP BY c.Gender, m.Title
)
SELECT * FROM RankedMovies
WHERE RankByGender <= 10
ORDER BY Gender, RankByGender;

/* Query 15: For each city, find the top 5 cinemas in terms of the total number of tickets sold from 2014 to 2018. */
WITH CinemaSales AS (
  SELECT
    cin.City,
    cin.CinemaName,
    SUM(f.TicketQuantity) AS TotalTicketsSold,
    RANK() OVER (PARTITION BY cin.City ORDER BY SUM(f.TicketQuantity) DESC) AS RankInCity
  FROM FactTicketSales f
  JOIN DateDim d ON f.DateKey = d.DateKey
  JOIN CinemaDim cin ON f.CinemaKey = cin.CinemaKey
  WHERE d.Year BETWEEN 2014 AND 2018
  GROUP BY cin.City, cin.CinemaName
)
SELECT * FROM CinemaSales
WHERE RankInCity <= 5
ORDER BY City, RankInCity;

/* Query 16: Compute the 8-week moving average of total sales, for each week in 2018. */
WITH WeeklySales AS (
  SELECT
    d.WeekNumber,
    SUM(f.TicketQuantity * f.TicketPrice) AS WeekSales
  FROM FactTicketSales f
  JOIN DateDim d ON f.DateKey = d.DateKey
  WHERE d.Year = 2018
  GROUP BY d.WeekNumber
)
SELECT
  WeekNumber,
  WeekSales,
  AVG(WeekSales) OVER (ORDER BY WeekNumber ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS MovingAvg8Week
FROM WeeklySales
ORDER BY WeekNumber;

/* Query 17: Compute the largest three 4-week moving averages of total sales, among the weeks in 2018. */
WITH WeeklySales AS (
  SELECT
    d.WeekNumber,
    SUM(f.TicketQuantity * f.TicketPrice) AS WeekSales
  FROM FactTicketSales f
  JOIN DateDim d ON f.DateKey = d.DateKey
  WHERE d.Year = 2018
  GROUP BY d.WeekNumber
),
MovingAvg4 AS (
  SELECT
    WeekNumber,
    WeekSales,
    AVG(WeekSales) OVER (ORDER BY WeekNumber ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS MovingAvg4Week
  FROM WeeklySales
)
SELECT WeekNumber, MovingAvg4Week
FROM MovingAvg4
ORDER BY MovingAvg4Week DESC
LIMIT 3;

/* Query 18: For each city, compute the largest 4-week moving average of total sales from 2010 to 2018. */
WITH CityWeeklySales AS (
  SELECT
    cin.City,
    d.Year,
    d.WeekNumber,
    SUM(f.TicketQuantity * f.TicketPrice) AS WeekSales
  FROM FactTicketSales f
  JOIN DateDim d ON f.DateKey = d.DateKey
  JOIN CinemaDim cin ON f.CinemaKey = cin.CinemaKey
  WHERE d.Year BETWEEN 2010 AND 2018
  GROUP BY cin.City, d.Year, d.WeekNumber
),
CityMovingAvg AS (
  SELECT
    City,
    Year,
    WeekNumber,
    WeekSales,
    AVG(WeekSales) OVER (PARTITION BY City ORDER BY Year, WeekNumber ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS MovingAvg4Week
  FROM CityWeeklySales
)
SELECT City, MAX(MovingAvg4Week) AS MaxMovingAvg4Week
FROM CityMovingAvg
GROUP BY City
ORDER BY City;
