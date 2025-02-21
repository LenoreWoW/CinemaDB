-- Revised Data Warehouse Schema

-- 1. Dimension Table: DateDim
CREATE TABLE DateDim (
    DateKey       INT PRIMARY KEY,
    FullDate      DATE NOT NULL,
    Day           INT NOT NULL,
    Month         INT NOT NULL,
    MonthName     VARCHAR(20),
    Quarter       INT,
    Year          INT NOT NULL,
    WeekNumber    INT,         -- Added to support week-based queries
    DayOfWeek     VARCHAR(20)
);
CREATE INDEX idx_datedim_fulldate ON DateDim (FullDate);

-- 2. Dimension Table: MovieDim
CREATE TABLE MovieDim (
    MovieKey      SERIAL PRIMARY KEY,
    Title         VARCHAR(200),
    Genre         VARCHAR(50),
    Language      VARCHAR(50),
    ReleaseYear   INT,
    Country       VARCHAR(50),
    DirectorName  VARCHAR(200)
);
CREATE INDEX idx_moviedim_title ON MovieDim (Title);

-- 2a. Dimension Table: MovieCastDim (to record actors in a movie)
CREATE TABLE MovieCastDim (
    MovieCastID SERIAL PRIMARY KEY,
    MovieKey    INT NOT NULL,
    ActorName   VARCHAR(200) NOT NULL,
    CONSTRAINT fk_moviecast_movie FOREIGN KEY (MovieKey) REFERENCES MovieDim(MovieKey)
);

-- 3. Dimension Table: CinemaDim
CREATE TABLE CinemaDim (
    CinemaKey     SERIAL PRIMARY KEY,
    CinemaName    VARCHAR(100),
    Address       VARCHAR(200),
    City          VARCHAR(100),
    State         VARCHAR(100),  -- Added to support queries by state
    Country       VARCHAR(100),
    HallNumber    INT,
    HallCapacity  INT
);
CREATE INDEX idx_cinemadim_city ON CinemaDim (City);

-- 4. Dimension Table: CustomerDim
CREATE TABLE CustomerDim (
    CustomerKey          SERIAL PRIMARY KEY,
    Gender               VARCHAR(10),
    AgeGroup             VARCHAR(20),  -- May be used for reporting
    City                 VARCHAR(100),
    OtherCustomerAttrib  VARCHAR(50),
    BirthDate            DATE         -- Added to compute age at purchase
);
CREATE INDEX idx_customerdim_age_gender ON CustomerDim (AgeGroup, Gender);

-- 5. Dimension Table: PromotionDim
CREATE TABLE PromotionDim (
    PromotionKey     SERIAL PRIMARY KEY,
    Description      VARCHAR(200),
    DiscountAmt      NUMERIC(5,2),
    StartDate        DATE,
    EndDate          DATE,
    ActiveFlag       CHAR(1)
);
CREATE INDEX idx_promotiondim_activeflag ON PromotionDim (ActiveFlag);

-- 6. Dimension Table: PaymentDim
CREATE TABLE PaymentDim (
    PaymentKey     SERIAL PRIMARY KEY,
    PaymentMethod  VARCHAR(50),
    BrowserName    VARCHAR(50),  -- If not null, indicates an online transaction
    DeviceSystem   VARCHAR(50)
);

-- 7. Fact Table: FactTicketSales
CREATE TABLE FactTicketSales (
    FactTicketSalesID SERIAL PRIMARY KEY,
    DateKey          INT NOT NULL,
    MovieKey         INT NOT NULL,
    CinemaKey        INT NOT NULL,
    CustomerKey      INT NOT NULL,
    PromotionKey     INT,
    PaymentKey       INT,
    TicketQuantity   INT NOT NULL DEFAULT 1,
    TicketPrice      NUMERIC(10,2),
    SeatRow          VARCHAR(5),
    SeatNumber       VARCHAR(5),
    DiscountAmount   NUMERIC(6,2),
    ScreeningTime    TIME,  -- Added to determine time-of-day for showings

    -- Foreign Keys
    CONSTRAINT fk_fact_date FOREIGN KEY (DateKey) REFERENCES DateDim(DateKey) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_movie FOREIGN KEY (MovieKey) REFERENCES MovieDim(MovieKey) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_cinema FOREIGN KEY (CinemaKey) REFERENCES CinemaDim(CinemaKey) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_customer FOREIGN KEY (CustomerKey) REFERENCES CustomerDim(CustomerKey) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_promotion FOREIGN KEY (PromotionKey) REFERENCES PromotionDim(PromotionKey) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_payment FOREIGN KEY (PaymentKey) REFERENCES PaymentDim(PaymentKey) ON DELETE RESTRICT
);
CREATE INDEX idx_fact_datekey ON FactTicketSales (DateKey);
CREATE INDEX idx_fact_moviekey ON FactTicketSales (MovieKey);
CREATE INDEX idx_fact_cinemakey ON FactTicketSales (CinemaKey);
CREATE INDEX idx_fact_customerkey ON FactTicketSales (CustomerKey);
CREATE INDEX idx_fact_promotionkey ON FactTicketSales (PromotionKey);
CREATE INDEX idx_fact_paymentkey ON FactTicketSales (PaymentKey);
