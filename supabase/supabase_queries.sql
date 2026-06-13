-- ============================================================
--  STAR SCHEMA (DDL) - PostgreSQL
--  Dimension Tables + Fact Table
-- ============================================================

-- Create Tables
-- ------------------------------------------------------------
-- 1. dim_hotels
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_hotels (
    hotel_id        SERIAL          PRIMARY KEY,
    city            TEXT,
    hotel_name      TEXT    NOT NULL,
    price           DECIMAL(10, 2),
    review_score    DECIMAL(4, 2),
    hotel_url       TEXT,
    description     TEXT,
    latitude        DECIMAL(10, 6),
    longitude       DECIMAL(10, 6),
    distance_km     DECIMAL(10, 2)
);


-- ------------------------------------------------------------
-- 2. dim_room_hotel
--    Linked to dim_hotels via hotel_id (FK)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_room_hotel (
    room_id             SERIAL          PRIMARY KEY,
    hotel_id            INT             REFERENCES dim_hotels(hotel_id) ON DELETE CASCADE,
    city                TEXT,
    hotel_name          TEXT,
    hotel_url           TEXT,
    number_of_guests    TEXT,
    price_5_nights      TEXT,
    your_choices        TEXT,
    room_name           TEXT,
    features            TEXT
);


-- ------------------------------------------------------------
-- 3. dim_restaurants
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_restaurants (
    restaurant_id   SERIAL          PRIMARY KEY,
    city            TEXT,
    restaurant_name TEXT,
    location        TEXT,
    cuisines        TEXT,
    url             TEXT,
    total_items     INT,
    prices_list     TEXT,
    min_price       DECIMAL(10, 2),
    max_price       DECIMAL(10, 2),
    avg_price       DECIMAL(10, 4)
);


-- ------------------------------------------------------------
-- 4. dim_places
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_places (
    place_id         SERIAL          PRIMARY KEY,
    city             TEXT,
    title            TEXT,
    rating           DECIMAL(3, 1),
    reviews          DECIMAL(3, 1),
    description      TEXT,
    tips             TEXT,
    address          TEXT,
    timings          TEXT,
    ticket_price     DECIMAL(10, 2),
    avg_ticket_price DECIMAL(10, 2)
);


-- ------------------------------------------------------------
-- 5. fact_trips
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id         SERIAL  PRIMARY KEY,
    hotel_id        INT     REFERENCES dim_hotels(hotel_id) ON DELETE SET NULL,
    room_id         INT     REFERENCES dim_room_hotel(room_id) ON DELETE SET NULL,
    restaurant_id   INT     REFERENCES dim_restaurants(restaurant_id) ON DELETE SET NULL,
    place_id        INT     REFERENCES dim_places(place_id) ON DELETE SET NULL
);





ALTER TABLE dim_hotels ALTER COLUMN hotel_name DROP NOT NULL;
ALTER TABLE dim_hotels ALTER COLUMN price       DROP NOT NULL;
ALTER TABLE dim_hotels ALTER COLUMN review_score DROP NOT NULL;
