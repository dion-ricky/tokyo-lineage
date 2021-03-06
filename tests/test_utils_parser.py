import unittest

from tokyo_lineage.utils.parser import Parser


class TestUtilsParser(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.sql1 = """INSERT INTO sums (value)
            SELECT SUM(c.value) FROM counts AS c;
        """

        self.sql2 = """INSERT INTO movie_actor (a.actor_id, a.actor_name, m.movie_id, m.movie_name)
            SELECT actor_id, actor_name FROM actors AS a
            LEFT JOIN movies m
            ON m.actor_id = a.actor_id;
        """

        self.sql3 = """
        SELECT
            actor_id,
            first_name,
            last_name,
            last_update
        FROM
            actor
        """

        self.sql4 = """
        SELECT
            rental_id,
            rental_date,
            inventory_id,
            customer_id,
            COALESCE(return_date, '1970-01-01 07:00:00.000'::timestamp) AS return_date,
            staff_id,
            last_update
        FROM
            rental
        """

        self.sql5 = """
        WITH
        staff_alberta AS (
        SELECT
            staff_id,
            first_name,
            last_name,
            email,
            username,
            a.phone AS phone
        FROM
            `dionricky-personal.alberta.staff` s
        LEFT JOIN
            `dionricky-personal.alberta.address` a
        ON
            s.address_id = a.address_id ),
        staff_queensland AS (
        SELECT
            staff_id,
            first_name,
            last_name,
            email,
            username,
            a.phone AS phone
        FROM
            `dionricky-personal.queensland.staff` s
        LEFT JOIN
            `dionricky-personal.queensland.address` a
        ON
            s.address_id = a.address_id )
        SELECT
        *
        FROM
        staff_alberta
        UNION DISTINCT
        SELECT
        *
        FROM
        staff_queensland;
        """

        self.sql6 = """
        WITH
  customer_alberta AS (
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    a.address,
    a.address2,
    a.district,
    ct.city,
    ctr.country,
    a.postal_code,
    a.phone
  FROM
    `dionricky-personal.alberta.customer` c
  LEFT JOIN
    `dionricky-personal.alberta.address` a
  ON
    c.address_id = a.address_id
  LEFT JOIN
    `dionricky-personal.alberta.city` ct
  ON
    a.city_id = ct.city_id
  LEFT JOIN
    `dionricky-personal.alberta.country` ctr
  ON
    ct.country_id = ctr.country_id ),
  customer_queensland AS (
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    a.address,
    a.address2,
    a.district,
    ct.city,
    ctr.country,
    a.postal_code,
    a.phone
  FROM
    `dionricky-personal.queensland.customer` c
  LEFT JOIN
    `dionricky-personal.queensland.address` a
  ON
    c.address_id = a.address_id
  LEFT JOIN
    `dionricky-personal.queensland.city` ct
  ON
    a.city_id = ct.city_id
  LEFT JOIN
    `dionricky-personal.queensland.country` ctr
  ON
    ct.country_id = ctr.country_id ),
  merged AS (
  SELECT
    *
  FROM
    customer_alberta
  UNION DISTINCT
  SELECT
    *
  FROM
    customer_queensland ),
  mark_duplicate AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY TRUE) AS dup_mark
  FROM
    merged ),
  filtered AS (
  SELECT
    *
  FROM
    mark_duplicate
  WHERE
    dup_mark = 1 )
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  address,
  address2,
  district,
  city,
  country,
  postal_code,
  phone
FROM
  filtered;
        """

        self.sql7 = """
        SELECT
  FORMAT_DATE('%F', d) AS id,
  d AS full_date,
  EXTRACT(YEAR
  FROM
    d) AS year,
  EXTRACT(MONTH
  FROM
    d) AS month,
  FORMAT_DATE('%B', d) AS month_name,
  FORMAT_DATE('%w', d) AS week_day,
  FORMAT_DATE('%A', d) AS day_name
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2040-01-01', INTERVAL 1 DAY)) AS d );
        """
    
    def test_single_input_table(self):
        parser = Parser(self.sql1)

        parsed_tables = parser.tables
        self.assertListEqual(parsed_tables, ['counts'])

        # sql3
        parser = Parser(self.sql3)
        
        parsed_tables = parser.tables
        self.assertListEqual(parsed_tables, ['actor'])
    
    def test_two_input_tables(self):
        parser = Parser(self.sql2)

        parsed_tables = sorted(parser.tables)
        compare_to = sorted(['actors', 'movies'])

        self.assertListEqual(parsed_tables, compare_to)
    
    def test_different_schema(self):
        sql_different_schema = """INSERT INTO movie_actor (a.actor_id, a.actor_name, m.movie_id, m.movie_name)
            SELECT actor_id, actor_name FROM actors AS a
            LEFT JOIN imdb.movies m
            ON m.actor_id = a.actor_id;
        """

        parser = Parser(sql_different_schema)

        parsed_tables = sorted(parser.tables)
        compare_to = sorted(['actors', 'imdb.movies'])

        self.assertListEqual(parsed_tables, compare_to)

    def test_fn_call(self):
        parser = Parser(self.sql4)
        
        parsed_tables = parser.tables
        self.assertListEqual(parsed_tables, ['rental'])
    
    def test_cte(self):
        parser = Parser(self.sql5)

        parsed_tables = sorted(parser.tables)
        compare_to = sorted([
            'dionricky-personal.alberta.staff',
            'dionricky-personal.alberta.address',
            'dionricky-personal.queensland.staff',
            'dionricky-personal.queensland.address'
        ])

        self.assertListEqual(parsed_tables, compare_to)

        parser = Parser(self.sql6)

        parsed_tables = sorted(parser.tables)
        compare_to = sorted([
            'dionricky-personal.alberta.customer',
            'dionricky-personal.alberta.address',
            'dionricky-personal.alberta.city',
            'dionricky-personal.alberta.country',
            'dionricky-personal.queensland.customer',
            'dionricky-personal.queensland.address',
            'dionricky-personal.queensland.city',
            'dionricky-personal.queensland.country'
        ])

        self.assertListEqual(parsed_tables, compare_to)
    
    def test_select_from_fn(self):
        parser = Parser(self.sql7)

        parsed_tables = parser.tables
        compare_to = []

        self.assertListEqual(parsed_tables, compare_to)
    
    def test_multiple_join_to_one_table(self):
        sql = """-- ADDED JOIN WITH date_dimension
WITH
  rental_alberta AS (
  SELECT
    i.film_id,
    r.customer_id,
    r.staff_id,
    i.store_id,
    d_rental.id AS rental_date,
    d_return.id AS return_date,
    f.rental_rate,
    rp.amount_paid,
    CASE
      WHEN return_date != '1970-01-01 07:00:00.0' THEN DATE_DIFF(DATE(return_date), DATE(rental_date), DAY)
    ELSE
    NULL
  END
    AS rent_duration,
    1 AS src
  FROM
    `dionricky-personal.alberta.rental` r
  LEFT JOIN
    `dionricky-personal.alberta.inventory` i
  ON
    r.inventory_id = i.inventory_id
  LEFT JOIN
    `dionricky-personal.alberta.film` f
  ON
    i.film_id = f.film_id
  LEFT JOIN (
    SELECT
      r.rental_id,
      SUM(p.amount) AS amount_paid
    FROM
      `dionricky-personal.alberta.rental` r
    LEFT JOIN
      `dionricky-personal.alberta.payment` p
    ON
      r.rental_id = p.rental_id
    GROUP BY
      r.rental_id ) rp
  ON
    r.rental_id = rp.rental_id
  LEFT JOIN
    `dionricky-personal.warehouse.date_dimension` d_rental
  ON
    DATE(rental_date) = d_rental.full_date
  LEFT JOIN
    `dionricky-personal.warehouse.date_dimension` d_return
  ON
    DATE(COALESCE(return_date,
        '1970-01-01 07:00:00.0')) = d_return.full_date ),
  rental_queensland AS (
  SELECT
    i.film_id,
    r.customer_id,
    r.staff_id,
    i.store_id,
    d_rental.id AS rental_date,
    d_return.id AS return_date,
    f.rental_rate,
    rp.amount_paid,
    CASE
      WHEN return_date != '1970-01-01 07:00:00.0' THEN DATE_DIFF(DATE(return_date), DATE(rental_date), DAY)
    ELSE
    NULL
  END
    AS rent_duration,
    2 AS src
  FROM
    `dionricky-personal.queensland.rental` r
  LEFT JOIN
    `dionricky-personal.queensland.inventory` i
  ON
    r.inventory_id = i.inventory_id
  LEFT JOIN
    `dionricky-personal.queensland.film` f
  ON
    i.film_id = f.film_id
  LEFT JOIN (
    SELECT
      r.rental_id,
      SUM(p.amount) AS amount_paid
    FROM
      `dionricky-personal.queensland.rental` r
    LEFT JOIN
      `dionricky-personal.queensland.payment` p
    ON
      r.rental_id = p.rental_id
    GROUP BY
      r.rental_id ) rp
  ON
    r.rental_id = rp.rental_id
  LEFT JOIN
    `dionricky-personal.warehouse.date_dimension` d_rental
  ON
    DATE(rental_date) = d_rental.full_date
  LEFT JOIN
    `dionricky-personal.warehouse.date_dimension` d_return
  ON
    DATE(COALESCE(return_date,
        '1970-01-01 07:00:00.0')) = d_return.full_date ),
  rental_mongo AS (
  SELECT
    r.filmId AS film_id,
    c._id AS customer_id,
    r.staffId AS staff_id,
    NULL AS store_id,
    d_rental.id AS rental_date,
    d_return.id AS return_date,
    NULL AS rental_rate,
    p.Amount AS amount_paid,
    CASE
      WHEN r.Return_Date != '1970-01-01 07:00:00.0' THEN DATE_DIFF(DATE(r.Return_Date), DATE(r.Rental_Date), DAY)
    ELSE
    NULL
  END
    AS rent_duration,
    3 AS src
  FROM
    `dionricky-personal.mongo.customers` c,
    UNNEST(Rentals) AS r,
    UNNEST(r.Payments) AS p
  LEFT JOIN
    `dionricky-personal.warehouse.date_dimension` d_rental
  ON
    DATE(r.Rental_Date) = d_rental.full_date
  LEFT JOIN
    `dionricky-personal.warehouse.date_dimension` d_return
  ON
    DATE(COALESCE(r.Return_Date,
        '1970-01-01 07:00:00.0')) = d_return.full_date ),
  merged AS (
  SELECT
    *
  FROM
    rental_alberta
  UNION DISTINCT
  SELECT
    *
  FROM
    rental_queensland
  UNION DISTINCT
  SELECT
    *
  FROM
    rental_mongo ),
  mark_duplicate AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY film_id, customer_id ORDER BY src) AS dup_mark
  FROM
    merged ),
  filtered AS (
  SELECT
    *
  FROM
    mark_duplicate
  WHERE
    dup_mark = 1 )
SELECT
  film_id,
  customer_id,
  staff_id,
  store_id,
  rental_date,
  return_date,
  rental_rate,
  amount_paid,
  rent_duration
FROM
  filtered;"""

        parser = Parser(sql)

        parsed_tables = parser.tables
        compare_to = sorted([
            'dionricky-personal.alberta.rental',
            'dionricky-personal.alberta.inventory',
            'dionricky-personal.alberta.film',
            'dionricky-personal.alberta.payment',
            'dionricky-personal.queensland.rental',
            'dionricky-personal.queensland.inventory',
            'dionricky-personal.queensland.film',
            'dionricky-personal.queensland.payment',
            'dionricky-personal.mongo.customers',
            'dionricky-personal.warehouse.date_dimension'
        ])

        self.assertListEqual(parsed_tables, compare_to)