CREATE TABLE "monthlysales"."f_order" (
    order_id VARCHAR(20),
    order_date DATE,
    ship_date DATE,
    shipmode VARCHAR(50),
    customer_id VARCHAR(20),
    postal_code INTEGER,
    product_id VARCHAR(30),
    sales NUMERIC(10, 2),
    quantity INTEGER,
    discount NUMERIC(3, 2),
    profit NUMERIC(10, 4)
);
