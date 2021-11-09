CREATE TABLE "store" (
  "store_id" SERIAL PRIMARY KEY,
  "store_name" varchar,
  "city_id" int,
  "store_type_id" int,
  "store_open_date" date,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "city" (
  "city_id" SERIAL PRIMARY KEY,
  "city_name" varchar,
  "country_id" int,
  "created_dt" timestamp,
  "modified_dt" timestamp
);

CREATE TABLE "country" (
  "country_id" SERIAL PRIMARY KEY,
  "country_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp
);

CREATE TABLE "store_type" (
  "store_type_id" SERIAL PRIMARY KEY,
  "store_type_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "product" (
  "product_id" SERIAL PRIMARY KEY,
  "product_name" varchar,
  "product_category_id" int,
  "product_cost" float,
  "product_price" float,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "product_category" (
  "product_category_id" SERIAL PRIMARY KEY,
  "product_category_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "inventory" (
  "inventory_id" SERIAL PRIMARY KEY,
  "store_id" int,
  "product_id" int,
  "inventory_dt" date,
  "stock_on_hand" int
);

CREATE TABLE "type_customer" (
  "type_customer_id" SERIAL PRIMARY KEY,
  "type_customer_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "gender" (
  "gender_id" SERIAL PRIMARY KEY,
  "gender_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "marital_status" (
  "marital_status_id" SERIAL PRIMARY KEY,
  "marital_status_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "customer" (
  "customer_id" SERIAL PRIMARY KEY,
  "customer_name" varchar,
  "type_customer_id" int,
  "gender_id" int,
  "marital_status_id" int,
  "age" int,
  "spending_score" int,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "ratings" (
  "rating_id" SERIAL PRIMARY KEY,
  "customer_id" int,
  "product_id" int,
  "rating" int,
  "rating_dt" timestamp
);

CREATE TABLE "sales_status" (
  "sales_status_id" SERIAL PRIMARY KEY,
  "sales_status_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "sales_purchase" (
  "sales_purchase_id" SERIAL PRIMARY KEY,
  "sales_purchase_name" varchar,
  "created_dt" timestamp,
  "modified_dt" timestamp,
  "effective_dt" timestamp
);

CREATE TABLE "sales" (
  "sales_id" SERIAL PRIMARY KEY,
  "sales_dt" date,
  "store_id" int,
  "product_id" int,
  "sales_status_id" int,
  "delivered_dt" timestamp,
  "sales_purchase_id" int,
  "customer_id" int,
  "units" int,
  "discount" float
);

ALTER TABLE "store" ADD FOREIGN KEY ("store_type_id") REFERENCES "store_type" ("store_type_id");

ALTER TABLE "store" ADD FOREIGN KEY ("city_id") REFERENCES "city" ("city_id");

ALTER TABLE "city" ADD FOREIGN KEY ("country_id") REFERENCES "country" ("country_id");

ALTER TABLE "product" ADD FOREIGN KEY ("product_category_id") REFERENCES "product_category" ("product_category_id");

ALTER TABLE "inventory" ADD FOREIGN KEY ("product_id") REFERENCES "product" ("product_id");

ALTER TABLE "inventory" ADD FOREIGN KEY ("store_id") REFERENCES "store" ("store_id");

ALTER TABLE "sales" ADD FOREIGN KEY ("product_id") REFERENCES "product" ("product_id");

ALTER TABLE "sales" ADD FOREIGN KEY ("store_id") REFERENCES "store" ("store_id");

ALTER TABLE "sales" ADD FOREIGN KEY ("sales_status_id") REFERENCES "sales_status" ("sales_status_id");

ALTER TABLE "sales" ADD FOREIGN KEY ("sales_purchase_id") REFERENCES "sales_purchase" ("sales_purchase_id");

ALTER TABLE "sales" ADD FOREIGN KEY ("customer_id") REFERENCES "customer" ("customer_id");

ALTER TABLE "customer" ADD FOREIGN KEY ("type_customer_id") REFERENCES "type_customer" ("type_customer_id");

ALTER TABLE "customer" ADD FOREIGN KEY ("gender_id") REFERENCES "gender" ("gender_id");

ALTER TABLE "customer" ADD FOREIGN KEY ("marital_status_id") REFERENCES "marital_status" ("marital_status_id");

ALTER TABLE "ratings" ADD FOREIGN KEY ("customer_id") REFERENCES "customer" ("customer_id");

ALTER TABLE "ratings" ADD FOREIGN KEY ("product_id") REFERENCES "product" ("product_id");
