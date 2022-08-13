CREATE TABLE public.dim_customer (
	id int4 NULL,
	"name" varchar NULL
);

CREATE TABLE public.dim_date (
	id int4 NULL,
	"date" date NULL,
	"month" int4 NULL,
	quarter_of_year int4 NULL,
	"year" int4 NULL,
	is_weekend bool NULL
);


CREATE TABLE public.fact_order_accumulating (
	order_date_id int4 NULL,
	invoice_date_id int4 NULL,
	payment_date_id int4 NULL,
	customer_id int4 NULL,
	order_number varchar NULL,
	invoice_number varchar NULL,
	payment_number varchar NULL,
	total_order_quantity int4 NULL,
	total_order_usd_amount numeric NULL,
	order_to_invoice_lag_days int4 NULL,
	invoice_to_payment_lag_days int4 NULL
);