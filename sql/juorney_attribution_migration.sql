DROP TABLE "data".journey_attribution;

CREATE TABLE "data".journey_attribution (
	number_in_chain float8 NULL,
	"Undefined" float8 NULL,
	"Awareness" float8 NULL,
	"Consideration" float8 NULL,
	"Acquisition" float8 NULL,
	"Service" float8 NULL,
	"Loyalty" float8 NULL,
	"Loyalty+" float8 NULL,
	total float8 NULL,
	account_id int8 NULL,
	"timestamp" timestamp NULL DEFAULT now()
);
CREATE INDEX ix_data_journey_attribution_number_in_chain ON data.journey_attribution USING btree (number_in_chain);