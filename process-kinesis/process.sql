/* UPDATE INPUT */

CREATE OR REPLACE STREAM "INPUT_STREAM" ( 
    "COL_LOCAL" varchar(255), 
    "TEMP" real,
    "UMID" real,
    "COL_MIN" real,
    "COL_MAX" real
);

CREATE OR REPLACE PUMP "INPUT_PUMP" AS 
    INSERT INTO "INPUT_STREAM"
    SELECT STREAM "COL_LOCAL", (("TEMP" * 1.8) + 32), ("UMID" * 0.01), "COL_MIN", "COL_MAX"
    FROM "SOURCE_SQL_STREAM_001";
    

/* PROCESS INPUT DATA */

CREATE OR REPLACE STREAM "OUTPUT_STREAM" (
    "DISTRICT" varchar(255),
    "HEAT_INDEX" real,
    "MINIMA" real,
    "MAXIMA" real
);

CREATE OR REPLACE PUMP "OUTPUT_PUMP" AS 
	INSERT INTO "OUTPUT_STREAM"
	SELECT STREAM "COL_LOCAL",
        CASE
            WHEN (
                (1.1 * "TEMP")
                -10.3
                + (0.047 * "UMID")
            ) < 80
            THEN (
                (1.1 * "TEMP")
                - 10.3
                + (0.047 * "UMID")
            )
            WHEN (
                "TEMP" >= 80.0
                AND "TEMP" <= 112.0
                AND "UMID" <= 0.13
            )
            THEN (
                (
                    - 42.379
                    + (2.04901523 * "TEMP")
                    + (10.14333127 * "UMID")
                    - (0.22475541 * "TEMP" * "UMID")
                    - (6.83783 * POWER(10, -3) * POWER("TEMP", 2))
                    - (5.481717 * POWER(10, -2) * POWER("UMID", 2))
                    + (1.22874 * POWER(10, -3) * POWER("TEMP", 2) * "UMID")
                    + (8.5282 * POWER(10, -4) * "TEMP" * POWER(10, 2))
                    - (1.99 * POWER(10, -6) * POWER("TEMP", 2) * POWER("UMID", 2))
                )
                - (
                    (3.25 - (0.25 * "UMID"))
                    * POWER((17 - ABS("TEMP" - 95)) / 17, 0.5)
                )
            )
            WHEN (
                "TEMP" >= 80.0
                AND "TEMP" <= 87.0
                AND "UMID" > 0.85
            )
            THEN (
                (
                    - 42.379
                    + (2.04901523 * "TEMP")
                    + (10.14333127 * "UMID")
                    - (0.22475541 * "TEMP" * "UMID")
                    - (6.83783 * POWER(10, -3) * POWER("TEMP", 2))
                    - (5.481717 * POWER(10, -2) * POWER("UMID", 2))
                    + (1.22874 * POWER(10, -3) * POWER("TEMP", 2) * "UMID")
                    + (8.5282 * POWER(10, -4) * "TEMP" * POWER(10, 2))
                    - (1.99 * POWER(10, -6) * POWER("TEMP", 2) * POWER("UMID", 2))
                )
                + (
                    0.02
                    * ("UMID" - 85)
                    * (87 - "TEMP")
                )

            )
        ELSE (
                (
                    - 42.379
                    + (2.04901523 * "TEMP")
                    + (10.14333127 * "UMID")
                    - (0.22475541 * "TEMP" * "UMID")
                    - (6.83783 * POWER(10, -3) * POWER("TEMP", 2))
                    - (5.481717 * POWER(10, -2) * POWER("UMID", 2))
                    + (1.22874 * POWER(10, -3) * POWER("TEMP", 2) * "UMID")
                    + (8.5282 * POWER(10, -4) * "TEMP" * POWER(10, 2))
                    - (1.99 * POWER(10, -6) * POWER("TEMP", 2) * POWER("UMID", 2))
                )
        )
        END,
		"COL_MIN", "COL_MAX"
	FROM "INPUT_STREAM";
