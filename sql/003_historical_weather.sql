CREATE TABLE IF NOT EXISTS historical_weather (
    id                      SERIAL,
    station_ref             TEXT         NOT NULL,
    date                    DATE         NOT NULL,
    precipitation_sum       NUMERIC(7,2),
    precipitation_hours     NUMERIC(5,1),
    windspeed_max           NUMERIC(6,2),
    winddirection_dominant  INTEGER,
    temperature_mean        NUMERIC(5,2),
    temperature_min         NUMERIC(5,2),
    et0_evapotranspiration  NUMERIC(6,2),
    shortwave_radiation_sum NUMERIC(7,2)
) PARTITION BY RANGE (date);

CREATE TABLE IF NOT EXISTS historical_weather_2000
    PARTITION OF historical_weather
    FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2001
    PARTITION OF historical_weather
    FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2002
    PARTITION OF historical_weather
    FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2003
    PARTITION OF historical_weather
    FOR VALUES FROM ('2003-01-01') TO ('2004-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2004
    PARTITION OF historical_weather
    FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2005
    PARTITION OF historical_weather
    FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2006
    PARTITION OF historical_weather
    FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2007
    PARTITION OF historical_weather
    FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2008
    PARTITION OF historical_weather
    FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2009
    PARTITION OF historical_weather
    FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2010
    PARTITION OF historical_weather
    FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2011
    PARTITION OF historical_weather
    FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2012
    PARTITION OF historical_weather
    FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2013
    PARTITION OF historical_weather
    FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2014
    PARTITION OF historical_weather
    FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2015
    PARTITION OF historical_weather
    FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2016
    PARTITION OF historical_weather
    FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2017
    PARTITION OF historical_weather
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2018
    PARTITION OF historical_weather
    FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2019
    PARTITION OF historical_weather
    FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2020
    PARTITION OF historical_weather
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2021
    PARTITION OF historical_weather
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2022
    PARTITION OF historical_weather
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2023
    PARTITION OF historical_weather
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2024
    PARTITION OF historical_weather
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2025
    PARTITION OF historical_weather
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS historical_weather_2026
    PARTITION OF historical_weather
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

ALTER TABLE historical_weather
ADD CONSTRAINT uq_historical_weather
UNIQUE (station_ref, date);
