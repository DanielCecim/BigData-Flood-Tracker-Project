CREATE TABLE IF NOT EXISTS historical_river_levels (
    id          SERIAL,
    station_ref TEXT        NOT NULL,
    date        DATE        NOT NULL,
    value_m     NUMERIC(8,3),
    quality     TEXT,
    source      TEXT        NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE IF NOT EXISTS historical_river_levels_2000
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2001
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2002
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2003
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2003-01-01') TO ('2004-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2004
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2005
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2006
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2007
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2008
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2009
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2010
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2011
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2012
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2013
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2014
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2015
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2016
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2017
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2018
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2019
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2020
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2021
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2022
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2023
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2024
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2025
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS historical_river_levels_2026
    PARTITION OF historical_river_levels
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

ALTER TABLE historical_river_levels
ADD CONSTRAINT uq_river_level
UNIQUE (station_ref, date);
