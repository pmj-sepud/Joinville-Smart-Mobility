CREATE SCHEMA IF NOT EXISTS waze;

CREATE TABLE IF NOT EXISTS waze.data_files
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"start_time_millis"                 BIGINT NOT NULL,
"end_time_millis"                   BIGINT NOT NULL,
"start_time"                        TIMESTAMP,
"end_time"                          TIMESTAMP,
"date_created"                      TIMESTAMP,
"date_updated"                      TIMESTAMP,
"file_name"                         TEXT NOT NULL,
"json_hash"                         VARCHAR(40) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "IDX_UNIQUE_json_hash"
ON waze.data_files USING btree
(json_hash COLLATE pg_catalog."default")
TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS waze.jams 
(
  "id"                              VARCHAR(40) PRIMARY KEY NOT NULL,
  "uuid"                            TEXT NOT NULL,
  "pub_millis"                      BIGINT NOT NULL,
  "pub_utc_date"                    TIMESTAMP,
  "start_node"                      TEXT,
  "end_node"                        TEXT,
  "road_type"                       INTEGER,
  "street"                          TEXT,
  "city"                            TEXT,
  "country"                         TEXT,
  "delay"                           INTEGER,
  "speed"                           float4,
  "speed_kmh"                       float4,
  "length"                          INTEGER,
  "turn_type"                       TEXT,
  "level"                           INTEGER,
  "blocking_alert_id"               TEXT,
  "line"                            JSONB,
  "datafile_id"                     BIGINT NOT NULL REFERENCES waze.data_files (id)
);

CREATE TABLE IF NOT EXISTS waze.alerts
(
  "id"                              VARCHAR(40) PRIMARY KEY NOT NULL,
  "uuid"                            TEXT NOT NULL, 
  "pub_millis"                      BIGINT NOT NULL,
  "pub_utc_date"                    TIMESTAMP,
  "road_type"                       INTEGER,
  "location"                        JSONB,
  "street"                          TEXT,
  "city"                            TEXT,
  "country"                         TEXT,
  "magvar"                          INTEGER,
  "reliability"                     INTEGER,
  "report_description"              TEXT,
  "report_rating"                   INTEGER,
  "confidence"                      INTEGER,
  "type"                            TEXT,
  "subtype"                         TEXT,
  "report_by_municipality_user"     BOOLEAN,
  "thumbs_up"                       INTEGER,
  "jam_uuid"                        TEXT,
  "datafile_id"                     BIGINT NOT NULL REFERENCES waze.data_files (id)
);

CREATE TABLE IF NOT EXISTS waze.irregularities
(
  "id"                              VARCHAR(40) PRIMARY KEY NOT NULL,
  "uuid"                            TEXT NOT NULL,
  "detection_date_millis"           BIGINT NOT NULL,
  "detection_date"                  TEXT,
  "detection_utc_date"              TIMESTAMP,
  "update_date_millis"              BIGINT NOT NULL,
  "update_date"                     TEXT,
  "update_utc_date"                 TIMESTAMP,
  "street"                          TEXT,
  "city"                            TEXT,
  "country"                         TEXT,
  "is_highway"                      BOOLEAN,
  "speed"                           float4,
  "regular_speed"                   float4,
  "delay_seconds"                   INTEGER,
  "seconds"                         INTEGER,
  "length"                          INTEGER,
  "trend"                           INTEGER,
  "type"                            TEXT,
  "severity"                        float4,
  "jam_level"                       INTEGER,
  "drivers_count"                   INTEGER,
  "alerts_count"                    INTEGER,
  "n_thumbs_up"                     INTEGER,
  "n_comments"                      INTEGER,
  "n_images"                        INTEGER,
  "line"                            JSONB,
  "datafile_id"                     BIGINT NOT NULL REFERENCES waze.data_files (id)
);


CREATE TABLE IF NOT EXISTS waze.coordinates 
(
  "id"                              SERIAL PRIMARY KEY NOT NULL,
  "latitude"                        float8 NOT NULL,
  "longitude"                       float8 NOT NULL,
  "order"                           INTEGER NOT NULL,
  "jam_id"                          VARCHAR(40) REFERENCES waze.jams (id),
  "irregularity_id"                 VARCHAR(40) REFERENCES waze.irregularities (id),
  "alert_id"                        VARCHAR(40) REFERENCES waze.alerts (id)
);

CREATE TABLE IF NOT EXISTS waze.roads 
(
  "id"                              SERIAL PRIMARY KEY NOT NULL,
  "value"                           INTEGER NOT NULL,
  "name"                            VARCHAR[100] NOT NULL
);

CREATE TABLE IF NOT EXISTS waze.alert_types 
(
  "id"                              SERIAL PRIMARY KEY NOT NULL,
  "type"                            TEXT NOT NULL,
  "subtype"                         TEXT
);

CREATE SCHEMA IF NOT EXISTS geo;

CREATE TABLE IF NOT EXISTS geo.sections
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"id_arcgis"                         BIGINT NOT NULL,
"street_code"                       INTEGER NOT NULL,
"street_name"                       TEXT NOT NULL,
"cumulative_meters"                 INTEGER NOT NULL,
"length"                            float4,
"wkt"                               TEXT NOT NULL
);


/*class JamPerSection(Base):
    __tablename__ = "JamPerSection"
    
    id = Column("JpsId", Integer, primary_key=True)
    JamDateStart = Column("JamDateStart", DateTime(timezone=True), nullable=False)
    JamUuid = Column("JamUuid", Integer, nullable=False) 
    SctnId = Column("SctnId", Integer, ForeignKey("Section.SctnId", ondelete="CASCADE"), nullable=False)
    
    __table_args__ = (ForeignKeyConstraint([JamDateStart, JamUuid],
                                           ["Jam.JamDateStart", "Jam.JamUuid"],
                                           ondelete="CASCADE"),
                      UniqueConstraint("JamDateStart", "JamUuid", "SctnId", name="jammed_section"),
                      {})*/