CREATE OR REPLACE PROCEDURE SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD1_SP()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
  // SET DATABASE
  snowflake.execute({sqlText: `USE DATABASE SAMPLE_DB;`});

  // SET SCHEMA
  snowflake.execute({sqlText: `USE SCHEMA SAMPLE_SCHEMA;`});

  // Create SCD1 target table if not exists
  snowflake.execute({sqlText: 
  `
   CREATE TABLE IF NOT EXISTS SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD1 (
    PersonID Varchar(10),
    PersonName	Varchar(255),
    FlightID	Varchar(10),
    Source	Varchar(10),
    Destination	Varchar(10),
    TravelDate DATE,
    SourceFilename Varchar(255),
    VendorTS TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP
  );
  `
  })

  // Insert or update to capture latest changes only
  snowflake.execute({sqlText: 
  `  
    MERGE INTO SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD1 AS tgt
    USING 
    (
     SELECT 
     *
     FROM
     SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2_STREAM 
     WHERE
        END_DT = '9999-12-31'    
    )
    AS src
    ON tgt.PERSONID = src.PERSONID
    WHEN MATCHED and (src.FLIGHTID <> tgt.FLIGHTID OR src.SOURCE <> tgt.SOURCE OR src.DESTINATION <> tgt.DESTINATION OR src.TRAVELDATE <> tgt.TRAVELDATE OR src.SOURCEFILENAME <> tgt.SOURCEFILENAME OR src.VENDORTS <> tgt.VENDORTS) THEN
    UPDATE SET 
        tgt.FLIGHTID = src.FLIGHTID, 
        tgt.SOURCE = src.SOURCE,
        tgt.DESTINATION = src.DESTINATION,
        tgt.TRAVELDATE = src.TRAVELDATE,
        tgt.SOURCEFILENAME = src.SOURCEFILENAME,
        tgt.VENDORTS = src.VENDORTS
    WHEN NOT MATCHED THEN
        INSERT (PersonID, PersonName, FlightID, Source, Destination, TravelDate, SourceFilename, VendorTS) VALUES (src.PersonID, src.PersonName, src.FlightID, src.Source, src.Destination, src.TravelDate, src.SourceFilename, src.VendorTS)
      ;
  `
  })
  
  // Define your JavaScript logic here
  var result = "ETL latest data only load to target table completed successfully!";
  return result;
  
$$
;

CALL SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD1_SP();
