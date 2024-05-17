CREATE OR REPLACE PROCEDURE SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2_SP()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
  // SET DATABASE
  snowflake.execute({sqlText: `USE DATABASE SAMPLE_DB;`});

  // SET SCHEMA
  snowflake.execute({sqlText: `USE SCHEMA SAMPLE_SCHEMA;`});

  // Create target table if not exist
  snowflake.execute({sqlText: 
  `
   CREATE TABLE IF NOT EXISTS SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2(
    PersonID Varchar(10),
    PersonName	Varchar(255),
    FlightID	Varchar(10),
    Source	Varchar(10),
    Destination	Varchar(10),
    TravelDate DATE,
    SourceFilename Varchar(255),
    VendorTS TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP,
    END_DT DATE
    );
    `
    })

   // Create stream
  snowflake.execute({sqlText: 
  `
    CREATE OR REPLACE STREAM SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2_STREAM ON TABLE 
    SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2
    APPEND_ONLY = TRUE;
  `
  })
  ;

   // Create temporary soruce table from stream
   snowflake.execute({sqlText: 
    `
    CREATE OR REPLACE TEMPORARY TABLE SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SOURCE
    AS
    SELECT
        PersonID,
        PersonName,
        FlightID,
        Source,
        Destination,
        TravelDate,
        SourceFilename,
        InsertTS AS VendorTS
    FROM SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_STREAM 
    ;
    `
    })
   

  // Update existing record for closing latest version
  snowflake.execute({sqlText: 
  `
    UPDATE SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2 T_V1
    SET 
     T_V1.END_DT = S.VENDOR_MIN_DT
    FROM 
    (
        SELECT PersonID, TO_DATE(MIN(VendorTS)) AS VENDOR_MIN_DT 
        FROM SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SOURCE 
        GROUP BY PersonID
    ) S
    JOIN 
    SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2 T_V2
    ON
       S.PersonID = T_V2.PersonID
    AND
       T_V2.END_DT = '9999-12-31'
    ;
  `
  })
  

  // Insert new records from  existing closed version
   snowflake.execute({sqlText: 
   `
    INSERT INTO SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2
    (
        PersonID,
        PersonName,
        FlightID,
        Source,
        Destination,
        TravelDate,
        SourceFilename,
        VendorTS,
        END_DT
    )
    SELECT
        S.PersonID,
        S.PersonName,
        S.FlightID,
        S.Source,
        S.Destination,
        S.TravelDate,
        S.SourceFilename,
        S.VendorTS,
        LEAD(TO_DATE(S.VendorTS),1, '9999-12-31') OVER(PARTITION BY S.PersonID ORDER BY TO_DATE(S.VendorTS), S.TravelDate) AS END_DT
    FROM (SELECT * FROM SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SOURCE) S
    JOIN
    (SELECT DISTINCT PersonID FROM SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2) T
    ON
     S.PersonID = T.PersonID
   ;
   `
   })

  
  //Insert fresh new records
  snowflake.execute({sqlText: 
   `
    INSERT INTO SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2
    (
        PersonID,
        PersonName,
        FlightID,
        Source,
        Destination,
        TravelDate,
        SourceFilename,
        VendorTS,
        END_DT
    )
    SELECT
        S.PersonID,
        S.PersonName,
        S.FlightID,
        S.Source,
        S.Destination,
        S.TravelDate,
        S.SourceFilename,
        S.VendorTS,
        LEAD(TO_DATE(S.VendorTS),1, '9999-12-31') OVER(PARTITION BY S.PersonID ORDER BY TO_DATE(S.VendorTS), S.TravelDate) AS END_DT
    FROM (SELECT * FROM SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SOURCE) S
    LEFT JOIN 
    SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2 T
    ON
    S.PersonID = T.PersonID
    WHERE 
    T.PersonID is NULL
   ;
   `
   })
  
  // Define your JavaScript logic here
  var result = "ETL historical load to target table completed successfully!";
  return result;
$$
;


CALL SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2_SP();
