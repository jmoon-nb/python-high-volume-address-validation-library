IF EXISTS (
    SELECT
        *
    FROM
        sys.objects
    WHERE
        object_id = OBJECT_ID (N'Staging.LocationStandardized')
        AND type IN (N'U')
)
DROP TABLE Staging.LocationStandardized;
GO

CREATE TABLE Staging.LocationStandardized (
    LocationID INT NOT NULL PRIMARY KEY 
   ,ImportedLocationID INT NULL
   ,JRSLocationID VARCHAR(MAX) NULL
   ,LocationName VARCHAR(MAX) NULL
   ,Address1 VARCHAR(MAX) NULL
   ,Address2 VARCHAR(MAX) NULL
   ,City VARCHAR(MAX) NULL
   ,State VARCHAR(MAX) NULL
   ,Zip VARCHAR(MAX) NULL
   ,County VARCHAR(MAX) NULL
   ,FIPS VARCHAR(MAX) NULL
   ,Country VARCHAR(MAX) NULL
   ,StreetNumberValidation VARCHAR(MAX) NULL /* <COLUMNS returned BY VALIDATION>*/
   ,StreetValidation VARCHAR(MAX) NULL
   ,LocalityValidation VARCHAR(MAX) NULL
   ,StateCodeValidation VARCHAR(MAX) NULL
   ,PostalCodeValidation VARCHAR(MAX) NULL
   ,PostalCodeSuffixValidation VARCHAR(MAX) NULL
   ,ValidationError VARCHAR(MAX) NULL
);
GO

BULK INSERT Staging.LocationStandardized
FROM '/var/opt/mssql/data/imports/final-validation-results.csv'
WITH (  
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2 -- Skip the header row (optional)
);
GO 

--     LocationID INT NOT NULL PRIMARY KEY
--    ,MappedLocationID INT NULL
--    ,ImportedLocationID INT NULLs
--    ,JRSLocationID VARCHAR(MAX) NULL
--    ,ActionID INT NULL
--    ,ActionComment VARCHAR(MAX) NULL
--    ,ImportResultID INT NULL
--    ,ImportResultComment VARCHAR(MAX) NULL
--    ,LocationName VARCHAR(MAX) NULL
--    ,Address1 VARCHAR(MAX) NULL
--    ,Address2 VARCHAR(MAX) NULL
--    ,City VARCHAR(MAX) NULL
--    ,State VARCHAR(MAX) NULL
--    ,Zip VARCHAR(MAX) NULL
--    ,County VARCHAR(MAX) NULL
--    ,CountyID INT NULL
--    ,FIPS VARCHAR(MAX) NULL
--    ,Country VARCHAR(MAX) NULL
--    ,Phone VARCHAR(MAX) NULL
--    ,ContactID_Invoice INT NULL
--    ,ImportedContactIDInvoiceTo INT NULL
--    ,ContactID_Cert INT NULL
--    ,ImportedContactIDCertTo INT NULL
--    ,ContactID_Owner INT NULL
--    ,ImportedContactIDOwner INT NULL
--    ,ContactID_Primary INT NULL
--    ,ImportedContactIDPrimary INT NULL
--    ,LocationAltNum VARCHAR(MAX) NULL
--    ,SICID INT NULL
--    ,SICValue VARCHAR(MAX) NULL
--    ,NatureOfBusinessID INT NULL
--    ,NatureOfBusinessValue VARCHAR(MAX) NULL
--    ,LocationComments VARCHAR(MAX) NULL
--    ,PopulationDensity FLOAT NULL
--    ,LocationLatitude NUMERIC(18, 6) NULL
--    ,LocationLongitude NUMERIC(18, 6) NULL
--    ,LegacyID VARCHAR(MAX) NULL
--    ,LocationAltNum2 VARCHAR(MAX) NULL
--    ,LocationAltNum3 VARCHAR(MAX) NULL
--    ,LocationAltNum4 VARCHAR(MAX) NULL
--    ,LocationAltNum5 VARCHAR(MAX) NULL
--    ,LocationAltNum6 VARCHAR(MAX) NULL
--    ,LocationAltNum7 VARCHAR(MAX) NULL
--    ,LocationAltNum8 VARCHAR(MAX) NULL
--    ,LocationAltNum9 VARCHAR(MAX) NULL
--    ,LocationAltNum10 VARCHAR(MAX) NULL
--    ,LocationAltNum11 VARCHAR(MAX) NULL
--    ,LocationTypeID INT NULL
--    ,LocationTypeValue VARCHAR(MAX) NULL