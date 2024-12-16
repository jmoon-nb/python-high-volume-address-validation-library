IF EXISTS
(
	SELECT *
	FROM sys.objects
	WHERE object_id = OBJECT_ID(N'Staging.LocationStandardized')
		  AND type IN ( N'U' )
)
	DROP TABLE Staging.LocationStandardized;
GO

CREATE TABLE Staging.LocationStandardized
(
	StandardizedLocationID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY
   ,LocationName VARCHAR(MAX) NULL
   ,Address1 VARCHAR(MAX) NULL  
   ,Address2 VARCHAR(MAX) NULL
   ,City VARCHAR(MAX) NULL
   ,State VARCHAR(MAX) NULL
   ,Zip VARCHAR(MAX) NULL
   ,County VARCHAR(MAX) NULL
   ,FIPS VARCHAR(MAX) NULL
   ,Country VARCHAR(MAX) NULL
   ,StreetNumberValidation VARCHAR(MAX) NULL
   ,StreetValidation VARCHAR(MAX) NULL
   ,LocalityValidation VARCHAR(MAX) NULL
   ,StateCodeValidation VARCHAR(MAX) NULL
   ,PostalCodeValidation VARCHAR(MAX) NULL
   ,PostalCodeSuffixValidation VARCHAR(MAX) NULL
   ,ValidationError VARCHAR(MAX) NULL
);
GO