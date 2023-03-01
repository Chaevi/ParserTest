CREATE TABLE [dbo].[Table]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY, 
    [dealNumber] NCHAR(30) NOT NULL UNIQUE, 
    [sellerName] NCHAR(200) NOT NULL, 
    [sellerInn] NCHAR(20) NOT NULL, 
    [buyerName] NCHAR(200) NOT NULL, 
    [buyerInn] NCHAR(20) NULL, 
    [dealDate] DATE NOT NULL, 
    [woodVolume] NCHAR(30) NOT NULL
)