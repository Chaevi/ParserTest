CREATE TABLE [Content](
	[Id] INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	[dealNumber] NVARCHAR(30) NOT NULL,
	[sellerName] NVARCHAR(max) NULL,
	[sellerInn] NVARCHAR(50) NULL,
	[buyerName] NVARCHAR(max) NULL,
	[buyerInn] NVARCHAR(50) NULL,
	[dealDate] DATE NOT NULL,
	[woodVolumeSeller] NVARCHAR(30) NOT NULL DEFAULT 0,
	[woodVolumeBuyer] NVARCHAR(30) NOT NULL DEFAULT 0
)