@ECHO OFF
SET HOUR=%time:~0,2%
SET dtStamp9=%date:~-4%%date:~4,2%%date:~7,2%
SET dtStamp24=%date:~-4%%date:~4,2%%date:~7,2%
cd\
D:
cd D:\Redshift_DataIngestion

 

Del error.txt
Del stderr.txt

 

if "%HOUR:~0,1%" == " " (SET dtStamp=%dtStamp9%) else (SET dtStamp=%dtStamp24%)

 

ECHO %dtStamp%

 

cmd.exe /c D:\Redshift_DataIngestion\DSG_Extracts_S000000.cmd 2>>stderr.txt
cmd.exe /c D:\Redshift_DataIngestion\DSG_Extracts_S007000.cmd 2>>stderr.txt
cmd.exe /c D:\Redshift_DataIngestion\DSG_Extracts_S083000.cmd 2>>stderr.txt
cmd.exe /c D:\Redshift_DataIngestion\DSG_Extracts_S121000.cmd 2>>stderr.txt
cmd.exe /c D:\Redshift_DataIngestion\DSG_Extracts_S140000.cmd 2>>stderr.txt
cmd.exe /c D:\Redshift_DataIngestion\DSG_Extracts_S143000.cmd 2>>stderr.txt




Findstr "failed Error ERROR error unable cannot No directory" stderr.txt >>error.txt

 

IF NOT EXIST error.txt (ECHO File Does not exists &GOTO :EOF)
FOR %%R IN (error.txt) DO (
IF %%~zR EQU 0 ( blat SuccessMail.html -s "INFO: SUCCESS: DSG_Extracts %dtStamp% completed" -f SqlserverDBA@data-axle.com -to DataLakeNotification@data-axle.com,prateek.agrawal@data-axle.com -html
) ELSE (blat FailureMail.html -s "Alert: FAILED: DSG_Extracts %dtStamp% Failed" -f SqlserverDBA@Data-axle.com -to DataLake@data-axle.com,prateek.agrawal@data-axle.com -html -Attach error.txt)
)
:EOF

 

Exit

