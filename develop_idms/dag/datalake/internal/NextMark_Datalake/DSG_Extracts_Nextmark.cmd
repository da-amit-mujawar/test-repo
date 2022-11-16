@ECHO OFF

D:
cd D:\Redshift_DataIngestion

for /F "tokens=1,2,3,4" %%i in (TablesList.txt) do call :process %%i %%j %%k %%L
goto Mail
goto thenextstep
:process

set BCP_EXPORT_SERVER=%1
set BCP_EXPORT_DB=%2
set BCP_EXPORT_TABLE=%3
set BCP_EXPORT_PATH=%4

@ECHO on

BCP %BCP_EXPORT_DB%.dbo.View_%BCP_EXPORT_TABLE%_DSG out %BCP_EXPORT_DB%_%BCP_EXPORT_TABLE%.csv -S %BCP_EXPORT_SERVER% -c -T

Move /Y %BCP_EXPORT_DB%_%BCP_EXPORT_TABLE%.csv %BCP_EXPORT_PATH%


del %BCP_EXPORT_TABLE%_Headers.txt
del %BCP_EXPORT_TABLE%_Data.txt

set BCP_EXPORT_SERVER=
set BCP_EXPORT_DB=
set BCP_EXPORT_TABLE=
set BCP_EXPORT_PATH=

goto :EOF

:Mail


Exit

