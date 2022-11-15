@ECHO OFF
: Sets the proper date and time stamp with 24Hr Time for log file naming
: convention

D:
cd D:\Datalake_To_Redshift

del D:\Datalake_To_Redshift\output\*.gz

for /F "tokens=1,2,3,4" %%i in (Tablelist.txt) do call :process %%i %%j %%k %%L
goto Mail
goto thenextstep
:process

set BCP_EXPORT_SERVER=%1
set BCP_EXPORT_DB=%2
set BCP_EXPORT_TABLE=%3
set BCP_EXPORT_PATH=%4

@ECHO on

BCP %BCP_EXPORT_DB%.dbo.View_%BCP_EXPORT_TABLE%_DSG out %BCP_EXPORT_DB%_%BCP_EXPORT_TABLE% -S %BCP_EXPORT_SERVER% -C 65001 -c -T

gzip %BCP_EXPORT_DB%_%BCP_EXPORT_TABLE% 

aws s3 cp %BCP_EXPORT_DB%_%BCP_EXPORT_TABLE%.gz s3://axle-internal-sources/oess/%BCP_EXPORT_DB%_%BCP_EXPORT_TABLE%/%BCP_EXPORT_DB%_%BCP_EXPORT_TABLE%.gz

Move %BCP_EXPORT_DB%_%BCP_EXPORT_TABLE%.gz %BCP_EXPORT_PATH%


set BCP_EXPORT_SERVER=
set BCP_EXPORT_DB=
set BCP_EXPORT_TABLE=
set BCP_EXPORT_PATH=

goto :EOF

:Mail


Exit

