INSERT INTO 
PRYORPARK_Log_ToBeDropped (cLog)
Select 'Records deleted - File date:'+ FILE_DATE + ':     Count:'+ Cast(COUNT(*) as Varchar(10) ) as cLog
from 
{maintable_name} 
WHERE CONVERT(datetime, FILE_DATE)  < DATEADD(week, -18, GETDATE())
group by FILE_DATE
;



Delete
from 
{maintable_name}
WHERE CONVERT(datetime, FILE_DATE)  < DATEADD(week, -18, GETDATE())
;