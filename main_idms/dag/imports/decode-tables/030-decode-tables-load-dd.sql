--load from b2c-link-dd into decode tables
select cvalue as ccode, cdescription into {table_empsz}_new
from {tablename1} where cfieldname = 'BUS_EMPLOYEESIZECODE';

select cvalue as ccode, cdescription into {table_salesvol}_new
from {tablename1} where cfieldname = 'BUS_CORPORATE_SALES_VOLUME_CODE';

select cvalue as ccode, cdescription into {table_sqftg}_new
from {tablename1} where cfieldname = 'BUS_SQUARE_FOOTAGE';

select cvalue as ccode, cdescription into {table_busdept}_new
from {tablename1} where cfieldname = 'BUS_DEPARTMENTCODE';

select cvalue as ccode, cdescription into {table_buslvl}_new
from {tablename1} where cfieldname = 'BUS_LEVELCODE';

select cvalue as ccode, cdescription into {table_busfuncarea}_new
from {tablename1} where cfieldname = 'BUS_FUNCTIONALAREACODE';

select cvalue as ccode, cdescription into {table_busrole}_new
from {tablename1} where cfieldname = 'BUS_ROLECODE';






