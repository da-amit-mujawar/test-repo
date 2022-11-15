INSERT INTO {maintable_name}
(
  ListID,
  LISTTYPE,
  PRODUCTCODE,
  PERMISSIONTYPE,
  FILE_DATE,
  VIP_NO,
  REPCIRNO,
  EMAILADDRESS,
  DOMAIN_NAME,
  EVENT_CODE,
  EVENT_DATE,
  EVENT_TIME,
  EVENT_IP_ADDRESS,
  STATE 
)
SELECT  
  ListID,
  LISTTYPE,
  PRODUCTCODE,
  PERMISSIONTYPE,
  FILE_DATE,
  VIP_NO,
  REPCIRNO,
  EMAILADDRESS,
  DOMAIN_NAME,
  EVENT_CODE,
  EVENT_DATE,
  EVENT_TIME,
  EVENT_IP_ADDRESS,
  STATE
From PRYORPARK_NEW;
