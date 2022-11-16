copy {{ params.tablename }}
(
cTitle,
cTitleCode,
cFunctionCode
)
from 's3://idms-2722-internalfiles/neptune/TitleToCode_lookup.txt'
iam_role '{{ params.iamrole }}'
delimiter '|'
IGNOREHEADER 0
;COMMIT;
