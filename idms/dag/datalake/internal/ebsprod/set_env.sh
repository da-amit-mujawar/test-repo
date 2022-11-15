set -Eeo pipefail

export NLS_LANG=American_America.utf8
export NLS_DATE_FORMAT='YYYY-MM-DD'

extract_data_dir="/u02/datalake"
s3_bucket_url="s3://axle-internal-sources/raw"

log_dir=$script_dir/log
[ ! -d $log_dir ] && mkdir -p $log_dir
run_dt=$(date +%F)
log_file=$log_dir/run_extract_${run_dt}.log
out_file=$log_dir/run_extract_${run_dt}.out
err_file=$log_dir/run_extract_${run_dt}.err
runFile=${log_dir}/${PROGNAME}.lck
touch $out_file

psql_redshift_cmd="psql -qtAX -w -h da-rs-01-dev -p 5439 -d dev -U awsuser"
email_success=srihari.leelakrishnan@data-axle.com
email_success="DataLakeNotification@data-axle.com srihari.leelakrishnan@data-axle.com"
email_failure=srihari.leelakrishnan@data-axle.com
email_failure="DataLake@data-axle.com srihari.leelakrishnan@data-axle.com"

function log {
  echo "[$(date --rfc-3339=seconds)]: $*" >> $log_file
# echo "[$(date --rfc-3339=seconds)]: $*" 
}

error_exit()
{
#       ----------------------------------------------------------------
#       Function for exit due to fatal program error
#               Accepts 1 argument:
#                       string containing descriptive error message
#       ----------------------------------------------------------------

	[ $# -ge 1 ] && log $1
        error="ERROR: $(caller): ${BASH_COMMAND}"   
        #echo "${PROGNAME}: ${1:-"Unknown Error"}" 1>&2
        log $error
        if [[ "$error" == *"run_extract"* ]];
        then
            echo $error |  mailx -s "DTLK-CRITICAL: ebsprod daily extracts failed!" -a $log_file -a $out_file -a $err_file $email_failure
        fi
        [ -f $runFile ] && rm $runFile
        exit 1
}
trap error_exit ERR 

dl_user=`cat ~/.datalake`


usage()
{
echo "------------------------------------------------------------------------------------------------------------------------------------------"
echo "usage: run_extracts [-h] | [-type full|incremental  -table EXTRACT_VIEW_NAME ] "
echo ""
echo ""
echo "Examples:"
echo "------------------------------------------------------------------------------------------------------------------------------------------"
echo "ex: To run incremental extracts of all tables in source_extracts table, run below command:"
echo " ./run_extracts.sh "
echo ""
echo "ex: To run incremental extracts of specific table from source_extracts table, run below command:"
echo " ./run_extracts.sh -table ar.ar_collectors "
echo ""
echo "ex: To run full extracts of all tables in source_extracts table, run below command:"
echo " ./run_extracts.sh -type full "
echo ""
}

function run_sql()
{
echo "$*"
sqlplus -s "${dl_user}" <<EOF >/dev/null 2>> $err_file
$*
commit;
exit;
EOF
}

ret_val=""
function exec_sql()
{
echo "$*"
ret_val=$(sqlplus -s "${dl_user}" <<EOF 
set linesize 200 pagesize 0 feedback off verify off heading off echo off timing off TRIMOUT ON TRIMSPOOL ON ;
$*
exit;
EOF
)  >/dev/null 2>> ${err_file}
echo "ret_val=$ret_val"
}

function ensure_only_running {
  if [ -f $runFile ]; then
  #if [ "$(pgrep -fn $0)" -ne "$(pgrep -fo $0)" ]; then
    log "ERROR: Detected multiple instances of $0 running, exiting."
    exit 1
  else
    touch $runFile
  fi
}

function get_tables()
{

SPOOLLOG="$1"
wherestr=$2

### Generate the Table list for extraction
[ -f $SPOOLLOG ] && (rm -f $SPOOLLOG)
sqlplus -s "${dl_user}" <<EOF > /dev/null 2>&1
spool $SPOOLLOG
SET ECHO        OFF
SET FEEDBACK    OFF
SET HEADING     OFF
SET LINESIZE    5000
SET PAGESIZE    0
SET TERMOUT     OFF
SET TIMING      OFF
SET TRIMOUT     OFF
SET TRIMSPOOL   ON
SET VERIFY      OFF
select lower(SCHEMA_NAME)||'.'||
       lower(TABLE_NAME)
from DATALAKE.SOURCE_EXTRACTS $wherestr
ORDER BY TABLE_ROWS_COUNT ASC;
spool off
exit;
EOF
}

