#!/bin/bash
echo "[$(date --rfc-3339=seconds)]: run_extracts.sh:Daily Extract Started" 
set -Eeo pipefail

#PROGNAME=$(basename $0)
script_dir=$(dirname "$0")
cd $script_dir

source $script_dir/set_env.sh
type=incremental
table=all
tablist="${log_dir}/tab_list.txt"

run_extract()
{
  [ ! -f $tablist ] && error_exit "ERROR: Table List file is missing. Aborting Extracts..."
  pids=()
  i=0
  # Loop through the table list and extract them
  for name in $(cat $tablist); do
     ((i=i+1))
     sch_name=$(echo $name | cut -f1 -d.)
     tab_name=$(echo $name | cut -f2 -d.)
     # Create preformatted views for extracts
     run_sql "exec DATALAKE.generate_views('$sch_name','$tab_name', upper('$type'));"
     extract_table.sh -type $type -table $tab_name >> $out_file 2>>$err_file&
     pids[$i]=$!
     log "INFO: Initiated Table Extract in background for:$name:pid(${pids[$i]})"
  done

  ## wait for background process status
  for pid in ${pids[*]}; do
    log "INFO: Waiting on pid:$pid" 
    wait $pid
    log "INFO: $pid Completed" 
  done
  ## Archive Run data to another table.
  q_str="insert into datalake.daily_extract_log (select a.* from datalake.source_extracts a "
  q_str=${q_str}" where not exists (select 'x' from datalake.daily_extract_log b where a.start_time=b.start_time and a.schema_name=b.schema_name and a.table_name=b.table_name ));"
  run_sql "$q_str";
}

send_report()
{
 # Send Report
 repFile=$log_dir/extract_report_`date +%Y%m%d`
 sqlplus -s "${dl_user}" @sqls/extract_report.sql $repFile
 (
 echo "From:DataLakeTeam@data-axle.com"
 echo "To: $2"
 echo "MIME-Version: 1.0"
 echo "Subject: $1"
 echo "Content-Type: text/html"
 cat ${repFile}.htm
 ) | sendmail -t
}

start_extract()
{
 log "INFO: Starting Extract"
 # Get Views to be extracted
 log "INFO: Get Tables"
 if [ "$table" != "all"  ]; then
   tablist="${log_dir}/extract_${table}.txt"
   get_tables $tablist "where table_name=upper('$table')";
 else
   get_tables $tablist
 fi
 
 # Run Extracts and upload to s3 bucket for each table
 log "INFO: Running Extracts"
 run_extract 
 # Validate Counts between source and destination
 declare -i chk_cnts=0
 chk_cnts=$(sqlplus -s  "${dl_user}"  <<END
       set pagesize 0 feedback off verify off heading off echo off timing off TRIMOUT ON TRIMSPOOL ON;
       SELECT count(*) FROM datalake.source_extracts 
       where  abs(table_rows_count - redshift_post_load_count) > round(table_rows_count*.01) 
       and status = 'Completed';
       exit;
END
)
if [ ${chk_cnts} -gt 0 ]; then
 log "WARN: ebsprod daily extracts has count mismatch between oracle and redshift tables. Verify extract report"
 echo "ebsprod daily extracts has count mismatch between oracle and redshift tables. Verify extract report" | mailx -s "DTLK-CRITICAL: ebsprod daily extracts has counts mismatch!" -a $log_file $email_failure
 send_report "DTLK-CRITICAL: ebsprod daily extracts has count mismatch over 1% threshold!" $email_failure
else
 # Send Report
 send_report "DTLK-REPORT: daily ebsprod extract report" $email_success
fi

 log "INFO: Completed Extract"
 rm $runFile
 echo "[$(date --rfc-3339=seconds)]: run_extracts.sh:Daily Extract Completed" 
 exit 0
}

##### Main
ensure_only_running
[ $# -eq 0 ] && start_extract

while [ "$1" != "" ]; do
        case $1 in
        -type )
                shift
                type=$1
        	;;
        -table )
                shift
                table=$1
        	;;
        -h )
                usage
                exit
        	;;
        * )
		usage 	
		exit 1
		;;
        esac
       shift
done
start_extract 
