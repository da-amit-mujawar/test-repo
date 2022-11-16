#!/bin/bash
set -Eeo pipefail

#PROGNAME=$(basename $0)
script_dir=$(dirname "$0")
cd $script_dir

source $script_dir/set_env.sh

[ -z $1 ] && error_exit "ERROR: Table Details is missing.."

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

exec_sql "select lower(SCHEMA_NAME)||'.'||lower(TABLE_NAME)||'.'||lower(DATALAKE_VIEW_NAME)||'.'||lower(EXTRACT_VIEW_NAME)||'.'||lower(SYS_CONTEXT('USERENV','DB_NAME')) from DATALAKE.SOURCE_EXTRACTS where table_name=upper('${table}') ;"
[ -z $ret_val ] && error_exit "ERROR: Unable to read table details from DATALAKE.SOURCE_EXTRACTS for table ${table}. Aborting Extracts..."
name=$ret_val
[ -z $name ] && error_exit "ERROR: Name Details is missing.."

sch_name=$(echo $name | cut -f1 -d.)
tab_name=$(echo $name | cut -f2 -d.)
dl_name=$(echo $name | cut -f3 -d.)
ex_name=$(echo $name | cut -f4 -d.)
db_name=$(echo $name | cut -f5 -d.)

log "INFO: Strating Extract for ${db_name}-${ex_name}"
rs_view_name=spectrumdb.${db_name}_${sch_name}_${tab_name}
rs_pre_cnt=$(echo "select count(*) from ${rs_view_name}" | ${psql_redshift_cmd} );
[ $? -ne 0 ] && error_exit "Error: Getting pre count from Redshfit table $?"
log "INFO: Redshift View $rs_view_name Pre-Extract Rows Count: $rs_pre_cnt"
filename=${db_name}_${sch_name}_${tab_name}.$(date +%m%d%Y)
SPOOLLOG=${extract_data_dir}/$filename
sqlplus -s "${dl_user}" <<EOF >/dev/null  2>$err_file
   SET VERIFY      OFF
   -- 
   column t_cnt new_value t_count
   column e_cnt new_value e_count
   select count(*) t_cnt from ${sch_name}.${tab_name};
   select count(*) e_cnt from $ex_name;
   -- Update Status and StartDate
   update source_extracts 
   set    start_time = sysdate, 
            status='Running',
            table_rows_count=&t_count,
            extract_rows_count=&e_count,
            redshift_pre_load_count=${rs_pre_cnt},
            extract_type='${type}'
   where schema_name = upper('$sch_name') and table_name = upper('$tab_name'); 
   commit;
   spool $SPOOLLOG
   SET ECHO        OFF
   SET FEEDBACK    OFF
   SET HEADING     OFF
   SET LINESIZE    32767
   SET PAGESIZE    0
   SET TERMOUT     OFF
   SET TIMING      OFF
   SET TRIMOUT     OFF
   SET TRIMSPOOL   ON
   SET VERIFY      OFF
   select /*+ parallel(4) */ * from $ex_name ;
   spool off
   exit;
EOF
[ $? -ne 0 ] && error_exit "Error: sqlplus executing failed with error $?"
log "INFO: Extracted $db_name-$ex_name table to file ${extract_data_dir}/$filename"

## Validate File
src_filename=${extract_data_dir}/$filename
#gz_filename=${src_filename}.gz
if iconv -f UTF-8 $src_filename -o /dev/null 2>>$err_file ; then
  log "INFO: UTF-8 Validation Succeeded for $src_filename"
  ## Get Extract Counts
  ext_cnt=$(wc -l ${src_filename} | cut -f1 -d" " 2>>${err_file})
  log "INFO: $ext_cnt Rows Extracted in $src_filename"
  ## Copy file to s3 bucket
  #[ -f ${gz_filename} ] && rm ${gz_filename}
  #gzip $src_filename 2>> $log_file  
  #log "INFO: gzip Succeeded: $gz_filename"
  dest_filename=${s3_bucket_url}/${db_name}/${sch_name}_${tab_name}/
  #aws s3 cp $gz_filename $dest_filename >$out_file 2>> $log_file 
  if [ "$type" = "full" ]; then
   log "INFO: Deleted old-files from s3 bucket $dest_filename"
   aws s3 rm --recursive $dest_filename >>$out_file 2>> $log_file
  fi
  aws s3 cp $src_filename $dest_filename >>$out_file 2>> $log_file 
  #log "INFO: Uploaded $gz_filename to $dest_filename"
  log "INFO: Uploaded $src_filename to $dest_filename"
#Run Post counts
rs_post_cnt=`echo "select count(*) from $rs_view_name" | ${psql_redshift_cmd} `;
[ $? -ne 0 ] && error_exit "Error: Getting post count from Redshfit table $?"
run_sql "update source_extracts set status='Completed', end_time=sysdate, redshift_post_load_count=$rs_post_cnt where schema_name = upper('$sch_name') and table_name = upper('$tab_name');"
else
  error_exit "ERROR: File $src_filename is not UTF-8 compatible"
fi
exit
