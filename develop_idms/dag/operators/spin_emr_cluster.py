import json
import logging
import time
from datetime import date

import boto3
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class EMROperator(BaseOperator):
    """
    Create a Cluster on Aws EMR and Run Steps in the specified folder and copy data to s3 file using Emr
    Parameters: 
                aws_credentials_id: AWS credentials ID(aws_secret_key,aws_accesskey_id,aws_session)
                s3_bucket: S3 bucket where EMR Cluster Logs files are located
                region: AWS Region where the input data is located
    """

    # Define your operators params (with defaults) here
    @apply_defaults
    def __init__(self, base_config_path="", custom_config_path="", region="", process_info=None,
                 spark_config=None, *args, **kwargs):

        super(EMROperator, self).__init__(*args, **kwargs)
        # Map params here:
        if process_info is None:
            process_info = {}
        self.base_config_path = base_config_path
        self.config_file_path = custom_config_path
        self.process_info = process_info

        self.process_dt = self.process_info.get('process_date', date.today().strftime("%Y%m%d"))

        self.region = region
        self.spark_config = spark_config
        logger.info("Config File :{0}".format(self.base_config_path))
        logger.info("Base File   :{0}".format(self.config_file_path))
        logger.info("Process Dt. :{0}".format(self.process_dt))
        logger.info("Region      :{0}".format(self.region))
        logger.info("Spark Config:{0}".format(self.spark_config))
        # self.autocommit = True

    ui_color = "#89DA59"

    def execute(self, context):
        try:
            f_base_config = self.base_config_path
            f_job_config = self.config_file_path
            f_spark_config = self.spark_config
            ls_4y = self.process_dt[0:4]
            ls_mm = self.process_dt[4:6]
            ls_dd = self.process_dt[6:8]

            with open(f_base_config) as b:
                cfg_base = json.load(b)

            with open(f_job_config) as f:
                cfg_jobs = json.load(f)

            with open(f_spark_config) as s:
                cfs_spark = s.read()
                for x in self.process_info:
                    cfs_spark = cfs_spark.replace("{" + x + "}", self.process_info[x])
                cfs_spark = cfs_spark.replace("{yyyy}", ls_4y).replace('{mm}', ls_mm).replace(
                    '{dd}', ls_dd)
                cfg_spark = json.loads(cfs_spark)
            logger.info(cfg_spark)

            logger.info("Reading DAG Variables...")
            #   Read DAG Variables

            ll_appln = []
            for i in range(len(cfg_base["Application"])):
                ld_app = {'Name': cfg_base["Application"][i]}
                ll_appln.append(ld_app.copy())

            logger.info("Setting up Values from the Variables to AWS thru Boto3...")
            # Setup Instances
            ld_instance = cfg_base["Instance"]

            lj_emr_data = Variable.get("var-EMRGrp-SummaryCalc", deserialize_json=True)
            ls_sg_master = lj_emr_data["MasterSecurityGroup"]
            ls_sg_slave = lj_emr_data["SlaveSecurityGroup"]
            ls_subnet = lj_emr_data["SubnetId"]
            ls_ssh_key = lj_emr_data["SSHKeyName"]
            ls_inst_profile = lj_emr_data["InstanceProfile"]
            ls_log_bucket = lj_emr_data["LogsBucket"]
            ls_sg_srvc_access = lj_emr_data["ServiceAccessSecurityGroup"]
            ls_region = self.region or (cfg_base["Region"] or lj_emr_data["Region"])

            ls_concur_level = int(cfg_jobs.get('Concurrency', cfg_base.get('Concurrency', 1)))

            logger.info("Connecting to AWS thru Boto3...")
            connection = boto3.client('emr', region_name=ls_region)

            # Setup Bootstrap Actions
            lj_bootstrap = [{'Name': lj_emr_data["BootstrapName"],
                             'ScriptBootstrapAction': {'Path': lj_emr_data["BootstrapPath"]}
                             }]

            lj_instance = {
                'InstanceGroups': [
                    {'Name': ld_instance[0]["Name"],
                     'Market': 'ON_DEMAND',
                     'InstanceRole': ld_instance[0]["InstanceGroupType"],
                     'InstanceType': ld_instance[0]["InstanceType"],
                     'InstanceCount': ld_instance[0]["InstanceCount"]},
                    {'Name': ld_instance[1]["Name"],
                     'Market': 'ON_DEMAND',
                     'InstanceRole': ld_instance[1]["InstanceGroupType"],
                     'InstanceType': ld_instance[1]["InstanceType"],
                     'InstanceCount': ld_instance[1]["InstanceCount"]}
                ],
                'Ec2KeyName': ls_ssh_key,
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'Ec2SubnetId': ls_subnet,
                'EmrManagedMasterSecurityGroup': ls_sg_master,
                'EmrManagedSlaveSecurityGroup': ls_sg_slave,
                'ServiceAccessSecurityGroup': ls_sg_srvc_access
            }

            # Setup AutoScaling
            try:
                ld_scaling = cfg_jobs["ComputeLimits"]
            except:
                ld_scaling = cfg_base["ComputeLimits"]

            lj_scaling = {
                'ComputeLimits': {
                    'UnitType': ld_scaling["UnitType"],
                    'MinimumCapacityUnits': ld_scaling["MinimumCapacityUnits"],
                    'MaximumCapacityUnits': ld_scaling["MaximumCapacityUnits"],
                    'MaximumOnDemandCapacityUnits': ld_scaling["MaximumOnDemandCapacityUnits"],
                    'MaximumCoreCapacityUnits': ld_scaling["MaximumCoreCapacityUnits"]
                }
            }

            # lj_tags = cfg_base.get("Tags")
            lj_tags = lj_emr_data.get("Tags")

            logger.info("Starting EMR Cluster ...")

            logger.info("Name        :{0}\nLog         :{1}\n"
                        "App. Name   :{3}\nReleaseLabel:{2}\n"
                        "Instances   :{4}\nBootstrap   :{5}\n"
                        "Tags        :{6}".format(cfg_jobs["ClusterName"], ls_log_bucket,
                                                  ll_appln, cfg_base["EmrLabel"],
                                                  lj_instance, lj_bootstrap, lj_tags))

            cluster_id = connection.run_job_flow(
                Name=cfg_jobs["ClusterName"],
                LogUri=ls_log_bucket,
                ReleaseLabel=cfg_base["EmrLabel"],
                Applications=ll_appln,
                Instances=lj_instance,
                BootstrapActions=lj_bootstrap,
                VisibleToAllUsers=True,
                JobFlowRole=ls_inst_profile,
                ServiceRole='EMR_DefaultRole',
                StepConcurrencyLevel=ls_concur_level,
                ManagedScalingPolicy=lj_scaling,
                Tags=lj_tags)

            # Reading all the jobs / Steps and add to the Cluster
            logger.info("Setting up Jobs on EMR Cluster...")
            ld_process_date = self.process_dt
            if ld_process_date == "":
                ld_process_date = date.today().strftime("%Y%m%d")

            steps_involved = []
            for i in range(len(cfg_jobs["steps"])):
                list_steps = {
                    'Name': cfg_jobs["steps"][i]['Name'],
                    'ActionOnFailure': cfg_jobs["steps"][i]['ActionOnFailure'],
                    'HadoopJarStep': {
                        'Jar': cfg_jobs["steps"][i]['Jar'],
                        'Args': cfg_jobs["steps"][i]['Args']
                    }
                }
                steps_involved.append(list_steps.copy())
                logger.info(steps_involved)

            logger.info("Replace Process Date and S3 Code Bucket for the steps involved...")
            for i in range(len(steps_involved)):
                arg_list = []
                for j in steps_involved[i]["HadoopJarStep"]["Args"]:
                    lj_args = j
                    logger.info(j)
                    for x in self.process_info:
                        lj_args = lj_args.replace("{" + x + "}", self.process_info[x])

                    for x in cfg_spark:
                        lj_args = lj_args.replace("{" + x + "}", str(cfg_spark[x]))

                    arg_list.append(lj_args)
                logger.info("Argument :{0}".format(arg_list))
                steps_involved[i]["HadoopJarStep"]["Args"] = arg_list

            logger.info(steps_involved)

            action = connection.add_job_flow_steps(JobFlowId=cluster_id['JobFlowId'],
                                                   Steps=steps_involved)
            print("Added step: %s" % action)

            # Start the loop to watch the cluster status
            lb_failure = False
            error_message = ""
            ls_errors = ["TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"]
            while not lb_failure:
                time.sleep(240)
                response = connection.describe_cluster(ClusterId=cluster_id['JobFlowId'])
                ld_clstr_status = response['Cluster']['Status']
                logger.info("Cluster State is {0}".format(ld_clstr_status))
                if ld_clstr_status['State'] in ('WAITING', 'RUNNING'):
                    logger.info("Cluster is Running the Jobs/Ready to use")
                    break
                elif ld_clstr_status['State'] in ls_errors:
                    if ld_clstr_status['StateChangeReason']['Code'] == "USER_REQUEST":
                        logger.error("EMR Cluster Response\n {0}".format(response['Cluster']))
                        lb_failure = True
                        error_message = "EMR Cluster Terminated by the User"
                        break
                    elif ld_clstr_status['StateChangeReason']['Code'] == "ALL_STEPS_COMPLETED":
                        logger.error("EMR Cluster Response\n {0}".format(response['Cluster']))
                        lb_failure = True
                        error_message = "Cluster has no Jobs and reporting as All Steps completed."
                        break
                    else:
                        logger.error("EMR Cluster Response\n {0}".format(response['Cluster']))
                        lb_failure = True
                        error_message = "Error while Creating EMR Cluster"
                        break

            if lb_failure:
                raise Exception(error_message)

            # Monitor the Steps Status
            step_response = connection.list_steps(ClusterId=cluster_id['JobFlowId'])
            ln_tot_step = len(step_response['Steps'])
            while ln_tot_step:
                ld_step_status = step_response['Steps'][ln_tot_step - 1]
                if ld_step_status['Status']['State'] == 'COMPLETED':
                    ln_tot_step = ln_tot_step - 1
                    logger.info(ld_step_status['Name'] + " is Complete")
                elif ld_step_status['Status']['State'] in ('FAILED', 'CANCELLED'):
                    ln_tot_step = ln_tot_step - 1
                    logger.error(ld_step_status['Name'] + " is Failed")
                    # Response = connection.terminate_job_flows(JobFlowIds=[cluster_id['JobFlowId']])
                    raise AirflowException("One or many EMR Steps Failed")
                elif ld_step_status['Status']['State'] == 'CANCELLED':
                    ln_tot_step = ln_tot_step - 1
                    logger.error(ld_step_status['Name'] + " is Cancelled")
                    # Response = connection.terminate_job_flows(JobFlowIds=[cluster_id['JobFlowId']])
                    return AirflowException("One or many EMR Steps Cancelled")
                else:
                    time.sleep(120)
                    step_response = connection.list_steps(ClusterId=cluster_id['JobFlowId'])
                    # Response = connection.terminate_job_flows(JobFlowIds=[cluster_id['JobFlowId']])

        except Exception as e:
            logger.error("Exception: {0}".format(e))
            raise
