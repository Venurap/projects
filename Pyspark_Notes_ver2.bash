

Understanding sparkcontext

- Entrypoint into the world od spark
- An entrypoint is a way of connecting to a spark cluster
- You must have an entrypoint to run any pyspark jobs

Map function syntax:

map(function,list)

















CDP
=====================================================================================
GITHUB Login:  ivzp / Svtg80vp

CDP DEV login:
==============> 
aalhauap2g01.corp.aal.au 

pw for sys_hdp_gi_dev - Bbo@A910

c:\projects>
http://devci-adstash.corp.aal.au/stash/projects/DP/repos/wc/browse
git clone ssh://git@devci-adstash.corp.aal.au:7999/dp/wc.git


http://devci-adstash.corp.aal.au/stash/projects/DP/repos/idp_aif/browse
git clone ssh://git@devci-adstash.corp.aal.au:7999/dp/idp_aif.git

CDP Prod login:
=====================================================================================>
https://aalhapap2g01.corp.aal.au:8889/hue/editor/?type=hive

scheduler:
=====================================================================================>
https://aalhapap2n02.corp.aal.au:8090/cluster/scheduler

GIT:
=====================================================================================>
http://devci-adstash.corp.aal.au/stash/projects/DP/repos/idp_aif/browse


aalhapap2u01.corp.aal.au:

sys_hdp_gi_prod
DP9uC*mQ

sys_hdp_gi_uat 
mK!9S27R


sys_hdp_report_prod
AXnf24P7

pw for sys_hdp_gi_dev - Bbo@A910

general_insurance/common_hub/oozie/scripts/cop_env_variables.sh

http://devci-adstash.corp.aal.au/stash/projects/DP/repos/general_insurance/browse/common_hub/oozie/scripts/cop_env_variables.sh 

HDFS 
YARN - YET ANOTHER RESOURCE NEGOTIATOR
PIG  - Highlevel script langague like SQL to be used instead of java or python for MAP-REDUCE programs which goes through yarn and to HDFS
HIVE - Similar to PIG but like a SQL database
SPARK - Sit on top of YARN, same level as Map reduce, to run queries on your data
		Need programing like pyspark , handle streaming, sql, fast,machine learning
OOZIE - SCHEDULE AND RUN SPARK AND OTHER JOBS AND TASKS
SCOOP - Connector between hadoop and legacy databases ( like odbc) used in data ingestion
HUE - 	Query engine


PySpark Architecture
Apache Spark works in a master-slave architecture where the master is called “Driver” and slaves are called “Workers”. When you run a Spark application, 
Spark Driver creates a context that is an entry point to your application, and all operations (transformations and actions) are executed on worker nodes,
and the resources are managed by Cluster Manager.

Every Python module has it’s __name__ defined and if this is ‘__main__’, 
it implies that the module is being run standalone by the user and we can do corresponding appropriate actions.
If you import this script as a module in another script, the __name__ is set to the name of the script/module.
Python files can act as either reusable modules, or as standalone programs.
if __name__ == “main”: is used to execute some code only if the file was run directly, and not imported.

https://www.freecodecamp.org/news/if-name-main-python-example/

C:\Users\ivzp\AppData\Local\Programs\Python\Python39

https://intellij-support.jetbrains.com/hc/en-us/community/posts/360007576420-Error-while-finding-module-specification

Data science tasks
===================>
As a discipline that has come to prominence in the era of big data, data science is
about using data to tell stories. But before they can narrate the stories, data scientists
have to cleanse the data, explore it to discover patterns, and build models to predict
or suggest outcomes. Some of these tasks require knowledge of statistics, mathematics,
computer science, and programming.

directed acyclic graph (DAG);

https://sparkbyexamples.com/pyspark-tutorial/

DOCS
https://towardsdatascience.com/successful-spark-submits-for-python-projects-53012ca7405a

pw for sys_hdp_gi_dev - Bbo@A910

HADOOP Architecture
===================>

As noted in the previous chapter, Spark computations are expressed as operations.
These operations are then converted into low-level RDD-based bytecode as tasks,
which are distributed to Spark’s executors for execution.
Let’s look at a short example where we read in a text file as a DataFrame, show a sample
of the strings read, and count the total number of lines in the file. This simple
example illustrates the use of the high-level Structured APIs, which we will cover in
the next chapter. The show(10, false) operation on the DataFrame only displays the
first 10 lines without truncating; by default the truncate Boolean flag is true. Here’s
what this looks like in the Scala shell:


DEV
https://aalhauap2g01.corp.aal.au:8889/hue/editor?editor=341880

/data/shared/dev/gi/cdm_dl_db.password.jceks

strings = spark.read.text("/data/shared/dev/gi/cdm_dl_db.password.jceks")
strings.show(10, truncate=False)
strings.count()

strings = spark.read.text("/acms_share/dev/gi/feeds/external_data/acms_20210901/acms_claim_flag_history_daily_sr2_2021-08-31.csv")


/******************************************************************************* HADOOP NOTES *******************************************************************************/
Bitbucket Login:
ivzp / Svtg80vp

Oreilly

Winscp
/opt/sas/app/content/MarketingCustomerDatamart/csv
/opt/sas/app/config/Lev1/STM/BatchServer/Logs
/opt/sas/app/db/MarketingCustomerMart/staging/replica

/appl/gidw/dev
/appl/gidw/dev/work/ivzp
/appl/gidw/work/mmcustpoc

/appl/gidw/dev/programs/ivzp/SAS Programs
/home/CORP/ivzp/csv
/opt/wmqfte/scripts
/opt/sas/app/config/Lev1/STM/SASEnvironment/SASCode/Jobs
/opt/sas/app/config/Lev1/STM/BatchServer/Logs
/appl/gidw/test/programs/dw_programs
/appl/gidw/test/programs/macros
/appl/gidw/dev/programs/ivzp/SAS Programs/FI_Report/BA_Report/AGENT_OEM_DEALERSHIP
/appl/gidw/dev/programs/ivyb/WestpacCancellation Report/FRECC Regression
/opt/sas/app/db/MarketingCustomerMart/data/replica
/appl/gidw/test/data/replica/direc
/appl/gidw/dev/programs/macros
/appl/gidw/dev/programs/customer/Modelling
/opt/sas/app/db/MarketingCustomerMart/data/replica
/appl/gidw/dev/programs/mmcustpoc

AAL14863
AAL43153
AAL44022
-> AAL43308
AAL36855
AAL43271
					-> AAL39047



dwh 
password: bsmart01  

PC:
AAL35080 / AAL35288

aaldwcpdc016
Server:
 /home/cloudera/scripts/tst/python	
 ssh coludera@aaldwcpdc016
 coludera/ vapor5
 
GITHUB
=========================================
http://devci-adstash.corp.aal.au/stash/projects/DWHSAS/repos/customer_dm/browse/util

OOZIE
=========================================
http://aaldwcpdc016.corp.aal.au:8889/oozie/list_oozie_coordinators/ 

http://aaldwcpdc016.corp.aal.au:8889/hue/editor?editor=5732 

http://aaldwcpdc016.corp.aal.au:8889/oozie/list_oozie_coordinators/ 

http://aaldwcpdc016.corp.aal.au:8889/hue/editor?editor=5732 

http://aaldwcpdc016.corp.aal.au:8889/filebrowser/ 

Impala
=========================================
http://aaldwcpdc016.corp.aal.au:8889/notebook/editor?type=impala

GitHub - Pandu
========================================= 
http://devci-adstash.corp.aal.au/stash/projects/DWHSAS/repos/customer_dm/browse/util?at=refs%2Fheads%2Fpandu

http://devci-adstash.corp.aal.au/stash/projects/DWHSAS/repos/customer_dm/browse/util 

Ingestion script location:
=========================================
/local/app/cloudera/ingestion/

Ingestion script link:
=========================================

http://devci-adstash.corp.aal.au/stash/projects/DWHSAS/repos/customer_dm/browse/util

run_ingestion.sh

File:	

Python Script location in HDFS:
File Browser:

/home/cloudera/scripts
/user/scripts/dev/campaign_datamart/util
/home/cloudera/scripts/tst/util

source_type,source_name,application_name 

folder sas campaign_datamart

file sas campaign_datamart

sqoop adobe campaign_datamart 

sas_campaign_datamart_files_info_20180709.csv  

hdfs dfs -ls /data/landing/sas/campaign_datamart/sas_rnsent/load_dt=20180708

hdfs dfs -ls /user/scripts/dev/campaign_datamart/util/

Copy to local folder:
hdfs dfs -copyToLocal /user/scripts/dev/campaign_datamart/util/ /home/cloudera/scripts/tst/util

===========================================================
Impala script:

hadoop fs -ls /data/staging/sas/campaign_datamart/sas_riskind/ 

connect host

show tables.  

show databases;  
use database;
show tables; 
select * from table limit 100;  

===========================================================


https://www.tutorialspoint.com/pyspark/pyspark_quick_guide.htm 
Last two PySpark - MLlib and PySpark - Serializers are not required 

Two main concepts RDD and SparkContext 

	https://spark.apache.org/docs/1.6.0/ 
	we are using this version
	
Hadoop File browser:	
http://aaldwcpdc016.corp.aal.au:8889/filebrowser/ 	

hdfs dfs -put <localfilepath> <hdfsfilepath>

what are the parametes passed to the ingestion script 
=========================================
source_type,source_name,application_name 
folder sas campaign_datamart
file sas campaign_datamart
sqoop adobe campaign_datamart 
sas_campaign_datamart_files_info_20180709.csv  

hdfs dfs -ls /data/landing/sas/campaign_datamart/sas_rnsent/load_dt=20180708 
	
	
1. Create a directory in HDFS at given path(s).
Usage:
hadoop fs -mkdir <paths>
Example:
hadoop fs -mkdir /user/cloudera/ivzp

Upload:
hadoop fs -put /home/cloudera/scripts/tst/python/testfiles/risk_class_mart.csv /user/cloudera/ivzp/

hadoop fs -put /home/cloudera/scripts/tst/python/testfiles/CogsleyServices-SalesData-US-WithCommas* /user/cloudera/ivzp/

Download:
hadoop fs -get /user/saurzcode/dir3/Samplefile.txt /home/

See contents:
hadoop fs -cat /user/saurzcode/dir1/abc.txt

Copy a file from/To Local file system to HDFS:
hadoop fs -copyFromLocal /home/saurzcode/abc.txt  /user/saurzcode/abc.txt

Move file from source to destination:
hadoop fs -mv /user/saurzcode/dir1/abc.txt /user/saurzcode/dir2

Remove a file or directory in HDFS.
hadoop fs -rm /user/saurzcode/dir1/abc.txt

Display the aggregate length of a file
hadoop fs -du /user/saurzcode/dir1/abc.txt

Cloudera ingestion:
===================
/local/app/cloudera/ingestion

Hadoop command shell :
====================
https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html

•Overview
•appendToFile
•cat
•chgrp
•chmod
•chown
•copyFromLocal
•copyToLocal
•count
•cp
•du
•dus
•expunge
•get
•getfacl
•getmerge
•ls
•lsr
•mkdir
•moveFromLocal
•moveToLocal
•mv
•put
•rm
•rmr
•setfacl
•setrep
•stat
•tail
•test
•text
•touchz

Most of the commands in FS shell behave like corresponding Unix commands. Differences are described with each of the commands. Error information is sent to stderr and the output is sent to stdout.

test

Usage: hdfs dfs -test -[ezd] URI

Options:
•The -e option will check to see if the file exists, returning 0 if true.
•The -z option will check to see if the file is zero length, returning 0 if true.
•The -d option will check to see if the path is directory, returning 0 if true.

Example:
•hdfs dfs -test -e filename

hdfs location check if exists 0 returns:
/user/scripts/dev/campaign_datamart/

hdfs_code_base_path=/user/scripts/dev/campaign_datamart	

hdfs dfs -test -e /user/scripts/dev/campaign_datamart/tmp/_INGESTION_TRIG

/******************************************************************************* HADOOP NOTES *******************************************************************************/

Sqoop Tutorial
=====================================================================================================================================================================================================================================================
Sqoop is a tool designed to transfer data between Hadoop and relational database servers. It is used to 
import data from relational databases such as MySQL, Oracle to Hadoop HDFS, 
and export from Hadoop file system to relational databases. 
This is a brief tutorial that explains how to make use of Sqoop in Hadoop ecosystem.

source_type=${1^^}
source_name=$2
application_name=$3
landing_date=$4
code_base_path=/local/app/cloudera/ingestion
hdfs_landing_path=/data/landing/${source_name}/${application_name}
working_path=${code_base_path}/working
landing_db=db_campaign_landing
staging_db=db_campaign_staging

** All Hadoop config files are in location /etc/hadoop

JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera


Below are the list of scripts which gets executed sequentially
==============================================================

driver_run_ingestion.sh run_ingestion.sh : 
	Copy SAS CSV to data node(016) and then to Hadoop landing database db_campaign_landing

stage_full_load.py 
	Full load process from Hadoop landing database db_campaign_landing  to Hadoop staging database db_campaign_staging

stage_append_delete_load.py 
	Append and delete load process from Hadoop landing database db_campaign_landing  to Hadoop staging database db_campaign_staging

risk_consolidated_load.py  Consolidate all the risk files

rnsent_consolidated_load.py  Consolidate the response tables

optout_consolidated_load.py  Consolidate the optout files

customer_hub_master_load.py, customer_summary_load.py, customer_sat_load.py  Customer HUB and MART Tables load

policy_hub_master_load.py  Policy HUB Table load

risk_hub_master_load.py, risk_sat_load.py  Risk HUB and MART Tables load

contact_hub_master_load.py Contact HUB Table load

response_hub_master_load.py  Response HUB Table load

client_hub_master_load.py  Client HUB Table load

agent_hub_master.py  Agent HUB Table load




CDP
=====================================================================================
GITHUB Login:  ivzp / Svtg80vp

CDP DEV login:
==============> 
aalhauap2g01.corp.aal.au:

https://aalhauap2g01.corp.aal.au:8889/hue/editor/?type=hive
https://aalhauap2g02.corp.aal.au:8889/hue/editor?editor=73448&type=impala 
https://aalhauap2g01.corp.aal.au:8889/hue/editor?editor=78446&type=impala

http://devci-adstash.corp.aal.au/stash/projects/DWHSAS/repos/general_insurance/browse/cdp

/opt/app/sand_box/ipmc/cdp/general_insurance/cdp
/opt/app/sand_box/

/home/CORP/sys_hdp_cm_dev/util

CDP Prod login:
=====================================================================================>
https://aalhapap2g01.corp.aal.au:8889/hue/editor/?type=hive

aalhapap2u01.corp.aal.au:


CDP Documentation:
=====================================================================================>
This is for Tranprem https://wiki.corp.aal.au/display/CDM/Policy+premium+data+mapping
This is for MDM https://wiki.corp.aal.au/display/CDM/Client+Data+Mapping+for+Policy
These are the location for SAS https://wiki.corp.aal.au/display/CDM/CDP+-+SAS+files+scheduling+approach

This one has picture of SAS job https://wiki.corp.aal.au/display/CDM/CDP+Job+scheduling

Workflow name :- CDP_Sas_W_A 

CDP_Sas_W_A - Main workflow for Polisy source
  
  - Can see only d_ and q_ databases
  
  
  
General Insuarance Source Code:-
=====================================================================================>
http://devci-adstash.corp.aal.au/stash/projects/DP/repos/general_insurance/browse/common_hub/src?at=refs%2Fheads%2Fuat

Clone:-
ssh://git@devci-adstash.corp.aal.au:7999/dp/general_insurance.git

login:- ivzp / Svtg80vp

Data layers

Staging - current
Lake - History
Hub  - Latest version
Mart - Buisness related


GITHUB
Feature -> Development
		-> Release
		-> UAT
		-> Production
		-> Master

yaml -> DB2 target tables
dic  -> Source files





Policy table load from DB2:-
http://devci-adstash.corp.aal.au/stash/projects/DP/repos/general_insurance/browse/common_hub/src/policy?at=uat
mstr_policy.py

Transformation Functions call from functions created in /AIF/src/aif
http://devci-adstash.corp.aal.au/stash/projects/DP/repos/aif/browse/src/aif 


DB2 extracts 
=====================================================================================>
http://devci-adstash.corp.aal.au/stash/projects/DP/repos/aif/browse/conf


UAT
=====================================================================================

https://aalhapap2g01.corp.aal.au:8888/hue


select count(*) 
from u_gi_mrt_cm.policy_sat_vw p
LEFT JOIN u_gi_mrt_cm.risk_sat rsk 
ON (p.policy_key=rsk.policy_key)
WHERE p.policy_company IN ('1','6') 
AND rsk.risk_class IN ('HVP','HCC','HGP','HPT','DCP','DVP','DGP','DPV')
and policy_status in ('00','01','02','03','04') 
and p.policy_term_end_date > from_unixtime(unix_timestamp(now() ),'yyyyMMdd') ;---


Stage -> Lake -> Hub -> Mart

Here's a table showing Python objects and their equivalent conversion to JSON.

Python				JSON Equivalent


dict 				object 
list, tuple 		array 
str 				string 
int, float, int 	number 
True 				true 
False 				false 
None				null 

"""
Uses for docstering in defining a function


Web

An important step for any Spark driver application is to generate sparkContext. 
It allows your Spark application to access the Spark cluster with the help of the resource manager. The resource manager can be one of these three:

•SparkStandalone
•YARN
•Apache Mesos

Functions of sparkContext in Apache Spark

•Get the current status of your Spark application.
•Set configurations.
•Access various services.
•Cancel a job.
•Cancel a stage.
•Closure cleaning.
•Register SparkListener.
•Programmable dynamic allocation.
•Access persistent RDD.

Prior to Spark 2.0, sparkContext was used as a channel to access all Spark functionalities. 
The Spark driver program uses sparkContext to connect to the cluster through resource manager.

SparkConf is required to create sparkContext objects, which stores configuration parameters like appName (to identify your Spark driver), 
the core number, and the memory size of the executor running on a worker node.

In order to use SQL APIs, Hive, and streaming, separate contexst need to be created

val conf = new SparkConf()
setMaster("local")
setAppName("Spark Practice2")
val sc = new SparkContext(conf)

SparkSession – New entry-point of Spark

For streamin, we needed streamingContext. For SQL, sqlContext, and for Hive, 
hiveContext. But as DataSet and DataFrame APIs are becoming new standalone APIs, 
we need an entry point build for them. So in Spark 2.0, we have a new entry point build 
for DataSet and DataFrame APIs called as SparkSession.

It's a combination of SQLContext, HiveContext, and streamingContext. All the APIs available on those contexts are available on SparkSession; 
SparkSession also has a sparkContext for actual computation.

spark-sql-SessionState

Now we can look at how to create a SparkSession and interact with it.

/opt/apps/dev/cm/util 

hadoop fs -put /home/CORP/ivzp/tstpy/data/sas_mdm_aal_customer_tv_*.csv /data/warehouse/hadoop_practice.db/sas_mdm_aal_customer_tv/load_dt=20190808

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=true --conf spark.hadoop.fs.permissions.umask-mode=000 --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=ivzp --conf spark.ui.port=4057 --driver-memory 2g --num-executors 2 --executor-memory 2g --executor-cores 2  /home/CORP/ivzp/cdp/src/modules/tst1_2.py 

hadoop_practice,sas_mdm_aal_customer_tv

hive --hivevar landing_db=hadoop_practice --hivevar load_dt=20190808 -f /home/CORP/ivzp/tstpy/data/mdm_tv.hql

hdfs location:	/user/ivzp
unix location:	/home/CORP/ivzp

hive show databases;

describe database hadoop_practice;

describe FORMATTED hadoop_practice.sas_mdm_aal_customer_tv 

show partitions hadoop_practice.sas_mdm_aal_customer_tv 

hdfs://nameservice1/data/warehouse/hadoop_practice.db/sas_policy

hdfs://nameservice1/data/warehouse/hadoop_practice.db/sas_policy/load_dt=20190808

hadoop fs -put /home/CORP/ivzp/cdp/sas_policy_20190817_001.csv hdfs://nameservice1/data/warehouse/hadoop_practice.db/sas_policy/load_dt=20190808

https://aalhauap2g01.corp.aal.au:8889/hue/editor/?type=hive
https://aalhauap2g02.corp.aal.au:8889/hue/editor?editor=73448&type=impala

/opt/apps/dev/cm

'hdfs://nameservice1/data/warehouse/hadoop_practice.db/sas_mdm_aal_customer_tv'
hdfs://nameservice1/data/warehouse/hadoop_practice.db/sas_policy


Launching Spark on YARN

Yes, you can use the spark-submit to execute pyspark application or script. The spark-submit script in Spark’s installation bin directory is used to launch applications on a cluster.

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=true --conf spark.hadoop.fs.permissions.umask-mode=000 --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=ivzp --conf spark.ui.port=4057 --driver-memory 2g --num-executors 2 --executor-memory 2g --executor-cores 2  /home/CORP/ivzp/cdp/src/modules/__init__tst1.py 

spark-submit --master yarn --deploy-mode cluster /home/CORP/ivzp/cdp/src/modules/tst1_2.py 


./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
  
 --class: The entry point for your application (e.g. org.apache.spark.examples.SparkPi)
•--master: The master URL for the cluster (e.g. spark://23.195.26.187:7077)
•--deploy-mode: Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client) (default: client)  
•--conf: Arbitrary Spark configuration property in key=value format. For values that contain spaces wrap “key=value” in quotes (as shown).
•application-jar: Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an hdfs:// path or a file:// path that is present on all nodes.
•application-arguments: Arguments passed to the main method of your main class, if any

https://www.youtube.com/watch?v=KqaPMCMHH4g
============================================================================================================================================================================================================= 
=============================================================================================================================================================================================================
=============================================================================================================================================================================================================
=============================================================================================================================================================================================================
=============================================================================================================================================================================================================
Structuring our Jobs Repository

First, let’s go over how submitting a job to PySpark works:
spark-submit --py-files pyfile.py,zipfile.zip main.py --arg1 val1


spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files "/home/CORP/ivzp/cdp/conf/dp_client_prep_columns.dic,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_natural.json,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_legal.json,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_household.json,/home/CORP/ivzp/cdp/src/platinum_run.py,/home/CORP/ivzp/cdp/conf/dp_client_prep_2.yaml"  --driver-memory 4g --num-executors 4 --executor-memory 15g --executor-cores 4 /home/CORP/jrhm/source_code/cdp_new/src/platinum_run_yaml.py --conf_file dp_client_prep_2.yaml --type_of_load prep --env DEV 


spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files "/home/CORP/ivzp/cdp/conf/dp_client_prep_columns.dic,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_natural.json,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_legal.json,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_household.json,/home/CORP/ivzp/cdp/src/platinum_run.py,/home/CORP/ivzp/cdp/conf/dp_client_prep_2.yaml"  --driver-memory 4g --num-executors 4 --executor-memory 15g --executor-cores 4 /home/CORP/ivzp/cdp/src/platinum_run_yaml.py --conf_file dp_client_prep_2.yaml --type_of_load prep --env DEV


create table hadoop_practice.sas_policy_tmp like hadoop_practice.sas_mdm_aal_customer_tv

/home/CORP/ivzp/cdp > zip -r src.zip src/

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files 
"/home/CORP/ivzp/cdp/conf/dp_client_prep_columns.dic,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_natural.json,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_legal.json,/home/CORP/ivzp/cdp/conf/dp_client_platinum_rules_household.json,/home/CORP/ivzp/cdp/src/platinum_run_yaml.py,/home/CORP/ivzp/cdp/conf/dp_client_prep_2.yaml"  --driver-memory 4g --num-executors 4 --executor-memory 15g --executor-cores 4 /home/CORP/ivzp/cdp/src/platinum_run_yaml.py --conf_file dp_client_prep_2.yaml --type_of_load prep --env DEV

spark.sql("""CREATE EXTERNAL TABLE hadoop_practice.dp_client_prep( 
CLIENT_KEY STRING 
,MDM_ENTITY_CLUSTER_ID STRING 
,DF_SRC_HOUSEHOLD_VALID STRING
,DF_SRC_NATURAL_VALID STRING 
,DF_SRC_LEGAL_VALID STRING
,DF_SRC_TITLE_STND STRING 
,DF_SRC_FIRST_NAME_STND STRING 
,DF_SRC_MIDDLE_NAME_STND STRING 
,DF_SRC_LAST_NAME_STND STRING 
,DF_SRC_FULL_NAME_STND STRING 
,DF_GENDER_STND STRING 
,DF_SRC_DOB_STND STRING 
,DF_SRC_EMAIL_STND STRING 
,DF_SRC_MOBILE_PHONE_FULL_STND STRING 
,DF_SRC_HOME_PHONE_FULL_STND STRING 
,DF_SRC_WORK_PHONE_FULL_STND STRING 
,DF_SRC_COMPANY_NAME_STND STRING 
,DF_SRC_ABN_STND STRING 
,DF_FULL_ADDRESS_STND STRING 
,H_DPID STRING 
,H_POSTCODE STRING
,H_DPID_IND STRING
,H_DPID_CHK STRING
,DF_SRC_LAST_NAME_IND STRING
,DF_SRC_LAST_NAME_CHK STRING
,DF_SRC_DOB_IND STRING
,DF_SRC_DOB_CHK STRING
,DF_SRC_EMAIL_IND STRING 
,DF_SRC_EMAIL_CHK  STRING 
,DF_SRC_MOBILE_PHONE_FULL_IND STRING 
,DF_SRC_MOBILE_PHONE_FULL_CHK STRING
,ACTIVE_DT TIMESTAMP)
stored as parquet
LOCATION '/data/warehouse/hadoop_practice.db/dp_client_prep/'""")


LOAD DATA INPATH '/data/warehouse/hadoop_practice.db/sas_policy/load_dt=20190808' INTO TABLE hadoop_practice.sas_policy PARTITION (load_dt=20190808);

select * from u_tmp.sas_policy
where pol_key1 not in (select policy_key from u_gi_mrt_cm.policy_sat_vw)


-- select count(*) from u_gi_mrt_cm.policy_sat_vw
-- where policy_key not in (select pol_key1 from u_tmp.sas_policy)


select * from u_tmp.sas_risks
where pol_key1 not in (select policy_key from u_gi_mrt_cm.risk_sat)

select * from u_tmp.sas_policy
where pol_key1 not in (select policy_key from u_gi_mrt_cm.policy_sat_vw)


-- select count(*) from u_gi_mrt_cm.policy_sat_vw
-- where policy_key not in (select pol_key1 from u_tmp.sas_policy)


select * from u_tmp.sas_risks
where pol_key1 not in (select policy_key from u_gi_mrt_cm.risk_sat)

-- describe formatted u_tmp.sas_risks

INSERT OVERWRITE DIRECTORY '/user/data/output/test' select * from u_tmp.sas_risks
where pol_key1 not in (select policy_key from u_gi_mrt_cm.risk_sat)

UAT DATA CHECK

select count(*) 
from u_gi_mrt_cm.policy_sat_vw p
LEFT JOIN u_gi_mrt_cm.risk_sat rsk 
ON (p.policy_key=rsk.policy_key)
WHERE p.policy_company IN ('1','6') 
AND rsk.risk_class IN ('HVP','HCC','HGP','HPT','DCP','DVP','DGP','DPV')
and policy_status in ('00','01','02','03','04') 
and p.policy_term_end_date > from_unixtime(unix_timestamp(now() ),'yyyyMMdd') 

describe FORMATTED  u_gi_mrt_cm.policy_sat_vw


 1752190 


select count(*) from sas_policy
where pol_stat in ('00','01','02','03','04') 
and hdr_expr > from_unixtime(unix_timestamp(now() ),'yyyyMMdd') 
and substr(policy_key,1,1) in( '1','6')

select count(*) 
from u_gi_mrt_cm.policy_sat_vw p
WHERE p.policy_company IN ('1','6') 
AND  policy_status in ('00','01','02','03','04') 
and p.policy_term_end_date > from_unixtime(unix_timestamp(now() ),'yyyyMMdd')

select count(*) from sas_policy
where pol_stat in ('00','01','02','03','04') 
and hdr_expr > from_unixtime(unix_timestamp(now() ),'yyyyMMdd') 
and substr(pol_key1,1,1) in( '1','6')
and  rec_ind='Y'

659 444

select count(*) 
from u_gi_mrt_cm.policy_sat_vw p
LEFT JOIN u_gi_mrt_cm.risk_sat rsk 
ON (p.policy_key=rsk.policy_key)
WHERE p.policy_company IN ('1','6') 
AND rsk.risk_class IN ('HVP','HCC','HGP','HPT','DCP','DVP','DGP','DPV')
and policy_status in ('00','01','02','03','04') 
and p.policy_term_end_date > from_unixtime(unix_timestamp(now() ),'yyyyMMdd') 

5 328 751


select count(*) 
from u_gi_mrt_cm.policy_sat_vw p
WHERE p.policy_company IN ('1','6') 
and policy_status in ('00','01','02','03','04') 
and p.policy_term_end_date > from_unixtime(unix_timestamp(now() ),'yyyyMMdd') 

invalidate metadata risk_sat


/* 3,895,585*/
--  3,474,016

select  count(*) from risk_sat
where risk_class IN ('HVP','HCC','HGP','HPT','DCP','DVP','DGP','DPV');

/* from staging it will go to lak and to then to hub and mart in that order right	*/

select count(*) from u_gi_stg_sas.sas_policy

/*** HDFS Commands ****/

hdfs dfs -ls /data/dev/gi/staging/retention/land/sas/Mdm_aal_customer_tv_20200221.csv

hdfs dfs -ls /data/dev/gi/staging/retention/land/sas/Mdm_aal_customer_tv*

hdfs dfs -ls /data/dev/gi/staging/retention/land/d_gi_land_sas/tranprem_delta*

hdfs dfs -ls /data/qa/gi/staging/retention/land/q_gi_land_sas/tranprem_delta*

hdfs dfs -ls /data/uat/gi/staging/retention/land/u_gi_land_sas/control/*
 
 

/*** File transfer Commands ****/

rsubmit;
x curl -i -k -X PUT -L "https://aalhauap2u01.corp.aal.au:14000/webhdfs/v1/data/&env./gi/staging/retention/land/sas/Mdm_aal_customer_tv_&rundate..csv?op=CREATE&user.name=hdfs" --header "Content-Type:application/octet-stream" -T "/appl/gidw/test/hadoop/csv/Mdm_aal_customer_tv_&rundate..csv";
endrsubmit;


staging -> lake -> hub -> mart

x ;

curl -i -k -X PUT -L "https://aalhauap2u01.corp.aal.au:14000/webhdfs/v1/data/dev/gi/staging/retention/land/d_gi_land_sas/tranprem/tranprem_delta_20200522.csv?op=CREATE&user.name=sys_hdp_nprod_sas" --header "Content-Type:application/octet-stream" -T "/appl/gidw/test/hadoop/csv/tranprem_delta_20200522.csv"

curl -i -k -X PUT -L "https://aalhauap2u01.corp.aal.au:14000/webhdfs/v1/data/qa/gi/staging/retention/land/q_gi_land_sas/tranprem/tranprem_delta_20200522.csv?op=CREATE&user.name=sys_hdp_nprod_sas" --header "Content-Type:application/octet-stream" -T "/appl/gidw/test/hadoop/csv/tranprem_delta_20200522.csv";

 hdfs dfs -ls /data/dev/gi/staging/retention/land/d_gi_land_sas/tranprem/tranprem_delta*
 
  hdfs dfs -ls /data/qa/gi/staging/retention/land/q_gi_land_sas/tranprem/tranprem_delta*
  
  hdfs dfs -ls /data/uat/gi/staging/retention/land/u_gi_land_sas/control/*
  
  hdfs dfs -ls //data/uat/gi/staging/retention/land/u_gi_land_sas/control/sas_feed_success_20200610.csv
  
  hdfs dfs -ls //data/uat/gi/staging/retention/land/u_gi_land_sas/control/sas_feed_success_20200610.csv
  
  
/appl/gidw/dev/programs/macros/connect_nonprod_hadoop.sas

%let password =GtG+D627;
%let user =sys_hdp_nprod_sas;


/appl/gidw/dev/programs/macros/connect_prod_hadoop.sas
%let user ='sys_hdp_prod_sas';
      %let password ='B59wL!V2';
	  
ivzp / Svtg80vp
http://devci-adstash.corp.aal.au/stash/login

CDP Project code:

http://devci-adstash.corp.aal.au/stash/projects/DP/repos/general_insurance/browse/common_hub/src?at=refs%2Fheads%2Frelease%2Fuat1.1


http://devci-adstash.corp.aal.au/stash/projects/DP/repos/general_insurance/browse/common_hub/src?at=refs%2Fheads%2Frelease%2Fuat1.1

	  
it has both hive and impala
Hive and Impala are tools to perform SQL queries on data residing on HDFS/HBase. ... 
Hive uses HiveQL and converts data into MapReduce or Spark jobs that run on the Hadoop cluster. 
Impala uses a very fast specialized SQL engine faster than that of MapReduce.	


/home/CORP/jsng
P:\data\CDM\PYTHON

Parquet files
Back to glossary. Parquet is an open source file format available to any project in the 
Hadoop ecosystem. Apache Parquet is designed for efficient as well as performant 
flat columnar storage format of data compared to row based files like CSV or TSV files.



Spark SQL, DataFrames and Datasets Guide
https://spark.apache.org/docs/latest/sql-getting-started.html
Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, 
the interfaces provided by Spark SQL provide Spark with more information about the structure 
of both the data and the computation being performed. Internally, Spark SQL uses this extra 
information to perform extra optimizations. There are several ways to interact with Spark SQL 
including SQL and the Dataset API. When computing a result, the same execution engine is used,
 independent of which API/language you are using to express the computation. This unification
 means that developers can easily switch back and forth between different APIs based on which 
 provides the most natural way to express a given transformation.

All of the examples on this page use sample data included in the Spark distribution and can be run 
in the spark-shell, pyspark shell, or sparkR shell.

https://docs.python.org/3/howto/argparse.html

https://www.tutorialspoint.com/yaml/index.htm
https://zetcode.com/python/yaml/

20may2021==>

Stage -> Lake -> Hub -> Mart

Stage:
Temp layers
Lake:
All Data , including history

/opt/app/sand_box

/opt/app/sand_box

cd /opt/app/sand_box

zip -r src.zip src

cmd>
c:\projects>
http://devci-adstash.corp.aal.au/stash/projects/DP/repos/wc/browse
git clone ssh://git@devci-adstash.corp.aal.au:7999/dp/wc.git

https://aalhapap2n02.corp.aal.au:8090/cluster/scheduler

http://devci-adstash.corp.aal.au/stash/projects/DP/repos/idp_aif/browse
git clone ssh://git@devci-adstash.corp.aal.au:7999/dp/idp_aif.git

spark-submit 
	--master yarn 
	--deploy-mode cluster 
	--conf spark.dynamicAllocation.enabled=false 
	--py-files src.zip 
	--files "src/driver.py,conf/Mart/FIIQ/iq_mart.dic,conf/Mart/FIIQ/iq_mart.yaml" 
	--driver-memory 4g 
	--num-executors 3 
	--executor-memory 15g 
	--executor-cores 3 

src/driver.py 
	--conf_file iq_mart.yaml 
	--type_of_load mart_quote 
	--env DEV

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files "src/driver.py,conf/Mart/FIIQ/iq_mart.dic,conf/Mart/FIIQ/iq_mart.yaml" --driver-memory 4g --num-executors 3 --executor-memory 15g --executor-cores 3 src/driver.py --conf_file iq_mart.yaml --type_of_load mart_quote --env DEV


Sample File ingestion command -

sys_hdp_gi_dev / 
 
spark-submit 
	--master yarn 
	--deploy-mode cluster 
	--conf spark.dynamicAllocation.enabled=false 
	--conf spark.executor.memoryOverhead=4096 
	--conf spark.yarn.maxAppAttempts=0 
	--conf spark.driver.memoryOverhead=4096 
	--num-executors 4 
	--driver-memory 2g 
	--executor-memory 4g 
	--executor-cores 2 
	--conf spark.hadoop.fs.permissions.umask-mode=000 
	--conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=sys_hdp_gi_dev 
	--py-files src.zip --files "src/aif_run.py,conf/dev/gi/chr0001_84.csv,conf/dev/gi/connection_avaya.yaml,conf/dev/gi/avaya_header_info.dic" 
aif_run.py --env 'DEV' 
	--input_feed chr0001_84.csv 
	--columns avaya_header_info.dic 
	--conf_file connection_avaya.yaml 
	--mode yarn

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files "src/driver.py,conf/Mart/FIIQ/iq_mart.dic,conf/Mart/FIIQ/iq_mart.yaml" --driver-memory 4g --num-executors 3 --executor-memory 15g --executor-cores 3 src/driver.py --conf_file iq_mart.yaml --type_of_load mart_quote --env DEV
	
hdfs dfs -ls //data/dev/gi/storage/retention/aud/d_gi_aif_meta/aif_meta
hdfs dfs -ls /acms_share/dev/gi/feeds/external_data/avaya/echi

Unix location:
ls /acms_share/dev/gi/feeds/external_data/avaya/echi
chr0001_84.csv

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --conf spark.executor.memoryOverhead=4096 --conf spark.yarn.maxAppAttempts=0 --conf spark.driver.memoryOverhead=4096 --num-executors 4 --driver-memory 2g --executor-memory 4g --executor-cores 2 --conf spark.hadoop.fs.permissions.umask-mode=000 --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=sys_hdp_gi_dev --py-files src.zip --files "src/aif_run.py,conf/dev/gi/cop_inc_avaya.csv,conf/dev/gi/connection_avaya.yaml,conf/dev/gi/avaya_header_info.dic" aif_run.py --env 'DEV' --input_feed chr0001_84.csv --columns avaya_header_info.dic --conf_file connection_avaya.yaml --mode yarn

zip -r src.zip src
cop_full_avaya
 chr0001_84.csv
 
 
/* Orginal DB2 Command	*/

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --conf spark.executor.memoryOverhead=4096 --conf spark.yarn.maxAppAttempts=0 --conf spark.driver.memoryOverhead=2048 --num-executors 4 --driver-memory 3g --executor-memory 8g --executor-cores 2 --conf spark.hadoop.fs.permissions.umask-mode=000 --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=sys_hdp_gi_dev --driver-class-path /var/lib/sqoop/db2jcc4.jar --jars /var/lib/sqoop/db2jcc4.jar --conf spark.driver.extraClassPath=/var/lib/sqoop/db2jcc4.jar --conf spark.executor.extraClassPath=/var/lib/sqoop/db2jcc4.jar --py-files src.zip --files "src/aif_run.py,conf/dev/gi/cop_full_direct_polisy_stat_medium.csv,conf/dev/gi/connection_dwh.yaml" aif_run.py --env 'DEV' --input_feed cop_full_direct_polisy_stat_medium.csv --conf_file connection_dwh.yaml --mode yarn

/* Orginal DB2 Command splits */

spark-submit --master yarn 
	--deploy-mode cluster 
	--conf spark.dynamicAllocation.enabled=false 
	--conf spark.executor.memoryOverhead=4096 
	--conf spark.yarn.maxAppAttempts=0 
	--conf spark.driver.memoryOverhead=2048 
	--num-executors 4 
	--driver-memory 3g 
	--executor-memory 8g 
	--executor-cores 2 
	--conf spark.hadoop.fs.permissions.umask-mode=000 
	--conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=sys_hdp_gi_dev 
	--driver-class-path /var/lib/sqoop/db2jcc4.jar 
	--jars /var/lib/sqoop/db2jcc4.jar 
	--conf spark.driver.extraClassPath=/var/lib/sqoop/db2jcc4.jar 
	--conf spark.executor.extraClassPath=/var/lib/sqoop/db2jcc4.jar 
	--py-files src.zip 
	--files "src/aif_run.py,conf/dev/gi/cop_full_direct_polisy_stat_medium.csv,conf/dev/gi/connection_dwh.yaml" 
	aif_run.py 
	--env 'DEV' 
	--input_feed cop_full_direct_polisy_stat_medium.csv 
	--conf_file connection_dwh.yaml --mode yarn

/user/ivzp

curl -i -k -X PUT -L "https://aalhauap2u01.corp.aal.au:14000/webhdfs/v1/data/dev/gi/staging/retention/land/d_gi_land_sas/tranprem/tranprem_delta_20200522.csv?op=CREATE&user.name=sys_hdp_nprod_sas" --header "Content-Type:application/octet-stream" -T "/appl/gidw/test/hadoop/csv/tranprem_delta_20200522.csv"

 
hadoop fs -put -f *.txt /user/ivzp


hadoop jar /contrib/streaming/hadoop-*streaming*.jar -file /opt/app/sand_box/ivzp/tst/src/mapper.py -mapper /opt/app/sand_box/ivzp/tst/src/mapper.py -file /opt/app/sand_box/ivzp/tst/src/reducer.py -reducer /opt/app/sand_box/ivzp/tst/src/reducer.py -input /user/ivzp/* -output /user/ivzp/ 


${wf:actionData('shell-5cd3')['driver_src_path']}/cop_create_success_trig.sh
cop_servis
${sys_hdp_usr}


spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files "src/driver.py,conf/Mart/FIIQ/iq_mart.dic,conf/Mart/FIIQ/iq_mart.yaml" --driver-memory 4g --num-executors 3 --executor-memory 15g --executor-cores 3 src/driver.py --conf_file iq_mart.yaml --type_of_load mart_quote --env DEV

spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files "src/driver.py,conf/Mart/FIIQ/iq_mart.dic,conf/Mart/FIIQ/iq_mart.yaml" --driver-memory 4g --num-executors 3 --executor-memory 15g --executor-cores 3 src/driver.py --conf_file iq_mart.yaml --type_of_load mart_quote --env DEV




sample Hub command - 
spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --py-files src.zip --files "src/driver.py,conf/claim/claim_dev.yaml,conf/claim/hub_claim_repair_motor.dic" --driver-memory 2g --num-executors 2 --executor-memory 4g --executor-cores 2 src/driver.py --conf_file claim_dev.yaml --type_of_load hub_claim_repair_motor --env 'DEV'


Sample Ingestion AIF DB2 command -
spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --conf spark.executor.memoryOverhead=4096 --conf spark.yarn.maxAppAttempts=0 --conf spark.driver.memoryOverhead=2048 --num-executors 4 --driver-memory 3g --executor-memory 8g --executor-cores 2 --conf spark.hadoop.fs.permissions.umask-mode=000 --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=sys_hdp_gi_dev --driver-class-path /var/lib/sqoop/db2jcc4.jar --jars /var/lib/sqoop/db2jcc4.jar --conf spark.driver.extraClassPath=/var/lib/sqoop/db2jcc4.jar --conf spark.executor.extraClassPath=/var/lib/sqoop/db2jcc4.jar --py-files src.zip --files "src/aif_run.py,conf/dev/gi/cop_full_direct_polisy_stat_medium.csv,conf/dev/gi/connection_dwh.yaml" aif_run.py --env 'DEV' --input_feed cop_full_direct_polisy_stat_medium.csv --conf_file connection_dwh.yaml --mode yarn

Sample Ingestion AIF File Injecttion command -
spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --conf spark.executor.memoryOverhead=4096 --conf spark.yarn.maxAppAttempts=0 --conf spark.driver.memoryOverhead=4096 --num-executors 4 --driver-memory 2g --executor-memory 4g --executor-cores 2 --conf spark.hadoop.fs.permissions.umask-mode=000 --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=sys_hdp_gi_dev --py-files src.zip --files "src/aif_run.py,conf/dev/gi/cop_full_avaya.csv,conf/dev/gi/connection_avaya.yaml,conf/dev/gi/avaya_header_info.dic" aif_run.py --env 'DEV' --input_feed cop_full_avaya.csv --columns avaya_header_info.dic --conf_file connection_avaya.yaml --mode yarn

spark-submit --master yarn --deploy-mode cluster 
--conf spark.dynamicAllocation.enabled=false 
--conf spark.executor.memoryOverhead=4096 
--conf spark.yarn.maxAppAttempts=0 
--conf spark.driver.memoryOverhead=4096 
--num-executors 4 --driver-memory 2g --executor-memory 4g --executor-cores 2 
--conf spark.hadoop.fs.permissions.umask-mode=000 
--conf 
spark.yarn.appMasterEnv.HADOOP_USER_NAME=sys_hdp_gi_dev 
--py-files src.zip 
--files "src/aif_run.py,
		conf/dev/gi/cop_hist_avaya.csv,
		conf/dev/gi/connection_avaya.yaml,
		conf/dev/gi/avaya_header_info.dic" 
aif_run.py --env 'DEV' 
--input_feed cop_hist_avaya.csv 
--columns avaya_header_info.dic 
--conf_file connection_avaya.yaml 

--mode yarn








