#!/bin/sh

source /data/scripts/mdtrequest_dbpurge/db_auth.cfg

MYSQL_USER=$(eval echo ${user})
MYSQL_PASSWD=$(eval echo ${pwd})
MYSQL_HOST=$(eval echo ${host})
MYSQL_PORT=$(eval echo ${port})
DB_NAME=$(eval echo ${db_name})
DATA_SET="1000000"
table_name="mdtrequestmessage"
i=1

LOG_DIR="/data/scripts/mdtrequest_dbpurge"
LOG_FILE="${LOG_DIR}/purge.log"

logger()
{
echo "[`date +'%Y/%m/%d - %H:%M:%S'`] -  $*" >> ${LOG_FILE}
}

#get first table entry
sql_statement="select messegeId,reportedtime from ${table_name} order by messegeId asc limit 1"

#exact last month date will be in human readable
lastmonth=$(date -d"$date -31 days")

#convert human readable to digit format
#lastm=$(echo "$lastmonth" | awk '{print "date -d\""$1FS$2FS$3"\" +%Y%m%d"}' | bash)
##last month incorrect date issue fix
lastm=$(date -d "1 month ago" +'%Y%m%d')


logger "run sql statement ...$sql_statement"
#grep unixtime
value1=$(mysql -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWD} -e "use $DB_NAME; ${sql_statement};")
rc=$?

logger "[execSQL] - Output : ${value1}"

if [ ${rc} -ne 0 ]; then
      logger "[execSQL] - Not executed the SQL ${sql_statement} in ${table_name}"
else
      logger "[execSQL] - Successfully executed the SQL ${sql_statement} in ${table_name}"
fi

id=$(echo $value1 | cut -d " " -f3)
grepdate=$(echo $value1 | cut -d " " -f4)

request=$(date -d @$(($grepdate/1000)) +%Y%m%d)

#echo "current date is $current_date"
logger "first table data created date is $request"
logger "last month date  is $lastm"
logger "first id is $id"

#sleep after select query
logger "sleep for 30 mins after first id select query"
sleep 1800
#find new range in which the data needs to be deleted
findrange()
{
  while [ $i -le 5 ]
  do
    logger "entered into findrange for cycle $i"
    logger "cycle $i - id value is $id"
    new_id[i]=$(($id + $DATA_SET))
    logger "new id value of cycle $i is  ${new_id[$i]}"

#grep table content of messageid with new_id
    sql_statement[i]="select messegeId,reportedtime from ${table_name} where messegeId=${new_id[$i]}"
    logger "sql statement $i is ${sql_statement[$i]}"
    logger "[execSQL] statement $i ...${sql_statement[$i]}"
    value2=$(mysql -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWD} -e "use $DB_NAME; ${sql_statement[$i]};")
    rc=$?

    logger "[execSQL] - Output : ${value2}"

    if [ ${rc} -ne 0 ]; then
          logger "[execSQL] - Not executed the SQL ${sql_statement[$i]} in ${table_name}"
    else
          logger "[execSQL] - Successfully executed the SQL ${sql_statement[$i]} in ${table_name}"
    fi
#sleep after query execution
    logger "sleep for 30 mins after cycle $i select query"
    sleep 1800

#check the above ran statement has true value
    if [ -n "$value2" ]
    then
      #grep $ith cycle date
          grepdate2=$(echo $value2 | cut -d " " -f4)

          request2=$(date -d @$(($grepdate2/1000)) +%Y%m%d)

          logger "cycle $i created date is $request2"
          logger "last month date is $lastm"


          if [ $request2 -lt $lastm ]
          then
              logger 'range table data is greater than 31 days,need to delete'
              delete_statement="delete from ${table_name} where messegeId between $id and ${new_id[$i]}"
              logger "cycle $i  delete query to be executed is $delete_statement"
              output=$(mysql -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWD} -e "use $DB_NAME; ${delete_statement};")
              rc=$?

              logger "[execSQL] - Output : ${output}"

              if [ ${rc} -ne 0 ]; then
                  logger "[execSQL] - Not executed the SQL ${delete_statement} in ${table_name}"
              else
                  logger "[execSQL] - Successfully executed the SQL ${delete_statement} in ${table_name}"
              fi

              #sleep after delete query
              logger "sleep for 30 mins after cycle $i delete query"
              sleep 1800
              id=$((${new_id[$i]} + 1))
              i=$(($i + 1))
              findrange
          else
              logger "range table data is lesser than 31 days,no need to delete"
              exit
          fi

    else
          logger "no id in that range value $range"
          exit
    fi
  done

}


if [ $request -lt $lastm ]
then
  logger 'first table data is greater than 31 days,need to delete'
  findrange
else
  logger "first table data is lesser than 31 days,no need to delete"
fi
