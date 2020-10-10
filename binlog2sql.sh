#!/bin/bash

mysql_home=$1
binlog_file=$2
start_pos=$3
stop_pos=$4
if [ "$start_pos" != "" ]; then
	start_pos="--start-position=$start_pos"
fi
if [ "$stop_pos" != "" ]; then
        stop_pos="--stop-position=$stop_pos"
fi

if [ "${mysql_home}X" == "X" ]; then
	echo "usage: $0 mysql_home binlog_file"
	exit 0
fi

tmp_dir=/tmp

mysql_host=127.0.0.1
mysql_user=root
mysql_password=baikai#1234
mysql_port=3306

tmp_table_names=${tmp_dir}/.mysql_binlog_parser_table_names
tmp_binlog_output_file=${tmp_dir}/.mysql_binlog_parser_output

prefix=mysql_binlog_parser
export_env_file=${tmp_dir}/.mysql_binlog_parser_env
reset_env_file=${tmp_dir}/.mysql_binlog_parser_env2
table_columns_file=${tmp_dir}/.table_columns_file

$mysql_home/bin/mysqlbinlog $binlog_file -vv $start_pos $stop_pos > $tmp_binlog_output_file

grep -E '^### INSERT INTO|^### DELETE' $tmp_binlog_output_file | sed 's/### INSERT INTO //' | sed 's/### DELETE FROM //' | sed 's/`//g' | sort | uniq > $tmp_table_names

for table_name in `cat $tmp_table_names`
do
	schema_name=`echo $table_name | awk -F '.' '{print $1}'`
	schema_table_name=`echo $table_name | awk -F '.' '{print $2}'`
	
	$mysql_home/bin/mysql -h$mysql_host -u$mysql_user -p$mysql_password -P$mysql_port --skip-column-names -e "select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='$schema_name' and table_name = '$schema_table_name' order by ORDINAL_POSITION asc" > $table_columns_file

	column_num=1
	for column_name in `cat $table_columns_file`
	do
		v_name="${prefix}_schema_${schema_name}_tab_${schema_table_name}_${column_num}"
		echo "export $v_name=$column_name" >> $export_env_file
		echo "unset $v_name" >> $reset_env_file
		column_num=`expr $column_num + 1`
	done
	
done

if [ ! -f $export_env_file ]; then
	exit 0
fi
source $export_env_file

grep '###' $tmp_binlog_output_file | sed 's/^###//' | awk '
BEGIN{
	disable_comment=0
	count = 0
	table_name=""
	schema_name=""
	is_update=0
	is_delete=0
	is_insert=0
	is_where=0
	conditions=""
}
{
	if($1 == "SET")
	{
		printf $0
		is_where = 0
	}
	if($1 == "WHERE")
	{	
		if(is_update == 0){
			printf $0
		}
		is_where = 1
	}
	else if($1 == "INSERT" && $2 == "INTO")
	{
		if(count > 0){
			if(is_update == 1 && length(conditions) > 0){
                                printf " WHERE "conditions
				conditions = ""
                        }
			print ";"
		}
		printf $0
		
		i1 = index($3, ".")
		
		schema_name = substr($3, 2, i1 - 1 - 2)
		table_name = substr($3, i1 + 2)
		table_name = substr(table_name, 0, index(table_name, "`") - 1)
		is_insert = 1
	}
	else if($1 == "DELETE" && $2 == "FROM")
	{
		if(count > 0){
			if(is_update == 1 && length(conditions) > 0){
                                printf " WHERE "conditions
				conditions = ""
                        }
                        print ";"
                }	
		printf $0

		i1 = index($3, ".")

		schema_name = substr($3, 2, i1 - 1 - 2)
                table_name = substr($3, i1 + 2)
                table_name = substr(table_name, 0, index(table_name, "`") - 1)
		is_delete = 1
	} 
	else if($1 == "UPDATE")
	{
		if(count > 0){
			if(is_update == 1 && length(conditions) > 0){
				printf " WHERE "conditions
				conditions = ""
			}
                        print ";"
                }
		printf $0
		i1 = index($2, ".")

                schema_name = substr($2, 2, i1 - 1 - 2)
                table_name = substr($2, i1 + 2)
                table_name = substr(table_name, 0, index(table_name, "`") - 1)
		is_update = 1
	}

	if($1 ~ /^@[0-9]/ && $1 !~ /^@1/)
	{
		if(is_where == 0){
			printf " ,"
		} else {
			if(is_update == 1){
				conditions = conditions" AND"			
			} else {
				printf " AND"
			}
		}
	}

	if($1 ~ /^@[0-9]/)
	{

		i2 = index($0, "=")
		i1 = index($0, "@")

		seq = substr($0, i1 + 1, i2 - i1 - 1)

		if(disable_comment == 1){
			value = substr($0, i2)
		} else {
			value = substr($0, i2)
		}

		flag = 0
		for(item in ENVIRON){
			# mysql_binlog_parser_schema_sakila_tab_actor_4
			if(item == "mysql_binlog_parser_schema_"schema_name"_tab_"table_name"_"seq){
				if(is_where == 1 && is_update == 1){
                                	conditions = conditions" "ENVIRON[item]   
                        	} else {
                                	printf " "ENVIRON[item]
                        	}
				flag = 1
				break
			}
		}	

		if(flag == 0){
			printf "=============ERROR COLUMN NAME===========";
		}


		if($0 ~ "TIMESTAMP.* meta="){
			if(is_where == 1 && is_update == 1){
				conditions = conditions"=FROM_UNIXTIME("substr(value, 2, index(value, " ") - 2)") "
			} else {
				printf "=FROM_UNIXTIME("substr(value, 2, index(value, " ") - 2)") "		
			}
		}
		else {
			if(is_where == 1 && is_update == 1){
				conditions = conditions""value" "
			} else {
				printf value" "
			}
		}
	}
	count += 1

}
END{
	if(is_update == 1 && length(conditions) > 0){
		printf " WHERE "conditions
		conditions = ""
	}
	print ";"
}
'


## reset
rm -rf $tmp_table_names
rm -rf $export_env_file
source $reset_env_file
rm -rf $reset_env_file
rm -rf $tmp_binlog_output_file
