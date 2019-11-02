hadoop fs -rm -r output/$1_out
hadoop fs -rm -r input/$1

hadoop fs -mkdir input/$1
hadoop fs -put ../$2 input/$1/
hadoop jar /usr/local/hadoop-2.9.2/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar -D mapred.reduce.tasks=5 -mapper "python /home/root/BigDataCourse/$1/python/$1_mapper.py" -reducer " python /home/root/BigDataCourse/$1/python/$1_reducer.py " -input input/$1/$2 -output output/$1_out

echo "Dumping result ..."

hadoop fs -getmerge output/$1_out $1_result.txt

