PART 1:
Instructions to run code:

For creating directory in Hadoop use following command:
(hdfs dfs -mkdir /user/userid/directoryname
- hdfs dfs –mkdir /user/ssamine/covid_olympics_dt (for covid and Olympics keyword data)

For collecting the data from twitter using Flume use the following command:
(nohup $FLUME_HOME/bin/flume/ -ng agent -n TwitterAfent -f /home/userid/directoryname.conf --conf /usr/local/flume/conf &
- nohup $FLUME_HOME/bin/flume/-ng agent –n TwitterAgent -f /home/ssamine/covid_olympics.conf –-conf /usr/local/flume/conf &

To check if the data is downloading and the downloaded data size, use the following command:
(hdfs dfs -du -s -h /user/userid/directoryname
- hdfs dfs –du –s –h /user/ssamine/covid_olympics_dt

To stop the nohup processes after collecting enough data use the following command:
- kill -9 processeid
(Used the command to kill all the processes running for my id : 'pkill -KILL -u ssamine -f flume', As the data download didn't stop after killing the process id) 

To check on the downloaded data files:
(hdfs dfs -ls /user/userid/directoryname/year/month/date/time
- hdfs dfs -ls /user/ssamine/covid_olympics_dt/2022/03/06/23 ( year/month/date/time)

To view the content of a file in HDFS that stores the tweets:
(hdfs dfs -cat /user/ssamine/directoryname/year/month/date/time/FlumeData.Recordid)
- hdfs dfs -cat /user/ssamine/covid_olympics_dt/2022/03/06/23/FlumeData.1646630307663 (a record id that was downloaded)
