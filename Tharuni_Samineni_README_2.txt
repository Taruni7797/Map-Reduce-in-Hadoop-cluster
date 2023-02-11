Part 2:

Instructions to run the code:

Created an input directory using the following command:
(hdfs dfs -mkdir /user/userid/directoryname
- hdfs dfs -mkdir /user/ssamine/covid_olympics_dt

To move the files from directory(covid_olympics_dt) to a new hadoop input direcotory(inputdata) to reduce the number of sub-directories path use the following command:
(hdfs dfs -cp <source path> <destination path>
- hdfs dfs -cp /user/ssamine/covid_olympics_dt/ /user/ssamine/covid_olympics_ipt
(Used another directory as the first directory was using each of the month date and year as sub-directories)
- hdfs dfs -cp /user/ssamine/covid_olympics_ipt/2022/03/06/23/* /user/ssamine/inpudata

To view the data present in the directory use the following command:
(hdfs dfs -ls /user/ssamine/inputdirectory)
- hdfs dfs -ls /user/ssamine/inputdata

To compile the (Tharuni_Samineni_Program_2.java)mapreduce program for line count use the following command:
($HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main ProgramName.java)
- $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Tharuni_Samineni_Program_2.java

To create a jar file use the following command:
(jar cf jarfilename.jar ProgramName*.class)
- jar cf rc.jar Tharuni_Samineni_Program_2*.class

To run the mapreduce program use the following command:
(hadoop jar jarfilename.jar ProgramName /user/userid/inputdirectory /user/userid/outputpath)
- hadoop jar rc.jar Tharuni_Samineni_Program_2 /user/ssamine/inputdata /user/ssamine/testop
(Use different directories for output, otherwise an error will be shown that directory already exist)

To check the output of the program use the command:
(hdfs dfs -cat /user/userid/outputpath/part-r-00000)
- hdfs dfs -cat /user/ssamine/testop/part-r-00000




