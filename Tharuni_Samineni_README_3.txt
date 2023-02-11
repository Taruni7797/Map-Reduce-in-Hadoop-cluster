Part 3:

Instructions to run the code:

To compile the (Tharuni_Samineni_Program_2.java)map reduce partition program to Partition the tweets based on the hashtag and count the 
number of rows in each partiton based on the keywords ‘Covid’ and ‘Olympics’ use the following command:
($HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main ProgramName.java)
- $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Tharuni_Samineni_Program_3.java

To create a jar file use the following command:
(jar cf jarfilename.jar ProgramName*.class)
- jar cf tc.jar Tharuni_Samineni_Program_3*.class

To run the map reduce partition program use the following command:
(hadoop jar jarfilename.jar ProgramName /user/userid/inputdirectory user/userid/Outputpath)
- hadoop jar tc.jar Tharuni_Samineni_Program_2 /user/ssamine/inputdata /user/ssamine/test
(Use different directories for output, otherwise an error will be shown that directory already exist)

To check the output of the program use the command:
(hdfs dfs -cat /user/userid/outputpath/part-r-00000 (reducervalue))
- hdfs dfs -cat /user/ssamine/test/part-r-00000
- hdfs dfs -cat /user/ssamine/test/part-r-00001




