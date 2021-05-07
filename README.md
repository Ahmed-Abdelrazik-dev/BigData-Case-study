# BigData-Case-study

Hello, everyone in this repo I will show u how to connect with Twitter API and get data from it then put it in Kafka topic and consume data from Kafka topic with Spark Structure streaming and write it in parquet file in HDFS and create a hive table to get this parquet file data then connect hive with PowerBI Desktop tool to make ur Dashboards and reports on data you get from Twitter. So let's start...

The first step to get data from Twitter to Kafka topic you should create a developer account on Twitter Platform once you get approved they will give you accesses tokens to use during your case study and you will find my script in a file called KafkaProducer.py 

1.	To run this script, you need to setup python environment 3:
    a.	python3.6 -m venv ./iti41
    b.	source iti41/bin/activate
    c.	pip install --upgrade pip
    d.	pip install confluent-kafka
    e.	pip install pyspark
    f.	pip install tweepy
    g.	pip install textblob
    h.	pip install findspark
    i.	pip install --force-reinstall pyspark==2.4.7

2.	After setup the environment  you need to run python KafkaProducer.py

The second step you will need to run Spark script to start streaming on Kafka topic and make your processing on data and write it on parquet file on HDFS 

1)	To run this script on windows, you need to add those three jars to your spark jars folder but you should check the compatible versions with your spark version:
    a)	spark-sql-kafka-0-10_2.11-2.4.7.jar
    b)	kafka-clients-2.4.0.jar
    c)	spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar

2)	To run this script on Hortonworks machine you will need to enter your python environment by writing 
    a)	source (path of your environment)
    b)	then you need to run spark by:
        i)	spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 KafkaSparkConsumer.py
        (you need to take care as you put your packages to not write .jar I will throw Exception and write the version with the same style I write it in my command)
        
The third step is to put parquet files in HDFS you will find spark streaming do that directly for you in writeStream function you will just need to give it a path where you want those parquet files to put 

The fourth step now you need to create an External table in hive and give it location of the parquet files path in HDFS to load its data automatically to create hive table writes:
    CREATE EXTERNAL TABLE case study.tweets (User_ID string , Tweet string , Created_time string , Followers_count string , Location string , Favorite_count string , Retweet_count string ,Message  string ) stored as parquet  LOCATION 'hdfs:///CaseStudy/OutputStream';
    (Make sure to write columns name with the same name of columns in spark DF if you change anything in the name it will bring NULL values to this column)

The fifth step is to connect your hive table with PowerBI in this case you will need to download ClouderaHiveODBC64.msi and install it then open it and create your connection with PowerBI.
