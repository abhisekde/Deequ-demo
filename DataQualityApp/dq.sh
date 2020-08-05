#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export HIVE_HOME=/opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/hive
export HADOOP_COMMON_HOME=/opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/hadoop
kinit -k -t /home/centos/impala.keytab impala
#sbt package
spark2-submit --class "com.teliacompany.datamall.DataQualityApp" --master yarn --conf spark.ui.port=4444 ./target/scala-2.11/dataquality_2.11-1.0.jar ${*} > spark.out 
