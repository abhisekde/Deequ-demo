name:= "DataQuality"

version:= "1.0"

scalaVersion:= "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "2.3.0", "com.amazon.deequ" % "deequ" % "1.0.2", "org.json4s" % "json4s-native_2.12" % "3.7.0-M4")
