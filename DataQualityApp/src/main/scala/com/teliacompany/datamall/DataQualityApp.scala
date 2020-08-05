package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{StandardDeviation, Compliance, Correlation, Size, 
                                   Completeness, Mean, ApproxCountDistinct, Maximum, 
                                   Minimum, Entropy, GroupingAnalyzer, Uniqueness}

import org.json4s.JsonDSL._

object DataQualityApp {
    def main(args: Array[String]) = {
        val spark = SparkSession.builder.appName("DataQualityApp").getOrCreate()

        // Validate command line arguments
        require(args.length == 2 && args(1).startsWith("/") && (args(0) == "--execute" || args(0) == "--check"), 
                "\nUsage: Requires 2 argument: \n - Execution mode: [--execute | --check]\n - <full path to parquet dataset> \nExample: \nspark2-submit --class \"com.teliacompany.datamall.DataQualityApp\" \\ \n --master yarn \\ \n --conf spark.ui.port=XXXX \\ \n /path/to/dataquality_xxxx.jar <execution_mode> <full path to parquet dataset>\n")
        
        val path        = args(1)
        // Output dataset 
        val p_items     = path.split("/")
        val pond        = "work" // p_items(p_items.indexOf("data") +1)
        val in_name     = p_items(p_items.lastIndexOf("data") -1)
        val project     = p_items(p_items.indexOf(in_name) -1)
        // val out_checks  = "/data/" + pond + "/checks_" + project + "_" + in_name
        // val out_metric  = "/data/" + pond + "/metric_" + project + "_" + in_name
        val out_checks  = "/data/" + pond + "/checks/"+ project + "/" + in_name
        val out_metric  = "/data/" + pond + "/metric/"+ project + "/" + in_name 

        val df = spark.read.option("basePath", path).parquet(path + "/*")
        val stage1 = suggest_constraints(in_name, df, spark)  // Get suggestions
        val stage2 = calc_metrics(in_name, df, stage1, spark) // Calulate daily metrices

        // Metrics calculation only
        if(args(0) == "--execute") {
            println("+++ Metrices Results: " + out_metric) 
            stage2.show(100)
            stage2.write
                .mode("append")
                .parquet(out_metric)
        }
        
        // Anomaly detection
        if(args(0) == "--check") {
            val metrics = spark.read.option("basePath", out_metric).parquet(out_metric + "/*") // historical record of metrices
            val stage3 = calc_thresholds(metrics) // Calculate boundaries of acceptable values
            val stage4 = anomaly_check(stage2, stage3) // Anomaly detection by comparing with historical metrices

            stage4.write
                .mode(SaveMode.Overwrite)
                .parquet(out_checks)
            stage2.write
                .mode("append")
                .parquet(out_metric)

            println("+++ Anomaly Check Results: " + out_checks)
            stage4.show(100)  
        }
        spark.stop() // exit
    }

    def suggest_constraints(name: String, dataset: DataFrame, spark: SparkSession) = {
        import spark.implicits._

        val schema = dataset.schema.map(e => (name, e.name, e.dataType.typeName)).toDF("name", "column", "data_type")
        val result = ConstraintSuggestionRunner().onData(dataset).addConstraintRules(Rules.DEFAULT) .run()
        val sug1 = result.constraintSuggestions
                         .flatMap { 
                                    case (column, suggestions) =>  suggestions.map { 
                                        constraint => (name, column, constraint.currentValue.split(": ")(0), constraint.currentValue.split(": ")(1))  
                                    } 
                          }
                         .toSeq.toDF("name", "column", "constraint", "current_value")
        // return
        schema.join(sug1, Seq("column", "name"), "inner")
    }

    def anomaly_check(metrics: DataFrame, thresholds: DataFrame) = {
        val met_thres = metrics.join(thresholds, Seq("analysis", "instance", "name"), "inner")
        //return
        met_thres.withColumn("check_ok", met_thres("value")-met_thres("lower") <= 0.01 && met_thres("upper")-met_thres("value") <= 0.01 ) // 1% margin for floating point errors
                 .select("name", "instance", "analysis", "check_ok", "value", "lower", "upper", "exec_time")
    }

    def calc_thresholds(metrics: DataFrame) = {
        val mean_std = metrics.groupBy("instance", "analysis", "name")
                              .agg(avg(col("value")), stddev_pop(col("value")))
                              .withColumnRenamed("avg(value)", "mean")
                              .withColumnRenamed("stddev_pop(value)", "std_dev")
        // return
        mean_std.where(mean_std("analysis").isNotNull)
                .withColumn("lower", mean_std("mean") - mean_std("std_dev") - mean_std("std_dev") - mean_std("std_dev")) // lower = mean - stddev*3
                .withColumn("upper", mean_std("mean") + mean_std("std_dev") + mean_std("std_dev") + mean_std("std_dev")) // upper = mean + stddev*3      
    }

    def time_now() = {
        new java.sql.Timestamp(System.currentTimeMillis())
    }

    def calc_metrics(name: String, dataset: DataFrame, suggestion: DataFrame, session: SparkSession) = {
        val completeness = suggestion.where(suggestion("constraint").startsWith("Completeness"))
        val compliance = suggestion.where(suggestion("constraint").startsWith("Compliance"))

        val complete_list = completeness.select("column").collect.map(e => e(0).toString).toSeq
        val compliance_list = compliance.select("column").collect.map(e => e(0).toString).toSet.toSeq

        var runner = AnalysisRunner.onData(dataset)
        complete_list.foreach(e => {
                runner.addAnalyzer(Completeness(e))
                runner.addAnalyzer(Uniqueness(e))
            }
        )
        compliance_list.foreach(e => {
                runner.addAnalyzer(Entropy(e))
            }
        )
        runner.addAnalyzer(Size())

        val analysis: AnalyzerContext = runner.run()
        // return
        successMetricsAsDataFrame(session, analysis)
            .withColumnRenamed("name","analysis")
            .withColumn("name", lit(name))
            .withColumn("exec_time", lit(time_now().toString)) 
    }

 /*
    def publish(dataset: DataFrame): Unit = {
        dataset(!dataset("check_ok"))
        var slack_msg = ("blocks" -> Option)
        var blocks = List.empty
        var block = (
            ("type" -> "section") ~ 
            ("text" -> 
                ("type" -> "mrkdwn") ~ 
                ("text" -> "AAA")
            )
        )
    }
*/
}
