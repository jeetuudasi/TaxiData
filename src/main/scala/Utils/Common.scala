package Utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

object Common {

  def dateDiffInMinutes(column1: Column, column2: Column): Column = {
    return (unix_timestamp(column1) - unix_timestamp(column2))/60
  }

  def readFromKafka(topic: String, schema: StructType ) : Dataset[Row] = {
      import SparkObject.spark.implicits._
      val rawKafkaDF = SparkObject.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", Configs.kafkaServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as[(String)]
                //.toDF()

      val structuredDf = rawKafkaDF.select(from_json(col("value"),schema).as("data"))
                    .select("data.*")

      /*structuredDf.writeStream
                  .format("console")
                  .outputMode(OutputMode.Append())
                  .start().awaitTermination() */

      return structuredDf
  }

}
