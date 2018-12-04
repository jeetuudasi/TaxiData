package DataProducer

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import Utils._

class DataProducer {

  def readFromCSV(location: String): Dataset[Row] ={

      val df = SparkObject.spark
              .read
              .option("header","true")
              .csv(location)

      return df
  }

  def preProcessSupply(input: Dataset[Row]): Dataset[Row] ={
      val pickupEvent = input
                          .select(
                                col("tpep_pickup_datetime").as("event_time")
                                ,col("PULocationID").as("location_id")
                          )
      val dropEvent = input
                          .select(
                            col("tpep_dropoff_datetime").as("event_time")
                            ,col("DOLocationID").as("location_id")
                          )
    return pickupEvent.union(dropEvent)
  }


  def preProcessDemand(input: Dataset[Row]): Dataset[Row] ={
    return input
              .select(
                col("tpep_pickup_datetime").as("event_time")
                ,col("PULocationID").as("location_id")
                ,col("trip_distance").as("trip_distance")
                ,col("tpep_dropoff_datetime")
              )
              .withColumn("trip_time", Common.dateDiffInMinutes(col("tpep_dropoff_datetime"),col("event_time")) )
              .drop(col("tpep_dropoff_datetime"))
  }

  def addLocationInfo(input: Dataset[Row]): Dataset[Row] ={
      val lookUpTable = readFromCSV(Configs.lookupLocation)
      input.as("a")
        .join(broadcast(lookUpTable.as("b")),col("a.location_id") === col("b.LocationID"))
        .select("a.*","latitude","longitude","geo_hash")
  }

  def writeToKafka(input: Dataset[Row], topic: String): Unit ={
    input.withColumn("value", (to_json(struct("*"))))
      .withColumn("key",col("geo_hash"))
      .select("key","value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", Configs.kafkaServers)
      .option("topic", topic)
      .save()
  }

  def startProcess(): Unit ={

      val sourceFile = readFromCSV(Configs.inputLocation)

      val supplyDf = addLocationInfo(preProcessSupply(sourceFile))
      val demandDf = addLocationInfo(preProcessDemand(sourceFile))

      writeToKafka(supplyDf,Configs.supplyTopic)
      writeToKafka(demandDf,Configs.demandTopic)

    }

}
