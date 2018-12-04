package Stream

import Utils.{Configs, SparkObject}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

class Aggregator {

  def startProcess() : Unit ={
      SparkObject.spark.sparkContext.setLogLevel("ERROR")

      val supplyDf = Utils.Common.readFromKafka("supply",Configs.supplyColumns)
      val demandDf = Utils.Common.readFromKafka("demand",Configs.demandColumns)

      val supplyAgg = supplyDf.groupBy(col("geo_hash")).count()
      val demandAgg = demandDf.groupBy(col("geo_hash")).count()

      supplyAgg.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()

      demandAgg.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start().awaitTermination()

      print("here")
  }

}
