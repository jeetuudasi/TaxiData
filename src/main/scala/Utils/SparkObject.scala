package Utils

import org.apache.spark.sql.SparkSession

object SparkObject {
    lazy val spark: SparkSession = getSparkSession()

    def getSparkSession(): SparkSession ={
      return SparkSession.builder().master("local").getOrCreate();
    }
}
