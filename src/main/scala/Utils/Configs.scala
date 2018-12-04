package Utils

import org.apache.spark.sql
import org.apache.spark.sql.types._

object Configs {
    var inputLocation = "TestData/RawFiles/"
    var lookupLocation = "TestData/Lookup/"
    var kafkaServers = "localhost:9092"
    var supplyTopic = "supply"
    var demandTopic = "demand"
    val demandColumns =  StructType(
                                    Array(
                                            StructField("event_time",StringType),
                                            StructField("location_id",StringType),
                                            StructField("trip_distance",StringType),
                                            StructField("trip_time",StringType),
                                            StructField("latitude",StringType),
                                            StructField("longitude",StringType),
                                            StructField("geo_hash",StringType)
                                         )
                                    )

    val supplyColumns =  StructType(
                            Array(
                              StructField("event_time",StringType),
                              StructField("location_id",StringType),
                              StructField("latitude",StringType),
                              StructField("longitude",StringType),
                              StructField("geo_hash",StringType)
                            )
                          )
}
