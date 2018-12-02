package Utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Common {

  def datediffinminutes(column1: Column, column2: Column): Column = {
    return (unix_timestamp(column1) - unix_timestamp(column2) )/60
  }

}
