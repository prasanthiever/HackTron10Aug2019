package com.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.implement.exec
import com.implement.exec
import com.implement.exec
import com.implement.exec
import com.implement.exec

object Implement {
  
  def main(args:Array[String]){
     
     val spark=SparkSession.builder().appName("app").master("local").getOrCreate()
     val salesSchema=StructType(Array(
StructField("Tid",IntegerType),
StructField("Cid",IntegerType),
StructField("Pid",IntegerType),
StructField("Date",StringType),
StructField("TotAmt",StringType),
StructField("Qty",IntegerType)
))

val p_schema=StructType(Array(
StructField("Pid",IntegerType),
StructField("Pname",StringType),
StructField("Ptype",StringType),
StructField("Pver",StringType),
StructField("Pprice",StringType)
))

val r_schema=StructType(Array(
StructField("Rid",IntegerType),
StructField("OTid",IntegerType),
StructField("Cid",IntegerType),
StructField("Pid",IntegerType),
StructField("Timestamp",StringType),
StructField("Ramt",StringType),
StructField("Rqty",IntegerType)
))

val c_schema=StructType(Array( 
StructField("customer_id",IntegerType),
StructField("customer_first_name",StringType),
StructField("customer_last_name",StringType),
StructField("phone_number",IntegerType)
))

     val s1=spark.read.format("csv").option("delimiter","|").schema(salesSchema).load("File:///C:/Users/omshanti/hackatron/Sales.csv")
    val p1=spark.read.format("csv").option("delimiter","|").schema(p_schema).load("File:///C:/Users/omshanti/hackatron/Product.csv")
    val r1=spark.read.format("csv").option("delimiter","|").schema(r_schema).load("File:///C:/Users/omshanti/hackatron/Refund.csv")
    val c1=spark.read.format("csv").option("delimiter","|").schema(c_schema).load("File:///C:/Users/omshanti/hackatron/Customer.csv")
   
     
     val exec=new exec
    
    exec.ConsecutivePurchase(r1)
    
     exec.notSoldAtLeastOnce(s1, p1)
     
     exec.salesDistributionByProductNameandType(s1, p1)
     
     exec.secondMostPurchase(s1, c1, r1)
  }
}