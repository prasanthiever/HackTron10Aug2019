package com.implement

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.to_date

class exec {
  
  
  def ConsecutivePurchase(r1:Dataset[Row]){


val r2=r1.select("Tid","Cid","Pid").withColumn("b",expr("substring(Date,0,10)"))

val r3= r2.groupBy("Cid","Pid","b").count()

val r4 = r3.filter("count> 2").count()
    
  }
  
  
  def salesDistributionByProductNameandType(s1:Dataset[Row],p1:Dataset[Row]){
    
val joinexp=(s1.col("Pid")===p1.col("Pid"))
val js1=s1.join(p1,joinexp,"LEFT").drop(p1("Pid"))

js1.groupBy("Pname").agg(count("Pname")).show
js1.groupBy("Ptype").agg(count("Ptype")).show

val sList=s1.groupBy("Pid").agg(count("Pid")).orderBy("Pid").select("Pid").rdd.map(r=>r.getInt(0)).collect.toList
s1.groupBy("Pid").agg(count("Pid")).orderBy("Pid").select("Pid").rdd.map(r=>r.getInt(0)).collect.toList.size

p1.where(not(col("Pid").isin(sList:_*))).show
    
  }
  
  def third(){}
  
  
  def secondMostPurchase(s1:Dataset[Row],c1:Dataset[Row],r1:Dataset[Row]){
    
    val rList=r1.select("OTid").rdd.map(r=>r.getInt(0)).collect.toList
s1.where(not(col("Tid").isin(rList:_*)))
    val dfResult=s1.join(c1, s1("customer_id").equalTo(c1("customer_id")),"leftouter")
 
 val df=dfResult.select(s1.col("customer_id"), s1.col("transaction_id"),s1.col("timestamp"),c1("customer_id")
     ,c1("customer_first_name"),c1("customer_last_name"))
     
     df.show()
     
      val winSales=Window.partitionBy(s1("customer_id")).orderBy("transaction_id")
     
      
      val winDfSale=df.withColumn("count", count(s1("transaction_id")).over(winSales))
      
      winDfSale.show()
    
  }
  
  def  notSoldAtLeastOnce(s1:Dataset[Row],p1:Dataset[Row]){
    val sList=s1.groupBy("Pid").agg(count("Pid")).orderBy("Pid").select("Pid").rdd.map(r=>r.getInt(0)).collect.toList
s1.groupBy("Pid").agg(count("Pid")).orderBy("Pid").select("Pid").rdd.map(r=>r.getInt(0)).collect.toList.size

p1.where(not(col("Pid").isin(sList:_*))).show

  }
  
  
}