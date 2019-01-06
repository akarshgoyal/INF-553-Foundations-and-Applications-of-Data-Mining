import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType}
//import java.io.File
//import java.io.{File, PrintWriter}
import java.io.File

import java.io.PrintWriter





// For implicit conversions like converting RDDs to DataFrames



object Task3 {

  def main(args: Array[String]) {



    val conf = new SparkConf().setAppName("Survey Response").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val csvreader = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val df = csvreader.read
      .format("csv")
      .option("header", "true") //reading the headers
      //.option("mode", "DROPMALFORMED")
      .load(args(0))

    //println("df test ")
    //println(df.limit(10))

    val colNames = Seq("Country", "Salary", "SalaryType")

    var result = df.select(colNames.head, colNames.tail: _*)
    //var removedna = result.filter(!(col("Salary") === "NA" || col("Salary") === "0"))
    var removedna = result.filter("Salary!='NA' and Salary!='0'")
    removedna = removedna.withColumn("Salary", regexp_replace(removedna("Salary"), ",", ""))
      .withColumn("Country", regexp_replace(removedna("Country"), ",", ""))
//result.select("Country", "Salary").show()
    //removedna.show()
    removedna = removedna.withColumn("Salary", removedna.col("Salary").cast("Double"))
    val new_column = when(col("SalaryType") === "Monthly" , (col("Salary")*12).cast(DoubleType)).when(col("SalaryType") === "Weekly", (col("Salary")*52).cast(DoubleType)).otherwise(col("Salary")).cast(DoubleType)

    removedna = removedna.withColumn("Salary", new_column)
    removedna = removedna.drop("SalaryType")
    //removedna.show()

    //removedna.printSchema()
    //var kar = removedna.filter(col("Country")==="Afghanistan")
    //kar.show()
    var minIdx = removedna.groupBy(col("Country")).agg(min(col("Salary")).cast(IntegerType)).toDF("Country", "MinSalary")
    var maxIdx = removedna.groupBy(col("Country")).agg(max(col("Salary")).cast(IntegerType)).toDF("Country", "MaxSalary")
    var avgIdx = removedna.groupBy(col("Country")).agg(round(avg(col("Salary")),2).cast(DoubleType)).toDF("Country", "AvgSalary")
    var countIdx = removedna.groupBy(col("Country")).agg(count(col("Salary"))).toDF("Country", "CountSalary")
    //minIdx.show()
    //maxIdx.show()
    //avgIdx.show()
    //countIdx.show()
    countIdx = countIdx.join(minIdx, Seq("Country"),"fullouter" )
    countIdx = countIdx.join(maxIdx, Seq("Country"),"fullouter" )
    countIdx = countIdx.join(avgIdx, Seq("Country"),"fullouter" )
    countIdx = countIdx.orderBy(asc("Country"))
    countIdx.collect().foreach(x=> println(x(0)+","+x(1)+","+x(2)+","+x(3)+","+x(4)))

    //var result_4 = countIdx.rdd.collect()

    var result_4 = countIdx.rdd.map(x=>x.mkString(",")).collect()
    //countIdx.write.option("header", "true").csv("data.csv")
    //countIdx.coalesce(1).write.option("header", "false").csv("sample_file.csv")

    val rwt = new PrintWriter(new File(args(1)))

    //result_4.foreach(res => {rwt.write(res.toString.replaceAll("\\[","").replaceAll("\\]","") + "\n")})
    result_4.foreach(res => {rwt.write(res.toString + "\n")})
   rwt.close()

    

  }



}
