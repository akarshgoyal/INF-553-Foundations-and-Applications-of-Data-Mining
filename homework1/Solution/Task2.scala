import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import java.io.File

import java.io.PrintWriter

object Task2{
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

    //var onetime = System.currentTimeMillis()


    val colNames = Seq("Country", "Salary", "SalaryType")

    var result = df.select(colNames.head, colNames.tail: _*)

    result.limit(10).collect().foreach(println)

    //val ratingsRDD = result.map(line=>(line(1),line(2),line(3))).filter(x=>(  x._2 != "NA" || x._2 !='0')).map(x=>x._1->1).reduceByKey(_ + _).sortByKey()
    val ratingRDDE = result.rdd
    var onetime = System.currentTimeMillis()
    var result_8 = ratingRDDE.map(line => (line.getAs[String]("Country"), line.getAs[String]("Salary"), line.getAs[String]("SalaryType"))).filter(x => (!(x._2 == "NA" || x._2 == "0")))


    var result_2 = result_8.map(x => x._1 -> 1).reduceByKey(_ + _).sortByKey()
    //ratingRDDE.take(10).foreach(println) // -- working


    //println("Total," + result_2.values.sum().toInt)
    //result_2.collect().foreach(x => println(x._1 + "," + x._2))

    var secondtime = System.currentTimeMillis()
    var duration = (secondtime - onetime)

    var thirdtime = System.currentTimeMillis()

    var counPartition = df.repartition(2,df("Country"))
    val colNames_1 = Seq("Country", "Salary", "SalaryType")

    var result_1 = df.select(colNames.head, colNames.tail: _*)

    result_1.limit(10).collect().foreach(println)

    //val ratingsRDD = result.map(line=>(line(1),line(2),line(3))).filter(x=>(  x._2 != "NA" || x._2 !='0')).map(x=>x._1->1).reduceByKey(_ + _).sortByKey()
    val ratingRDDE_1 = result_1.rdd
    var result_9 = ratingRDDE_1.map(line => (line.getAs[String]("Country"), line.getAs[String]("Salary"), line.getAs[String]("SalaryType"))).filter(x => (!(x._2 == "NA" || x._2 == "0")))

    var result_3 = result_9.map(x => x._1 -> 1).reduceByKey(_ + _).sortByKey()
    var fourthtime = System.currentTimeMillis()
    var duration_1 = (fourthtime-thirdtime)

    val rwt = new PrintWriter(new File(args(1)))
    rwt.write("standard")
    val sd = ratingRDDE.mapPartitions(iterations => Array(iterations.size).iterator, true).collect()
    sd.foreach(r => {rwt.write("," + r.toString)})
    rwt.write("," + duration.toString + "\n")
    rwt.write("partition")
    val sd_1 = ratingRDDE_1.mapPartitions(iterations => Array(iterations.size).iterator, true).collect()
    sd_1.foreach(r => { rwt.write("," + r.toString)})
    rwt.write("," + duration_1.toString + "\n")
    rwt.close()



  }

}