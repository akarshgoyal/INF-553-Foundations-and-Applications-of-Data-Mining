
//HW3 Task1 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import java.io.File

import java.io.PrintWriter
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.Row



object Task1{
  case class Record(Coun: String, Cou: Double)
  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("Survey Response").setMaster("local")
    //val sparkContext = new SparkContext(conf)
    val sc = new SparkContext(conf);



    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

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

    result.limit(10).collect().foreach(println)

    //val ratingsRDD = result.map(line=>(line(1),line(2),line(3))).filter(x=>(  x._2 != "NA" || x._2 !='0')).map(x=>x._1->1).reduceByKey(_ + _).sortByKey()
    val ratingRDDE = result.rdd
    var result_2 =  ratingRDDE.map(line=> (line.getAs[String]("Country"),line.getAs[String]("Salary"),line.getAs[String]("SalaryType"))).filter(x=>( !( x._2 == "NA" || x._2 =="0" ))).map(x=>x._1->1).reduceByKey(_+_).sortByKey()
    //ratingRDDE.take(10).foreach(println) // -- working


    println ("Total," + result_2.values.sum().toInt)
    result_2.collect().foreach(x=> println(x._1+","+x._2))
    //println(result_2)

    var result_5 = result_2.toDF()
    var result_22 = result_2.collect()

    //val newRow = "Total," + result_2.values.sum().toInt

    //val appended = firstDF.union(newRow)
    result_5.printSchema()

    var r = "Total"
    var s = result_2.values.sum().toString

    val  schema_string = "c,v"

    val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )

    val empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)
    //val row = Row("Total", result_2.values.sum())



    val first = new Record("Total", result_2.values.sum().toInt)
    val departmentsWithEmployeesSeq1 = Seq(first)
    val df1 = departmentsWithEmployeesSeq1.toDF()




    val appended = empty_df.union(df1)
    //appended.show()
    val appended_1 = appended.union(result_5)
    appended_1.show()
    //appended_1.show()

    /*appended_1
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save("mydata.csv")*/
    val rwt = new PrintWriter(new File(args(1)))
    rwt.write("Total,")
    var dgh=result_2.collect()
    rwt.write(result_2.values.sum().toString)
    rwt.write("\n")
    dgh.foreach(res => {rwt.write(res._1.toString.replaceAll(",","") + "," + res._2.toString + "\n")})
    rwt.close()

    //appended_1.coalesce(1).write.option("header", "false").csv("sample_file_6.csv")
    //appended_1.write.option("header", "true").csv("data.csv")


//    val resultWriter = new PrintWriter(new File("Task_1.csv"))
//    resultWriter.write("Total," + result_2.values.sum().toInt + "\n")
//    val tempResult2 = result_5.repartition(1).collect().foreach(x=> (x(0)+","+x(1)))
//    tempResult2(resultWriter)

    //resultWriter.write(result_5)

    //resultWriter.close()

    //result_5.repartition(1).write.csv("Task_1.csv")

    //result_5.coalesce(1).write.option("header", "false").csv("sample_file_1.csv")

    //result_1.foreach(println)
    //ratingRDDE.filter(x => (x(1),x(2),x(3)))
    //val ratingRDD = ratingRDDE.map(line=>(line(1),line(2),line(3))).filter(x=>(  x._2 != "NA" || x._2 !='0'))
    //ratingRDD.take(10).foreach(println)

    /*

    var ratingsData =  sparkContext.textFile("src/main/resources/survey_results_public.csv")

    //var testData = sparkContext.textFile("file:///D:/HW3/testing_small.csv")

    val r_header = ratingsData.first()
    print(r_header)
    ratingsData = ratingsData.filter(row => row != r_header)




    //Construct training data

    //val ratingsRDDE = ratingsData.map(line=>line.split(',')).map(line=>(line(3),line(50),line(51)))
    //ratingsRDDE.collect().take(10).foreach(println)


    val ratingsRDD = ratingsData.map(line=>line.split(',')).map(line=>(line(3).replace("\"", ""),line(52),line(53))).filter(x=>(  x._2 != "NA" || x._2 !='0')).map(x=>x._1->1).reduceByKey(_ + _).sortByKey()
    print(ratingsRDD.values.sum())


    ratingsRDD.collect().foreach(x=> println(x._1+","+x._2))

*/


  }
}



