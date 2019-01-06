import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.mllib.recommendation.ALS.logName()
import java.io.{File, PrintWriter}

import org.apache.spark.mllib.recommendation.{ALS, Rating}
object Akarsh_Goyal_ModelBasedCF {
  def main(args: Array[String]) {
    val t1 = System.nanoTime
    Logger.getLogger("org").setLevel(Level.OFF)
    val start_time = System.nanoTime()
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("Assignment2")
    val sc = new SparkContext(conf)
    //var txtRating = sc.textFile(args(0)).cache()
    // var txtTest = sc.textFile(args(1)).cache()
//    var trainData :String = args{0}
//    var testData :String = args{1}
    val rating_file :String = args{0}
    val testing_file :String = args{1}

    val trainData = sc.textFile(rating_file)

    val testData = sc.textFile(testing_file)

    //trainData.take(10).foreach(println)
    val header1 = trainData.first()
    val header2 = testData.first()




    val tD_1 = trainData.filter(x => x != header1).map(x => (x.split(",")(0),x.split(",")(1),x.split(",")(2)))
    val tE_1 = testData.filter(x => x != header1).map(x => (x.split(",")(0),x.split(",")(1),x.split(",")(2)))



    val trainRatings = trainData.filter(x => x != header1).map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.hashCode().toInt, item.hashCode().toInt, rate.toDouble)
    })

    val testUserRatingsMap = tE_1.map(l => l._1.hashCode().toInt -> l._1 ).distinct.sortByKey().collectAsMap()
    val testBusRatingsMap = tE_1.map(l => l._2.hashCode().toInt -> l._2).distinct.sortByKey().collectAsMap()


    val testRatings = testData.filter(x => x != header1).map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.hashCode().toInt, item.hashCode().toInt, rate.toDouble)
    })


    // Build the recommendation model using ALS
    val rank = 2
    val numIterations = 25
    val model = ALS.train(trainRatings, rank, numIterations, 0.28)

    // Evaluate the model on rating data
    val usersProducts = testRatings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }


    val train_RDD = tD_1.map(x => (x._1.hashCode(), x._2.hashCode()) -> x._3.toDouble)


    val ratesAndPreds = testRatings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)



    var l = 0
    var l1 =0
    var l2 =0
    var l3 =0
    var l4 =0
    var l5 = 0
    for( k <- ratesAndPreds.collect()){

      var diff = Math.abs(k._2._1 - k._2._2)

      if(diff >=0 && diff <1){
        l1+=1
      }else if(diff <2){
        l2+=1
      }else if(diff < 3){
        l3+=1
      }else if(diff < 4){
        l4+=1
      }else{
        l5+=1
      }
    }

    println(">=0 and <1: "+l1)
    println(">=1 and <2: "+l2)
    println(">=2 and <3: "+l3)
    println(">=3 and <4: "+l4)
    println(">=4: "+l5)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    println("RMSE:" + math.sqrt(MSE))
    val End_time = System.nanoTime()



    var writer = new PrintWriter(new File("Akarsh_Goyal_ModelBasedCF.txt"))
    //writer.write(total_preds)
    val finalPredsRDD = predictions.map(line => {

      ((testUserRatingsMap(line._1._1), testBusRatingsMap(line._1._2)), line._2)
    })

    //writer.write(total_preds)
    finalPredsRDD.sortBy(x => (x._1._1, x._1._2), true).collect().foreach(x=>{writer.write(x._1._1+","+x._1._2+","+x._2+"\n")})
    writer.close()
    val duration = (System.nanoTime - t1) / 1e9d
    println("Time:" + duration)


  }

}
