import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object Akarsh_Goyal_ItemBasedCF {

  def main(args:Array[String]):Unit = {

    // turn off logs
    val t1 = System.nanoTime
    Logger.getLogger("org").setLevel(Level.OFF)

    // create spark context
    val rating_file :String = args{0}
    val testing_file :String = args{1}
    val sparkConf = new SparkConf().setAppName("Task2").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // load files

    val trainData = sc.textFile(rating_file)
    val ttrainData = trainData.map(_.split(','))
    val testData = sc.textFile(testing_file)
    val ttestData = testData.map(_.split(','))

    // get headers
    val trainHeader = ttrainData.first()
    val testHeader = ttestData.first()

    // remove headers
    val filteredTrainData = ttrainData.filter(elem=>elem.deep != trainHeader.deep)

    val filteredTestData = ttestData.filter(elem=>elem.deep != testHeader.deep)

    val tempTrainRatings = testData.filter(ele => ele != trainHeader).map(x => (x.split(",")(0),x.split(",")(1),x.split(",")(2)))
    val testUserRatingsMap = tempTrainRatings.map(l => l._1.hashCode().toInt -> l._1 ).distinct.sortByKey().collectAsMap()

    val tempTestRatings = testData.filter(ele => ele != testHeader).map(x => (x.split(",")(0),x.split(",")(1),x.split(",")(2)))
    val testBusRatingsMap = tempTestRatings.map(l => l._2.hashCode().toInt -> l._2).distinct.sortByKey().collectAsMap()

    val itemReducedRdd = filteredTrainData.map(part => (part(1).hashCode, (part(2).toDouble, 1))).reduceByKey((r1, r2) =>
      (r1._1 + r2._1, r1._2 + r2._2))
    val avgRatingOfItem = mutable.HashMap[Int, Double]()
    itemReducedRdd.collect().foreach(x => {
      avgRatingOfItem.put(x._1, x._2._1 / x._2._2)
    })
    // filter and create maps
     val trainMap = filteredTrainData.collect().map(row=> ((row(0).hashCode,row(1).hashCode), row(2).toDouble - avgRatingOfItem(row(1).hashCode)))

    val test = filteredTestData.collect().map(row=>(row(0).hashCode(),row(1).hashCode()))

    val testMap = filteredTestData.collect().map(row=>{
      var norm_rating = row(2).toDouble
      if(avgRatingOfItem.contains(row(1).hashCode)){
        norm_rating = row(2).toDouble - avgRatingOfItem(row(1).hashCode)
      }
      ((row(0).hashCode,row(1).hashCode), norm_rating)
    }).toMap

    // other helper maps
    val userToItems = sc.parallelize(trainMap.map(elem=>elem._1)).groupByKey().collectAsMap()
    val userToItemAndRating = trainMap.map(elem=>(elem._1._1,(elem._1._2,elem._2)))

    val userAndItemToRating = trainMap.map(elem=>((elem._1._1,elem._1._2),elem._2)).toMap

    val userReducedRdd = filteredTrainData.map(part => (part(0).hashCode, (part(2).toDouble, 1))).reduceByKey((r1, r2) =>
      (r1._1 + r2._1, r1._2 + r2._2))
    val avgRatingByUser = mutable.HashMap[Int, Double]()
    userReducedRdd.collect().foreach(x => {
      avgRatingByUser.put(x._1, x._2._1 / x._2._2)
    })




    // join is taken to get all the possible pair of items
    val p1 = sc.parallelize(userToItemAndRating.toSeq)

    val userToItemAndRatingJoined  = p1.join(p1)
    val cleaned_userToMovieRating_joined = userToItemAndRatingJoined.filter(elem=>{
      elem._2._1._1 < elem._2._2._1
    })

    // map is from (item1, item2) to user
    val thefinale  = cleaned_userToMovieRating_joined.map(elem=>{
      ((elem._2._1._1,elem._2._2._1),elem._1)
    })

    // this variable contains list all users for each pair of items who rated them (key = (item1, item2), value = (list of users who rated both of them))
    val itemsToUsersWhoRatedBoth = thefinale.groupByKey()
    val pearsonSimilarityBetweenItems = itemsToUsersWhoRatedBoth.map(f = elem => {
      val item1 = elem._1._1
      val item2 = elem._1._2
      val usersRatedBothItems = elem._2
      var average_item1: Double = 0.0
      var average_item2: Double = 0.0

      usersRatedBothItems.foreach(elem => {
        average_item1 += userAndItemToRating((elem, item1))
        average_item2 += userAndItemToRating((elem, item2))
      })
      average_item1 = average_item1 / usersRatedBothItems.size
      average_item2 = average_item2 / usersRatedBothItems.size
      var numerator: Double = 0.0
      var denominator_left: Double = 0.0
      var denominator_right: Double = 0.0

      usersRatedBothItems.foreach(f = x => {
        numerator += (userAndItemToRating((x, item1))) * (userAndItemToRating((x, item2)) )
        denominator_left += scala.math.pow(userAndItemToRating((x, item1)) , 2)
        denominator_right += scala.math.pow(x = userAndItemToRating((x, item2)), 2)
      })
      denominator_left = math.sqrt(denominator_left)
      denominator_right = math.sqrt(denominator_right)
      val denominator = denominator_left * denominator_right

      // Pearson similarity formula
      var sim = 0.0
      if (!(denominator == 0.0)) {
        sim = numerator / denominator
      }
      (elem._1, sim)
    }).collectAsMap()

    val toTest = filteredTestData.collect().map(d=>(d(0).hashCode(),d(1).hashCode()))
    val neighbourhoodSize = 10

    // weighted average calculation
    val predictedandActual = toTest.map(elem=>{
      val user = elem._1
      val movie = elem._2

      if (userToItems.contains(user)) {

        if (avgRatingOfItem.contains(movie)) {
          val itemsRatedByThisUser = userToItems(user)
          var numberOfItems = neighbourhoodSize

          if (itemsRatedByThisUser.size < neighbourhoodSize) {
            numberOfItems = itemsRatedByThisUser.size
          }

          val similarityToItem = itemsRatedByThisUser.map(x => {
            var curr_sim: Double = Double.MinValue
            if (pearsonSimilarityBetweenItems.contains((movie, x))) {
              curr_sim = pearsonSimilarityBetweenItems((movie, x))
            } else if (pearsonSimilarityBetweenItems.contains((x, movie))) {
              curr_sim = pearsonSimilarityBetweenItems((x, movie))
            }
            (curr_sim, x)
          })

          val sortedSimilarityToItem = similarityToItem.toList.sortBy(e => e._1).reverse
          var numberOfItemsDone = 0
          var predictedRating: Double = 0.0
          var num: Double = 0.0
          var den: Double = 0.0

          // number of items change
          do {
            num = num + (userAndItemToRating(user, sortedSimilarityToItem(numberOfItemsDone)._2) * sortedSimilarityToItem(numberOfItemsDone)._1)
            den = den + sortedSimilarityToItem(numberOfItemsDone)._1
            numberOfItemsDone += 1
          } while (numberOfItemsDone < numberOfItems && numberOfItemsDone + 1 < sortedSimilarityToItem.size)

          predictedRating = 2.5
          if (den != 0.0) {
            predictedRating = num / den
          }

          if (predictedRating.isNaN || predictedRating > 1.0 || predictedRating < -1.0)
          {
            predictedRating = 0.0
          }
          (elem, predictedRating, testMap(elem))
        } else {
          (elem, avgRatingByUser(user), testMap(elem))
        }
      }
      else {
        if(avgRatingOfItem.contains(movie)){
          (elem, avgRatingOfItem(movie) - 2.5, testMap(elem))
        }
        else {
          (elem, 2.5, testMap(elem))
        }
      }
    })

    val predictedAndActualRDD = sc.parallelize(predictedandActual)

    val finalPredictionsRDD = predictedAndActualRDD.map(line => {
      var r = line._2
      if(avgRatingOfItem.contains(line._1._2)){
        r = line._2 + avgRatingOfItem(line._1._2)
      }
      ((testUserRatingsMap(line._1._1), testBusRatingsMap(line._1._2)), line._2)
    })

    val range1 = predictedAndActualRDD.filter(elem=> math.abs(elem._3 - elem._2) >= 0.toDouble &&  math.abs(elem._3 - elem._2) < 1.toDouble).count()
    val range2 = predictedAndActualRDD.filter(elem=> math.abs(elem._3 - elem._2) >= 1.toDouble &&  math.abs(elem._3 - elem._2) < 2.toDouble).count()
    val range3 = predictedAndActualRDD.filter(elem=> math.abs(elem._3 - elem._2) >= 2.toDouble &&  math.abs(elem._3 - elem._2) < 3.toDouble).count()
    val range4 = predictedAndActualRDD.filter(elem=> math.abs(elem._3 - elem._2) >= 3.toDouble &&  math.abs(elem._3 - elem._2) < 4.toDouble).count()
    val range5 = predictedAndActualRDD.filter(elem=> math.abs(elem._3 - elem._2) >= 4.toDouble).count()

    println(">=0 and <1: " + range1)
    println(">=1 and <2: " + range2)
    println(">=2 and <3: " + range3)
    println(">=3 and <4: " + range4)
    println(">=4: " + range5)

    //calculate RMSE
    val rms = math.sqrt(predictedAndActualRDD.map { case ((user, product), r1, r2) =>
      val err = r1 - r2
      err * err
    }.mean())

    println("RMSE:" + rms)


    var writer = new PrintWriter(new File("Akarsh_Goyal_ItemBasedCF.txt"))
    finalPredictionsRDD.sortBy(x => (x._1._1, x._1._2), true).collect().foreach(x=>{writer.write(x._1._1 + "," + x._1._2 + "," + x._2 + "\n")})
    writer.close()
    val duration = (System.nanoTime - t1) / 1e9d
    println("Time:" + duration)
  }
}
