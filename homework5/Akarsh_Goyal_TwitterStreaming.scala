
  import org.apache.spark.SparkConf
  import org.apache.log4j.{Level, Logger}
  import scala.collection.mutable.ListBuffer
  import org.apache.spark.streaming.twitter.TwitterUtils
  import org.apache.spark.streaming.{Seconds, StreamingContext}


  object Akarsh_Goyal_TwitterStreaming {

    def main(args: Array[String]) {

      Logger.getLogger("org").setLevel(Level.OFF)

      val consumerKey = "zJfegqLu3b1AZZUNUUqAzJvwD"
      val consumerSecret = "QMbE3RLnlzmEZucGKpAeaY8JMIjDcSPCH8drI9p1XNw22MNGT4"
      val accessToken = "1035026828417626112-WADK7LelUz81YAIrHXOLOVONxRjxVY"
      val accessTokenSecret = "F4wVGCrP4ahDilFgUNP3VFsk2JOQe6lP2Q9ObRJoQacYx"

      System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
      System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

      val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[2]")
      val ssc = new StreamingContext(sparkConf, Seconds(10))
      val stream = TwitterUtils.createStream(ssc, None)

      val tweets = stream.map(status => status.getText)
      val sampling: ListBuffer[String] = new ListBuffer()

      var tweetIndex = 0
      val rnd = new scala.util.Random
      tweets.foreachRDD(rdd => {

        var chunk: Array[String] = rdd.collect()

        if (sampling.length < 100) {
          val requiredSize = 100 - sampling.length
          chunk.take(requiredSize).foreach(line => sampling.append(line))
          chunk = chunk.slice(requiredSize, chunk.length)
          tweetIndex = 100
        }

        if (chunk.length > 0) {
          chunk.foreach(line => {
            tweetIndex += 1
            val randomValue = rnd.nextInt(tweetIndex)
            if (randomValue < 100) {
              val randomIndex = rnd.nextInt(100)
              sampling(randomIndex) = line

              var tweetsLength = 0
              val tagsDictionary = scala.collection.mutable.HashMap[String, Int]().withDefaultValue(0)
              sampling.foreach(status => {
                tweetsLength += status.length
                val tags = status.replace("\n", " ").split(" ").filter(_.startsWith("#"))
                tags.foreach(tag => tagsDictionary(tag.replace("#", "")) += 1)
              })
              val hashTags = tagsDictionary.toSeq.sortBy(_._2).reverse.take(5)

              println("The number of the twitter from beginning: " + tweetIndex.toString)
              println("Top 5 hastags: ")
              hashTags.foreach(line => println(line._1 + ":" + line._2.toString))
              println("The average length of the twitter is: " + roundOff(tweetsLength / 100.0) + "\n")
            }
          })
        }
      })

      ssc.start()
      ssc.awaitTermination()
    }

    def roundOff(value: Double): String = {
      f"$value%1.2f"
    }

}
