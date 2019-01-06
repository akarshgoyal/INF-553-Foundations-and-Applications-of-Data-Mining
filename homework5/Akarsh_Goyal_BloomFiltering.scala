import java.util
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Akarsh_Goyal_BloomFiltering {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("HW5_Task2").setMaster("local[*]")

    val consumerKey = "zJfegqLu3b1AZZUNUUqAzJvwD"
    val consumerSecret = "QMbE3RLnlzmEZucGKpAeaY8JMIjDcSPCH8drI9p1XNw22MNGT4"
    val accessToken = "1035026828417626112-WADK7LelUz81YAIrHXOLOVONxRjxVY"
    val accessTokenSecret = "F4wVGCrP4ahDilFgUNP3VFsk2JOQe6lP2Q9ObRJoQacYx"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(conf, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None)

    val tweets = stream.map(status => status.getText)
    val bucketSize = 1024
    val bucket: Array[Boolean] = new Array(bucketSize)
    val tagsPool: util.HashSet[String] = new util.HashSet[String]()
    var actualFound = 0
    var bloomFound = 0
    var tweetIndex = 0

    tweets.foreachRDD(rdd => {
      var chunk: Array[String] = rdd.collect()
      if (chunk.length > 0) {
        chunk.foreach(line => {
          tweetIndex += 1
          val tags = line.replace("\n", " ").split(" ").filter(_.startsWith("#"))
          tags.foreach(line => {
            val tag = line.replace("#", "")
            val h1 = math.abs(tag.hashCode) % bucketSize
            val h2 = (hashFunction(tag) % bucketSize).toInt
            if (bucket(h1) && bucket(h2)) {
              bloomFound += 1
            }
            bucket(h1) = true
            bucket(h2) = true
            if (tagsPool.contains(tag)) {
              actualFound += 1
            }
            tagsPool.add(tag)
          })
        })

        println("The number of the twitter from beginning: " + tweetIndex.toString)
        println("FalsePositives count: " + math.abs(bloomFound - actualFound).toString)
        println("Total number of hashtags found: " + tagsPool.size().toString + "\n")
        println("Correct predictions count: " + (tagsPool.size() - math.abs(bloomFound - actualFound)).toString)
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }

  def hashFunction(tag: String): Long = {
    var hashValue = 0
    tag.foreach(char => hashValue += 67 * Char.char2int(char))
    hashValue
  }
}
