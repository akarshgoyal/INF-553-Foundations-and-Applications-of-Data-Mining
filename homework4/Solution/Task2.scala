import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write,read}
import scala.collection.mutable.ListBuffer
import java.io.FileWriter


object Task2 {
  case class Cluster(
                      id: Int,
                      size: Int,
                      error: Double,
                      terms: List[String]
                    )
  case class Type(
                   algorithm: String,
                   WSSE: Double,
                   clusters: List[Cluster]
                 )

  def main(args: Array[String]): Unit = {
    val al = args(1)
    val nc: Int = args(2).toInt
    val maxIter = args(3).toInt
    val hashing = new HashingTF(numFeatures = 2^80)

    val sparkContext: SparkContext = new SparkContext(new SparkConf().setAppName("KM").setMaster("local"))
    val data: RDD[Iterable[String]] = sparkContext.textFile(args(0)).map(x => x.split(" ").toIterable)

    val tf: RDD[Vector] = hashing.transform(data)
    tf.cache()
    val idf = new IDF().fit(tf)
    val data_vectors: RDD[Vector] = idf.transform(tf)

    if(al == "K") {
      val kmeans = new KMeans().setK(nc).setMaxIterations(maxIter).setSeed(42)
      val clusters = kmeans.run(data_vectors)

      val WSSSE = clusters.computeCost(data_vectors)
      println("WSSSE = " + WSSSE)
      val centers = clusters.clusterCenters

      val error: Array[(Int, Double)] = data_vectors.map{ vector: Vector =>
        val id = clusters.predict(vector)
        (id, Vectors.sqdist(vector, centers(id)))
      }.reduceByKey((a, b) => a+b).collect()

      val size: Array[(Int, Int)] = data_vectors.map{ vector: Vector =>
        val id = clusters.predict(vector)
        (id, 1)
      }.reduceByKey((a, b) => a+b).collect()

      val cluster_words: Array[(Int, Iterable[(String, Int)])] = data.mapPartitions{reviews_list: Iterator[Iterable[String]] =>
        val word_clus_count: ListBuffer[((String, Int), Int)] = ListBuffer()

        for(words <- reviews_list) {
          val tf_vector: Vector = hashing.transform(words)
          val tfidf_vec: Vector = idf.transform(tf_vector)
          val id = clusters.predict(tfidf_vec)

          for(word <- words) {
            word_clus_count.append(((word, id), 1))
          }
        }
        word_clus_count.toIterator
      }.reduceByKey((a,b)=> a+b).map(x => (x._1._2,(x._1._1, x._2))).groupByKey().sortByKey().collect()

      val ten_words: ListBuffer[ListBuffer[String]] = ListBuffer()
      for(words <- cluster_words) {
        val word_count = words._2.toList.sortBy(_._2).reverse.take(10)
        val result: ListBuffer[String] = ListBuffer()

        for(w<-word_count) {
          result += w._1
        }
        ten_words += result
      }

      val clus_details: ListBuffer[Cluster] = ListBuffer()

      var i = 0
      while(i<error.length) {
        clus_details += Cluster(i+1, size(i)._2, error(i)._2,
          ten_words(i).toList)
        i += 1
      }

      val cluster_algo = Type("K-Means", WSSSE, clus_details.toList)

      writeFile(cluster_algo,"Akarsh_Goyal_Cluster_small" + "_" + args(1) + "_" + nc + "_" + maxIter + ".json")
    } else {
      val bkm = new BisectingKMeans().setK(nc).setMaxIterations(maxIter).setSeed(42)
      val clusters = bkm.run(data_vectors)
      val WSSSE = clusters.computeCost(data_vectors)
      val centers = clusters.clusterCenters

      val error: Array[(Int, Double)] = data_vectors.map{ vector: Vector =>
        val id = clusters.predict(vector)
        (id, Vectors.sqdist(vector, centers(id)))
      }.reduceByKey((a, b) => a+b).collect()

      val size: Array[(Int, Int)] = data_vectors.map{ vector: Vector =>
        val id = clusters.predict(vector)
        (id, 1)
      }.reduceByKey((a, b) => a+b).collect()

      val cluster_words: Array[(Int, Iterable[(String, Int)])] = data.mapPartitions{reviews_list: Iterator[Iterable[String]] =>
        val word_clus_count: ListBuffer[((String, Int), Int)] = ListBuffer()

        for(words <- reviews_list) {
          val tf_vector: Vector = hashing.transform(words)
          val tfidf_vec: Vector = idf.transform(tf_vector)
          val id = clusters.predict(tfidf_vec)

          for(word <- words) {
            word_clus_count.append(((word, id), 1))
          }
        }
        word_clus_count.toIterator
      }.reduceByKey((a,b)=> a+b).map(x => (x._1._2,(x._1._1, x._2))).groupByKey().sortByKey().collect()

      val ten_words: ListBuffer[ListBuffer[String]] = ListBuffer()
      for(words <- cluster_words) {
        val word_count = words._2.toList.sortBy(_._2).reverse.take(10)
        val result: ListBuffer[String] = ListBuffer()

        for(w<-word_count) {
          result += w._1
        }
        ten_words += result
      }

      val cluster_details: ListBuffer[Cluster] = ListBuffer()

      var i = 0
      while(i<error.length) {
        cluster_details += Cluster(i+1, size(i)._2, error(i)._2,
          ten_words(i).toList)
        i += 1
      }

      writeFile(Type("Bisecting K-Means", WSSSE, cluster_details.toList),"Akarsh_Goyal_Cluster_small" + "_" + args(1) + "_" + nc + "_" + maxIter + ".json")
    }
  }

  def writeFile(results: Type, output_file: String): Unit= {
    implicit val formats = Serialization.formats(NoTypeHints)
    val writer = new FileWriter(output_file)
    writer.write(write(results))
    writer.close()
  }
}
