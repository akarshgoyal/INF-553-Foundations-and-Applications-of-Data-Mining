import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import scala.collection.mutable.ListBuffer
import scala.util.Random
import java.io.FileWriter

import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object Task1 {
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
    var nc = args(2).toInt
    var maxIter = args(3).toInt
    val feature = args(1)
    val hashing = new HashingTF()

    val sparkContext: SparkContext = new SparkContext(new SparkConf().setAppName("KM").setMaster("local"))
    val data: RDD[Iterable[String]] = sparkContext.textFile(args(0)).map(x => x.split(" ").toIterable)

    val tf: RDD[Vector] = hashing.transform(data)

    if(args(1)=="W") {
      val new_clustroids: Array[Vector] = k_means(tf, nc, maxIter)

      var error_and_erro_square_and_size: Array[(Int,(Double, Double, Int))] = tf.map { vector: Vector =>
        val (id: Int, error: Double) = predict(vector, new_clustroids)
        (id, (error, math.pow(error, 2), 1))
      }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)).collect()

      val cluster_words: Array[(Int, Iterable[(String, Int)])] = data.mapPartitions{reviews_list: Iterator[Iterable[String]] =>
        val word_clus_count: ListBuffer[((String, Int), Int)] = ListBuffer()

        for(words <- reviews_list) {
          val tf_vector: Vector = hashing.transform(words)
          val temp = predict(tf_vector, new_clustroids)
          val id: Int = temp._1

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
      var wssse = 0.0
      while(i<error_and_erro_square_and_size.length) {
        clus_details += Cluster(i+1, error_and_erro_square_and_size(i)._2._3, error_and_erro_square_and_size(i)._2._1,
          ten_words(i).toList)
        wssse += error_and_erro_square_and_size(i)._2._2
        i += 1
      }

      writeFile(Type("K-Means", wssse, clus_details.toList),"Akarsh_Goyal_KMeans_small_W_" + nc + "_" + maxIter + ".json")
    } else {
      tf.cache()
      val idf = new IDF().fit(tf)
      val tfidf = idf.transform(tf)

      val new_clustroids: Array[Vector] = k_means(tfidf, nc, maxIter)

      var error_and_erro_square_and_size: Array[(Int,(Double, Double, Int))] = tf.map { vector: Vector =>
        val (id: Int, error: Double) = predict(vector, new_clustroids)
        (id, (error, math.pow(error, 2), 1))
      }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)).collect()

      val cluster_words: Array[(Int, Iterable[(String, Int)])] = data.mapPartitions{reviews_list: Iterator[Iterable[String]] =>
        val word_clus_count: ListBuffer[((String, Int), Int)] = ListBuffer()

        for(words <- reviews_list) {
          val tf_vector: Vector = hashing.transform(words)
          val temp = predict(tf_vector, new_clustroids)
          val id: Int = temp._1

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
      var wssse = 0.0
      while(i<error_and_erro_square_and_size.length) {
        clus_details += Cluster(i+1, error_and_erro_square_and_size(i)._2._3, error_and_erro_square_and_size(i)._2._1,
          ten_words(i).toList)
        wssse += error_and_erro_square_and_size(i)._2._2
        i += 1
      }

      writeFile(Type("K-Means", wssse, clus_details.toList),"Akarsh_Goyal_KMeans_small_T_" + nc + "_" + maxIter + ".json")
    }
  }

  def generateRandomCenter(data: RDD[Vector], nc: Int): Array[Vector] = {
    val centers: ListBuffer[Vector] = ListBuffer()

    val seed = Random
    seed.setSeed(20181031)

    val temp = data.collect()
    var index = 0

    while(index<nc) {
      centers += temp(seed.nextInt(data.count().toInt))
      index += 1
    }

    return centers.toArray
  }

  def generateNewCenters(tf: RDD[Vector], centroids: Array[Vector]): Array[Vector] = {
    val clusters_vectors: RDD[(Int, Vector)] = tf.map{vector =>
      var min_index: Int = 0
      var min = Vectors.sqdist(vector, centroids(min_index))

      var index =1
      while(index < centroids.length) {
        val temp = Vectors.sqdist(vector, centroids(index))
        if(temp<min) {
          min_index = index
          min = temp
        }
        index += 1
      }

      (min_index, vector)
    }

    var index = 0
    val new_clusters: ListBuffer[Vector] = ListBuffer()
    while(index < centroids.length) {
      val cluster_vectors: RDD[Vector] = clusters_vectors.filter(row => row._1 == index).map(row => row._2)
      val summary: MultivariateStatisticalSummary = Statistics.colStats(cluster_vectors)
      new_clusters += summary.mean

      index += 1
    }

    return new_clusters.toArray
  }

  def k_means(vectors: RDD[Vector], nc: Int, iter: Int): Array[Vector] = {
    var prev_center: Array[Vector] = null
    var curr_center: Array[Vector] = generateRandomCenter(vectors, nc)
    var i = 0
    do {
      prev_center = curr_center
      curr_center = generateNewCenters(vectors, prev_center)
      i += 1
    }while(i<=iter)

    return curr_center
  }

  def predict(vector: Vector, centroids: Array[Vector]): (Int, Double) = {
    var min_i: Int = 0
    var min = Vectors.sqdist(vector, centroids(0))

    var i = 1
    while(i < centroids.length) {
      val temp = Vectors.sqdist(vector, centroids(i))
      if(temp<min) {
        min_i = i
        min = temp
      }
      i += 1
    }

    return (min_i, min)
  }

  def writeFile(results: Type, output_file: String): Unit= {
    implicit val formats = Serialization.formats(NoTypeHints)
    val writer = new FileWriter(output_file)
    writer.write(write(results))
    writer.close()
  }
}
