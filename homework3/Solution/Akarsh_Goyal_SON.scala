import java.io._

import util.control.Breaks._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.spark.rdd.RDD

object Akarsh_Goyal_SON {

  def sort[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted
  def main(args: Array[String]) {
    val start_time = System.nanoTime()

    val conf = new SparkConf().setAppName("CF").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    var data_previous = sparkContext.textFile(args{0},minPartitions = 1)
    var given_basket = data_previous.map(row=> row.split(",")).map(row=>row(0).toInt->row(1)).groupByKey().map(_._2.toSet)
   // data.take(10).foreach(println)
    //val idata = data.map(row=>row(0)).groupByKey()

    var num_of_partition = data_previous.getNumPartitions
    val support = args{1}.toInt

    var tho = 1
    if (support/num_of_partition >tho){
      tho = support/num_of_partition
    }


    var runapriori_first = given_basket.mapPartitions(chunk => {

      algo_apriori(chunk, tho)
    }).map(x=>(x,1)).reduceByKey((v1,v2)=>1).map(_._1).collect()

    val value_bcasted = sparkContext.broadcast(runapriori_first)

    var mapping_secondtime = given_basket.mapPartitions(chunk => {
      var list_of_chunks = chunk.toList
      var put_out = List[(Set[String],Int)]()
      list_of_chunks.foreach{ i=>
        for (j<- value_bcasted.value){
          if (j.forall(i.contains)){
            put_out = Tuple2(j,1) :: put_out
          }
        }
      }
      put_out.iterator
    })


    var end = mapping_secondtime.reduceByKey(_+_).filter(_._2 >= support).map(_._1).map(x => (x.size,
      "(" + x.toList.sorted.mkString(", ") + ")")).groupByKey().sortByKey().collect()

    // Output code
    val writer = new PrintWriter(new File(args{2}))

    for(row: (Int, Iterable[String]) <- end){
      writer.write(row._2.toList.sorted.mkString(", ") + "\n")
    }



    writer.close()
    val end_time = System.nanoTime()
    println("Time: " + (end_time - start_time)/1e9d + " secs")
  }

  def algo_apriori(given_basket: Iterator[Set[String]], tho:Int): Iterator[Set[String]] = {
    var list_of_chunks = given_basket.toList

    // frequent single items
    val unit = list_of_chunks.flatten.groupBy(identity).mapValues(_.size).filter(x => x._2 >= tho).map(x => x._1).toSet

    var size = 2
    // freq to store the candidate single items for next round
    var count = unit
    // hmap to store all the valid candidate set
    var mapping_h = Set.empty[Set[String]]
    // store all the frequent single items to hmap
    unit.foreach{ i =>
      mapping_h = mapping_h + Set(i)
    }
    // res to store the candidate single items after each round
    var candidate_single = Set.empty[String]
    // size of hmap
    var sizeof_mappingh = mapping_h.size
    println("hmap00",sizeof_mappingh)


    while (count.size >= size) {
      var suitable_candidate = count.subsets(size)

      suitable_candidate.foreach { initial =>
        breakable{
          var counter = 0
          list_of_chunks.foreach{ i =>
            if (initial.subsetOf(i))
            {
              counter = counter + 1
              if (counter >= tho){
                candidate_single = candidate_single ++ initial
                mapping_h = mapping_h + initial
                break
              }
            }
          }
        }
      }

      if (mapping_h.size >sizeof_mappingh){
        count = candidate_single
        candidate_single = Set.empty[String]
        size += 1
        sizeof_mappingh = mapping_h.size
        println("hmap1",sizeof_mappingh)

      } else {
        size = 1 + count.size
        println("hmap1",size)

      }

    }


    mapping_h.iterator
  }


}
