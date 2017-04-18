package ca.uwaterloo.cs.bigdata2017w.project


import org.apache.spark.SparkContext
import scala.Array.canBuildFrom
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

/**
  * @author Irene
  */

class LouvainGraphRunner(minProgress: Int, progressCounter: Int, outputdir: String) extends LouvainInterface(minProgress: Int, progressCounter: Int) {

  var qValues = Array[(Int, Double)]()

  //Graph[VertexState,Long]vertex=>VertexState edge=>Long

  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {

    var verticePath = outputdir + "/level_" + level + "_vertices"
    var edgePath = outputdir + "/level_" + level + "_edges"
    graph.vertices.saveAsTextFile(verticePath)
    graph.edges.saveAsTextFile(edgePath)
    //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
    //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")
    qValues = qValues :+ ((level, q))
    println(s"qValue: $q")

    // overwrite the q values at each level
    sc.parallelize(qValues, 1).saveAsTextFile(outputdir + "/level_" + level + "_qvalues")

    //------------------ultimate communities+ initial vertex_id-------------------------

    val VInfoRDD = sc.textFile(verticePath)
    //(5,{community:5,communitySigmaTot:6,internalWeight:0,nodeWeight:3})

    var pairRDD = VInfoRDD.map { line =>
      var effectline = line.split("\\(")(1)
      //5,{community:5,communitySigmaTot:6,internalWeight:0,nodeWeight:3}
      var InfoList = effectline.split(",\\{")
      var Vid = InfoList(0).trim()
      var community = InfoList(1).split(",")(0).split(":")(1).trim()
      (community, Vid)
    }

    if (level == 0) {
      pairRDD.reduceByKey((x, y) => x.toString() + " " + y.toString()).saveAsTextFile(outputdir + "/level_" + level + "_communitys")
    }
    else {
      var FormerVerticePath = outputdir + "/level_" + (level - 1) + "_communitys"

      val FormerVInfoRDD = sc.textFile(FormerVerticePath)

      var FormerpairRDD = FormerVInfoRDD.map { line =>
        var effectline = line.split("\\(")(1)

        var InfoList = effectline.split(",")
        var Vid = InfoList(0).trim()
        var communityList = InfoList(1).trim()
        (Vid, communityList)
      }

      val valueList = pairRDD.values.collect()
      var cpair: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()


      for (pair <- pairRDD.collect()) {
        if (!cpair.contains(pair._1))
          if (pair._1 == pair._1)
            cpair += (pair._1 -> (FormerpairRDD.lookup(pair._2).mkString(" ")))
          else
            cpair += (pair._1 -> (FormerpairRDD.lookup(pair._1).mkString(" ") + " " + FormerpairRDD.lookup(pair._2).mkString(" ")))

        else if (pair._1 == pair._1)
          cpair(pair._1) = cpair(pair._1) + FormerpairRDD.lookup(pair._2).mkString(" ")
        else
          cpair(pair._1) = cpair(pair._1) + FormerpairRDD.lookup(pair._1).mkString(" ") + " " + FormerpairRDD.lookup(pair._2).mkString(" ")
      }

      var pairList = cpair.toArray

      var pairArray = pairList.map(item => item._1 + "," + item._2.split("[^\\d]").mkString(" "))

      var result = sc.parallelize(pairArray)

      result.saveAsTextFile(outputdir + "/level_" + level + "_communitys")
      sc.stop()
    }
  }
}

