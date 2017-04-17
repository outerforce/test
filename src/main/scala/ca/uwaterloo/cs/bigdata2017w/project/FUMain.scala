package ca.uwaterloo.cs.bigdata2017w.project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
  * @author Irene
  */

class MainConf(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input, output,reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  //val parallelism = opt[Int](descr = "parallel", required = false, default = Some(-1))
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(4))
  verify()
}

object FUMain{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {

    val args = new MainConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    //log.info("Parallelism: " + args.parallelism())
    log.info("Reducers: " + args.reducers())

    //val parallelism = args.parallelism()
    val reducers = args.reducers()
    var minProgress = 2000
    var progressCounter = 4

    val conf = new SparkConf().setAppName("MainGraph")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    if (deleted) println("output directory is deleted.")

    //var initialgraph : Graph[Int, Int] = GraphLoader.edgeListFile(sc, args.input()).cache()

    val textFile = sc.textFile(args.input())
    //val inputHashFunc = (id: String) => id.toLong

    var edgeRDD = textFile.map(line => {
      val tokens = line.split("\t").map(_.trim())
      tokens.length match {
        case 2 => {
          new Edge(tokens(0).toLong, tokens(1).toLong, 1L)
        }
        //case 3 => {new Edge(tokens(0).toLong,tokens(1).toLong,tokens(2).toLong)}
        case _ => {
          throw new IllegalArgumentException("-------------invalid input line: " + line + "-------------------------")
        }
      }
            //println(tokens(0),tokens(1))
           // new Edge(inputHashFunc(tokens(0)), inputHashFunc(tokens(1)), 1L)
      })
    // if the parallelism option was set map the input to the correct number of partitions,
    // otherwise parallelism will be based off number of HDFS blocks
    //if (parallelism != -1) edgeRDD = edgeRDD.coalesce(parallelism, shuffle = true)

    // create the graph
    val initialgraph = Graph.fromEdges(edgeRDD, None).groupEdges(_ + _)
     //use a helper class to execute the louvain algorithm and save the output.

    val out = args.output().toString()
    //println(out)

    //count the job running time
    val t1 = System.nanoTime

    val runner = new LouvainGraphRunner(minProgress, progressCounter, out)
    runner.run(sc, initialgraph)
    //sc.stop()
    val duration = (System.nanoTime - t1) / 1e9d
    println("\n\n\n\n\n\n----------Job finished in " + duration + "s---------------------------\n\n\n\n\n")
  }
}
