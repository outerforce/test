package ca.uwaterloo.cs.bigdata2017w.project

import org.rogach.scallop.ScallopConf
import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

/**
  * @author Irene
  */

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  //val parallelism = opt[Int](descr = "parallel", required = false, default = Some(-1))
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(4))
  verify()
}

object StatisticData extends Tokenizer {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    //log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("StatisticData")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile1 = sc.textFile(args.input() + "/part-00000")
    val textFile2 = sc.textFile(args.input() + "/part-00001")

    val lineLen1 = textFile1.map(s => s.length)
    val totalLength1 = lineLen1.reduce((a, b) => a + b)

    val lineLen2 = textFile1.map(s => s.length)
    val totalLength2 = lineLen2.reduce((a, b) => a + b)

    val total = totalLength1 + totalLength2
    println(total)

    val num1 = textFile1
      .map(line => {
        val tokens = tokenize(line)
        print(tokens)
        (tokens(0), tokens.drop(0).length)
      })
    val x1 = num1.map { line => line.swap }.reduceByKey(_ + _)

    val num2 = textFile2
      .map(line => {
        val tokens = tokenize(line)
        (tokens(0), tokens.drop(0).length)
      })
    val x2 = num2.map { pair => pair.swap }.reduceByKey(_ + _)

    val shit = (x1 union x2).reduceByKey(_ ++ _)

    shit.saveAsTextFile(args.output())
  }
}