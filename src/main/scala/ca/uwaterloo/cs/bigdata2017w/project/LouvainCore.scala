package ca.uwaterloo.cs.bigdata2017w.project

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import scala.math.BigDecimal._
/**
  * @author Irene
  */

object LouvainCore {

  //Create the initial Louvain graph, type Graph[VertexState,Long] based on an input graph of type.
  def createLouvainGraph[VD: ClassTag](graph: Graph[VD, Long]): Graph[VertexState, Long] = {

    val nodeWeights = graph.aggregateMessages[Long](
      cxt => {
        cxt.sendToSrc(cxt.attr)
        cxt.sendToDst(cxt.attr)
      },
      (e1: Long, e2: Long) => e1 + e2,
      TripletFields.EdgeOnly
    )

    //nodeWeights.foreach(result => println("\n\n\n\n\n\n\n\n\n\n\n\n"+s"nodeweight: ${result._1}, ${result._2}"+"\n\n\n\n\n\n\n\n\n\n\n\n"))

    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight
      state.internalWeight = 0L
      state.nodeWeight = weight
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    return louvainGraph
  }

  /**
    * Transform a graph from [VD,Long] to a [VertexState,Long] graph and label each vertex with a community
    * to maximize global modularity (without compressing the graph)
    */
  def louvainFromStandardGraph[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long], minProgress: Int = 1, progressCounter: Int = 1): (Double, Graph[VertexState, Long], Int) = {
    val louvainGraph = createLouvainGraph(graph)
    return louvain(sc, louvainGraph, minProgress, progressCounter)
  }


  /**
    * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity.
    * (without compressing the graph)
    */
  def louvain(sc: SparkContext,
              graph: Graph[VertexState, Long],
              minProgress: Int = 1,
              progressCounter: Int = 1): (Double, Graph[VertexState, Long], Int) = {

    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.values.map(vdata => vdata.nodeWeight).reduce(_ + _)
    val totalGraphWeight = sc.broadcast(graphWeight)

    println("\n\n\n\n\n\n-------------------totalEdgeWeight:" + totalGraphWeight.value + "\n\n\n\n\n\n")



    //    var msgRDD = louvainGraph.aggregateMessages[Map[(Long, Long), Long]](
    //      triplet => {
    //        val m1 = (triplet.dstId, Map((triplet.srcAttr.community, triplet.srcAttr.communitySigmaTot) -> triplet.attr))
    //        // val m1 = (Map((triplet.srcAttr.community, triplet.srcAttr.communitySigmaTot),triplet.dstId -> triplet.attr))
    //        val m2 = (triplet.srcId, Map((triplet.dstAttr.community, triplet.dstAttr.communitySigmaTot) -> triplet.attr))
    //        //val m2 = (Map((triplet.dstAttr.community, triplet.dstAttr.communitySigmaTot),triplet.srcId -> triplet.attr))
    //        Iterator(m1, m2)
    //      }, mergeMsg)


    // gather community information from each vertex's local neighborhood
    var communityRDD = louvainGraph.aggregateMessages(sendCommunityData, mergeCommunityMessages)
    var activeMessages = communityRDD.count() //materializes the msgRDD and caches it in memory
    print("\n\n\n\n\n" + activeMessages + "\n\n\n\n\n\n\n\n")

    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = 100000
    var stop = 0
    var updatedLastPhase = 0L
    do {
      count += 1
      even = !even

      // label each vertex with its best community based on neighboring community information
      val labeledVertices = louvainVertJoin(louvainGraph, communityRDD, totalGraphWeight, even).cache()

      // calculate new sigma total value for each community (total weight of each community)
      val communtiyUpdate = labeledVertices
        .map({ case (vid, vdata) => (vdata.community, vdata.nodeWeight + vdata.internalWeight) })
        .reduceByKey(_ + _).cache()

      // map each vertex ID to its updated community information
      val communityMapping = labeledVertices
        .map({ case (vid, vdata) => (vdata.community, vid) })
        .join(communtiyUpdate)
        .map({ case (community, (vid, sigmaTot)) => (vid, (community, sigmaTot)) })
        .cache()

      // join the community labeled vertices with the updated community info
      val updatedVertices = labeledVertices.join(communityMapping).map({ case (vid, (vdata, communityTuple)) =>
        vdata.community = communityTuple._1
        vdata.communitySigmaTot = communityTuple._2
        (vid, vdata)
      }).cache()
      updatedVertices.count()
      labeledVertices.unpersist(blocking = false)
      communtiyUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)

      val prevG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVertices)((vid, old, newOpt) => newOpt.getOrElse(old))
      /*louvainGraph.cache()
      print("--------------------\n\n\n\n")
      printlouvain(louvainGraph)*/

      // gather community information from each vertex's local neighborhood
      val oldMsgs = communityRDD
      communityRDD = louvainGraph.aggregateMessages(sendCommunityData, mergeCommunityMessages).cache()
      var activeMessages = communityRDD.count()

      //      msgRDD = louvainGraph.aggregateMessages[Map[(Long, Long), Long]](
      //        triplet => {
      //          val m1 = (triplet.dstId, Map((triplet.srcAttr.community, triplet.srcAttr.communitySigmaTot) -> triplet.attr))
      //          val m2 = (triplet.srcId, Map((triplet.dstAttr.community, triplet.dstAttr.communitySigmaTot) -> triplet.attr))
      //          Iterator(m1, m2)
      //        }
      //        , mergeMsg).cache()
      //      activeMessages = msgRDD.count()
      // materializes the graph by forcing computation
      print("\n\n\n\n\n" + activeMessages + "\n\n\n\n\n\n\n\n")

      oldMsgs.unpersist(blocking = false)
      updatedVertices.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)

      // half of the communites can switch on even cycles
      // and the other half on odd cycles (to prevent deadlocks)
      // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count
      if (!even) {
        println(" \n\n\n\n\n # vertices moved: " + java.text.NumberFormat.getInstance().format(updated) + "\n\n\n\n\n")
        if (updated >= updatedLastPhase - minProgress) stop += 1
        updatedLastPhase = updated
      }


    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))
    println("\n\nCompleted in " + count + " cycles\n\n\n")


    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVertices = louvainGraph.vertices.innerJoin(communityRDD)((vid, vdata, msgs) => {
      // sum the nodes internal weight and all of its edges that are in its community
      val community = vdata.community
      var k_i_in = vdata.internalWeight
      var sigmaTot = vdata.communitySigmaTot.toDouble
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        if (vdata.community == communityId) k_i_in += communityEdgeWeight
      })
      val M = totalGraphWeight.value
      val k_i = vdata.nodeWeight + vdata.internalWeight
      var q = (k_i_in.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
      if (q < 0) 0 else q
    })

    //println("----------------\n\n\n\n\n" + newVertices.values.collect().foreach(println) + "\n\n\n\n\n\n------------")

    val actualQ = newVertices.values.reduce(_ + _)
    //println("---------------------------------------------main--------------------------------------------")
    // return the modularity value of the graph along with the graph. vertices are labeled with their community
    return (actualQ, louvainGraph, count / 2)

  }


  /**
    * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
    * Returns a new set of vertices with the updated vertex state.
    */
  private def louvainVertJoin(louvainGraph: Graph[VertexState, Long], msgRDD: VertexRDD[Map[(Long, Long), Long]], totalEdgeWeight: Broadcast[Long], even: Boolean) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      var bestCommunity = vdata.community
      var startingCommunityId = bestCommunity
      var maxDeltaQ = BigDecimal(0.0);
      var bestSigmaTot = 0L
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight, totalEdgeWeight.value)
        //println("   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
          maxDeltaQ = deltaQ
          bestCommunity = communityId
          bestSigmaTot = sigmaTotal
        }
      })
      // only allow changes from low to high communties on even cyces and high to low on odd cycles
      if (vdata.community != bestCommunity && ((even && vdata.community > bestCommunity) || (!even && vdata.community < bestCommunity))) {
        //println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
        vdata.community = bestCommunity
        vdata.communitySigmaTot = bestSigmaTot
        vdata.changed = true
      }
      else {
        vdata.changed = false
      }
      vdata
    })
  }

  //Send message to every node
  private def sendMessage(edgeContext: EdgeContext[VertexState, Long, Map[Long, Iterator[(Long, Long, Long)]]]): Unit = {
    edgeContext.sendToDst(Map(edgeContext.srcAttr.community ->
      Iterator((edgeContext.srcAttr.communitySigmaTot, edgeContext.srcAttr.internalWeight, edgeContext.srcAttr.nodeWeight))))
    edgeContext.sendToSrc(Map(edgeContext.dstAttr.community ->
      Iterator((edgeContext.dstAttr.communitySigmaTot, edgeContext.dstAttr.internalWeight, edgeContext.dstAttr.nodeWeight))))
  }

  // Creates the messages passed between each vertex to convey neighborhood community data.
  /* private def sendMsg(et: EdgeTriplet[VertexState, Long]) = {
     val m1 = (et.dstId, Map((et.srcAttr.community, et.srcAttr.communitySigmaTot) -> et.attr))
     val m2 = (et.srcId, Map((et.dstAttr.community, et.dstAttr.communitySigmaTot) -> et.attr))
     Iterator(m1, m2)
   }*/

  private def sendCommunityData(e: EdgeContext[VertexState, Long, Map[(Long, Long), Long]]) = {
    val m1 = Map((e.srcAttr.community, e.srcAttr.communitySigmaTot) -> e.attr)
    val m2 = Map((e.dstAttr.community, e.dstAttr.communitySigmaTot) -> e.attr)
    e.sendToSrc(m2)
    e.sendToDst(m1)
    //Iterator(m1, m2)
  }

  // Merge message of every node
  private def mergeMessage(a: Map[Long, Iterator[(Long, Long, Long)]],
                           b: Map[Long, Iterator[(Long, Long, Long)]]): Map[Long, Iterator[(Long, Long, Long)]] = {
    val newMap = scala.collection.mutable.HashMap[Long, Iterator[(Long, Long, Long)]]()
    a.foreach {
      case (k, v) => {
        if (newMap.contains(k)) newMap(k) ++= v
        else newMap(k) = v
      }
    }
    b.foreach {
      case (k, v) => {
        if (newMap.contains(k)) newMap(k) ++= v
        else newMap(k) = v
      }
    }
    newMap.toMap
  }

/*
  private def mergeMsg(m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Long), Long]()
    m1.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }*/

  /**
    * Merge neighborhood community data into a single message for each vertex
    */
  private def mergeCommunityMessages(m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Long), Long]()
    m1.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }

  private def modularityOptimization(louvainGraph: Graph[VertexState, Long],
                                     msgRDD: VertexRDD[Map[Long, (Long, Long, Long)]],
                                     totalEdgeWeight: Broadcast[Long]): Unit = {
    louvainGraph.vertices.innerJoin(msgRDD)((vertexId, vertexData, msgs) => {
      var bestCommunity = vertexData.community
      var maxDeltaQ = 0L
      msgs.foreach { case (communityId, (sigmaTot, innerWeight, nodeWeight)) => {

      }
      }
    })
  }

  /**
    * Returns the change in modularity that would result from a vertex moving to a specified community.
    */
  private def q(currCommunityId: Long, testCommunityId: Long, testSigmaTot: Long, edgeWeightInCommunity: Long, nodeWeight: Long, internalWeight: Long, totalEdgeWeight: Long): BigDecimal = {
    val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
    val M = BigDecimal(totalEdgeWeight);
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
    val k_i_in = BigDecimal(k_i_in_L);
    val k_i = BigDecimal(nodeWeight + internalWeight);
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);

    var deltaQ = BigDecimal(0.0);
    if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
      if (M == 0) deltaQ = 0
      else deltaQ = k_i_in - (k_i * sigma_tot / M)
      //println(s" $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")
    }
    return deltaQ;
  }

  /**
    * Compress a graph by its communities, aggregate both internal node weights and edge
    * weights within communities.
    */
  def compressGraph(graph: Graph[VertexState, Long], debug: Boolean = true): Graph[VertexState, Long] = {

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        Iterator((et.srcAttr.community, 2 * et.attr)) // count the weight from both nodes
      }
      else Iterator.empty
    }).reduceByKey(_ + _)


    // aggregate the internal weights of all nodes in each community
    var internalWeights = graph.vertices.values.map(vdata => (vdata.community, vdata.internalWeight)).reduceByKey(_ + _)

    // join internal weights and self edges to find new interal weight of each community
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({ case (vid, (weight1, weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1 + weight2
      state.nodeWeight = 0L
      (vid, state)
    }).cache()


    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()


    // generate a new graph where each community of the previous, graph is now represented as a single vertex
    val compressedGraph = Graph(newVerts, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    // calculate the weighted degree of each node
    val nodeWeightMapFunc = (e: EdgeTriplet[VertexState, Long]) => Iterator((e.srcId, e.attr), (e.dstId, e.attr))
    val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2
    val nodeWeights = compressedGraph.aggregateMessages[Long](
      triplet => {
        Iterator((triplet.srcId, triplet.attr), (triplet.dstId, triplet.attr))
      }, nodeWeightReduceFunc)

    // fill in the weighted degree of each node
    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph

    newVerts.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    return louvainGraph
  }
}
