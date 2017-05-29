/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package umn.dcsg.examples

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.EdgeContext

import scala.reflect.ClassTag
import dcsg.hg._  // use default mergeMessages
import dcsg.hg.HyperGraph._
import dcsg.hg.Util._
import dcsg.hg.AlgebirdProgram._


object PageRank {

  def entropy(ranks: Seq[Double]): Double = {
    val totalRank = ranks.sum
    val normalizedRanks = ranks.map(_ / totalRank)
    normalizedRanks.map { p => p * math.log(1/p) }.sum / math.log(2)
  }

  /**
    * Given a HyperGraph hg with HE attrs (cardinality, weight), produce an
    * appropriately weighted 1-mode graph g that is "PageRank-equivalent"; i.e.,
    * graphPageRank(g) == hypergraphPageRank(hg)
    */
  def graph[VD : ClassTag](hg: HyperGraph[VD, (Int, Int)],
                           partition: Option[(PartitionStrategy, (Int, Int))] = None): Graph[VD, Double] = {
    val logger = Logger("PageRank.graph")

    val g = hg.toGraph { case (c, w) => w.toDouble / c } (_ + _)

    logger.log("g defined")

    // Now normalize the edge weights; i.e., ensure that the sum of edge
    // weights out of a vertex is 1
    val totalWeights =
    g.aggregateMessages[Double](ec => ec.sendToSrc(ec.attr), _ + _, TripletFields.EdgeOnly)

    logger.log("totalWeights defined")

    val graph = g.mapVertices {
      case (vid, vd) => vd -> 0.0
    }.joinVertices(totalWeights) {
      case (vid, (vd, _), w) => vd -> w
    }.mapTriplets { triplet =>
      val totalWeight = triplet.srcAttr._2
      triplet.attr / totalWeight
    }.mapVertices {
      case (vid, (vd, _)) => vd
    }

    logger.log("graph defined")

    val partitionedG = if (partition.isDefined) {
      val (partitionStrategy, (numPartitions, thresold)) = partition.get
      graph.partitionBy(partitionStrategy, numPartitions, thresold)
    }
    else {
      graph
    }

    logger.log("partitionedG defined")
    val (numVertices, numEdges) = (partitionedG.numVertices, partitionedG.numEdges)
    logger.log(s"size computed: numVertices=$numVertices, numEdges=$numEdges")

    partitionedG
  }

  def prGraph[VD: ClassTag](g: Graph[VD, Double],
                            maxIters: Int,
                            alpha: Double = 0.15): Graph[(VD, Double), Double] = {

    val logger = Logger("PageRank.prGraph")

    var augmentedG: Graph[(VD, Double), Double] = augment(g, 1.0)

    var iteration = 0
    var prevG = augmentedG
    while (iteration < maxIters) {
      val iterationLogger = Logger(s"PageRank.prGraph(iteration=$iteration)")

      augmentedG.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val sendMsg: EdgeContext[(VD, Double), Double, Double] => Unit = ec => {
        val (_, srcRank) = ec.srcAttr
        val edgeWeight = ec.attr
        ec.sendToDst(srcRank * edgeWeight)
      }
      val rankUpdates = augmentedG.aggregateMessages[Double](sendMsg, _ + _, TripletFields.Src)


      iterationLogger.log("rankUpdates defined")

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevG = augmentedG
      augmentedG = augmentedG.joinVertices(rankUpdates) {
        case (id, (vd, oldRank), msgSum) =>
          (vd, alpha + (1.0 - alpha) * msgSum)
      }.cache()

      iterationLogger.log("rankUpdates applied")

      augmentedG.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      prevG.vertices.unpersist(false)
      prevG.edges.unpersist(false)

      iteration += 1
    }

    logger.log("returning")

    augmentedG
  }

  def pr[HVD](hg: HyperGraph[HVD, (Int, Int)],
              maxIters: Int,
              alpha: Double = 0.15): HyperGraph[(HVD, Double), ((Int, Int), Double)] = {

    // Augment vertices and hyperedges with their rank
    val augmentedHg = augment(hg, 1.0, 0.0)

    type ToV = (Double, Double)
    type ToE = Double

    val prHvProcedure: Procedure[(HVD, Double), ToV, ToE] = (ss, id, attr, msg, ctx) => {
      if (ss == 0) {
        // Effectively a no-op, but based on our current implementation, we need
        // to emit something or else we terminate!
        ctx.broadcast(0.0)
      }
      else {
        // Receive (sum(weights), sum(ranks)) from hyperedges.
        // In order to effectively send out (rank * weight/totalWeight), we can
        // actually send rank / totalWeight, and let the hyperedge then perform
        // the final multiplication by its weight.
        val (totalWeight, rank) = msg
        val (hvd, _) = attr
        val newRank = alpha + (1.0 - alpha) * rank
        ctx.become((hvd, newRank))
        ctx.broadcast(newRank / totalWeight)
      }
    }

    val prHeProcedure: Procedure[((Int, Int), Double), ToE, ToV] = (ss, id, attr, msg, ctx) => {
      if (ss == 0) {
        // Distribute our weight to vertices so they can compute total weight of their hyperedges;
        // distribute enough rank so each edge receives 1 total.
        val ((cardinality, weight), _) = attr
        ctx.become((cardinality, weight), 0.0)
        ctx.broadcast((weight, 1.0 / cardinality))
      }
      else {
        // Vertices sent us rank / totalWeight; we need to do the remaining multiplication
        // by our own weight.
        val ((cardinality, weight), _) = attr
        val rank = msg * weight
        ctx.become(((cardinality, weight), rank))
        ctx.broadcast((weight, rank / cardinality.toDouble))
      }
    }

    val initialMessage = (0.0, 0.0)  // Doesn't matter; vertices no-op on ss 0 anyway
    augmentedHg.compute[ToE, ToV](maxIters, initialMessage, prHvProcedure, prHeProcedure)
  }

  def prEntropy[HVD](hg: HyperGraph[HVD, (Int, Int)],
                     maxIters: Int,
                     alpha: Double = 0.15)
  : HyperGraph[(HVD, Double), ((Int, Int), (Double, Double))] = {

    type ToV = (Double, Double)
    type ToE = Seq[(Double, Double)]

    // Augment vertices with their rank, hypervertices with (rank, entropy)
    val augmentedHg = augment(hg, 1.0, (0.0, 0.0))

    // Send messages of the form (rank, totalWeight).  HyperEdges need both of these
    // in order to compute both their PR as well as their entropy
    val prEntropyHvProcedure: Procedure[(HVD, Double), ToV, ToE] = (ss, id, attr, msg, ctx) => {
      if (ss == 0) {
        // Effectively a no-op, but based on our current implementation, we need
        // to emit something or else we terminate!
        ctx.broadcast(Seq(0.0 -> 0.0))
      }
      else {
        // Receive (sum(weights), sum(ranks)) from hyperedges.
        val (totalWeight, rank) = msg
        val (hvd, _) = attr
        val newRank = alpha + (1.0 - alpha) * rank
        ctx.become((hvd, newRank))
        ctx.broadcast(Seq(newRank -> totalWeight))
      }
    }
    // Distribute our weight to vertices so they can compute total weight of their hyperedges;
    // distribute enough rank so each edge receives 1 total.

    val prEntropyHeProcedure: Procedure[((Int, Int), (Double, Double)), ToE, ToV] =
      (ss, id, attr, msg, ctx) => {
        if (ss == 0) {
          // Distribute our weight to vertices so they can compute total weight of their hyperedges;
          // distribute enough rank so each edge receives 1 total.
          val ((cardinality, weight), _) = attr
          val rank = cardinality.toDouble
          val initialEntropy = 0.0
          ctx.become((cardinality, weight), (0.0, initialEntropy))
          ctx.broadcast((weight, 1.0 / cardinality.toDouble))
        }
        else {
          val ((cardinality, weight), _) = attr
          val rank = msg.map {
            case (rank, totalWeight) =>
              rank * weight / totalWeight
          }.sum
          val ent = entropy(msg.map(_._1))
          ctx.become(((cardinality, weight), (rank, ent)))
          ctx.broadcast((weight, rank / cardinality.toDouble))
        }
      }

    val initialMessage = (0.0, 0.0)  // Doesn't matter; vertices no-op on ss 0 anyway
    augmentedHg.compute[ToE, ToV](maxIters, initialMessage, prEntropyHvProcedure, prEntropyHeProcedure)
  }
}