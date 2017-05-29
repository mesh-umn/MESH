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

package dcsg.hg

import collection.mutable.ArrayBuffer
import java.io.Serializable
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Edge, EdgeContext}
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.graphx.{Graph, TripletFields}
import org.apache.spark.graphx.PartitionStrategy

import HyperGraph._
import Util._

object BipartiteHyperGraph {
  sealed trait BipartiteAttr[+HVD, +HED]
  case class HyperEdgeAttr[HED](eAttr: HED) extends BipartiteAttr[Nothing, HED]
  case class HyperVertexAttr[HVD](vAttr: HVD) extends BipartiteAttr[HVD, Nothing]

  def apply[HVD : ClassTag, HED : ClassTag](structure: RDD[(NodeId, NodeId)],
                                            hvAttrs: RDD[(NodeId, HVD)],
                                            heAttrs: RDD[(NodeId, HED)],
                                            partition: Option[(PartitionStrategy, (Int, Int))] = None)
    : BipartiteHyperGraph[HVD, HED] = {

    val logger = Logger("BipartiteHyperGraph.apply")

    val maxHyperVertexId = hvAttrs.keys.max
    val minHyperEdgeId = heAttrs.keys.min
    val shift = maxHyperVertexId - minHyperEdgeId + 1
    val shiftedHeAttrs = heAttrs.map {
      case (id, attr) => (id + shift) -> attr
    }
    val wrappedHvAttrs: RDD[(NodeId, BipartiteAttr[HVD, HED])] =
      hvAttrs.mapValues(HyperVertexAttr(_))
    val wrappedHeAttrs: RDD[(NodeId, BipartiteAttr[HVD, HED])] =
      shiftedHeAttrs.mapValues(HyperEdgeAttr(_))

    val vertices = wrappedHvAttrs union wrappedHeAttrs
    val edges = structure.map {
      case (hid, vid) =>
        Edge(srcId = vid, dstId = hid + shift, attr = ())
    }
    val graph = Graph(vertices, edges)

    logger.log("Graph returned")

    val partitionedGraph = if (partition.isDefined) {
      val (partitionStrategy, (numPartitions, thresold)) = partition.get
      logger.log(s"partition strategy: $partitionStrategy")
      graph.partitionBy(partitionStrategy, numPartitions, thresold)
    }
    else {
      graph
    }

    logger.log("partitionedGraph defined")

    val numVertices = partitionedGraph.numVertices
    val numEdges = partitionedGraph.numEdges
    val numHyperVertices = hvAttrs.count
    val numHyperEdges = heAttrs.count
    logger.log(s"size computed: numVertices=$numVertices, numEdges=$numEdges, " +
      s"numHyperVertices=$numHyperVertices, numHyperEdges=$numHyperEdges")

    new BipartiteHyperGraph(partitionedGraph)
  }
}

import BipartiteHyperGraph._

class BipartiteHyperGraph[HVD : ClassTag , HED : ClassTag](graph: Graph[BipartiteAttr[HVD, HED], Unit])
  extends HyperGraph[HVD, HED]
     with Serializable {

  type OriginalVD = BipartiteAttr[HVD, HED]
  type ED = Unit

  def degrees(): HyperGraph[Int, Int] = {
    val reTypedGraph: Graph[BipartiteAttr[Int, Int], Unit] = graph.mapVertices {
      case (_, HyperEdgeAttr(attr)) => HyperEdgeAttr(0)
      case (_, HyperVertexAttr(attr)) => HyperVertexAttr(0)
    }
    val graphWithDegrees = reTypedGraph.joinVertices(reTypedGraph.degrees) {
      case (vid, HyperEdgeAttr(_), degree) => HyperEdgeAttr(degree)
      case (vid, HyperVertexAttr(_), degree) => HyperVertexAttr(degree)
    }
    new BipartiteHyperGraph(graphWithDegrees)
  }

  abstract class ProgramTransformer[T, InMsg, OutMsg](program: Program[T, InMsg, OutMsg]) 
    extends Serializable {

    type ToV
    type ToE
    type M = Msg[ToV, ToE]
    type AugmentedVD = (OriginalVD, Set[(NodeId => M, Recipients)])

    def vProg: (Int, VertexId, AugmentedVD, M) => AugmentedVD
    def mergeMsg: (M, M) => M
    def tripletFields: TripletFields

    def envelopes(ec: EdgeContext[AugmentedVD, ED, M]): Set[(NodeId => M, Recipients)]
    def dst(ec: EdgeContext[AugmentedVD, ED, M]): VertexId
    def send(ec: EdgeContext[AugmentedVD, ED, M], msg: M): Unit

    def sendMsg: EdgeContext[AugmentedVD, ED, M] => Unit = { ec =>
      envelopes(ec).foreach {
        case (msgF, recipients) =>
          val destination = dst(ec)
          val shouldSend = recipients match {
            case All => true
            case Only(ids) => ids contains destination
            case AllExcept(ids) => !(ids contains destination)
          }
          if (shouldSend) {
            send(ec, msgF(destination))
          }
      }
    }
  }

  private class HVProgramTransformer[InMsg, OutMsg](program: Program[HVD, InMsg, OutMsg])
    extends ProgramTransformer[HVD, InMsg, OutMsg](program) {

    type ToV = InMsg
    type ToE = OutMsg

    def envelopes(ec: EdgeContext[AugmentedVD, ED, M]): Set[(NodeId => M, Recipients)] =
      ec.srcAttr._2
    def dst(ec: EdgeContext[AugmentedVD, ED, M]): VertexId = ec.dstId
    def send(ec: EdgeContext[AugmentedVD, ED, M], msg: M): Unit = ec.sendToDst(msg)

    // We need only the src attribute here
    def tripletFields = new TripletFields(true, false, false)

    def mergeMsg: (M, M) => M = {
      case (MsgToE(m1), MsgToE(m2)) =>
        MsgToE(program.messageCombiner(m1, m2))
      case (m1, m2) =>
        require(false, s"HyperVertexrocedure.mergeMsg called with incorrect types: ($m1, $m2)")
        m1
    }

    // First param is the superstep number
    val vProg: (Int, VertexId, AugmentedVD, M) => AugmentedVD = {
      case (ss, id, (HyperVertexAttr(attr), _), MsgToV(msg)) =>
        val context = new HyperVertexContext[OutMsg](attr)
        program.procedure(ss, id, attr, msg, context)
        val newAttr = HyperVertexAttr(context.newAttr)
        val envelopes = context.outMessages.map {
          case (f, recipients) =>
            val wrap: OutMsg => M = MsgToE(_)
            (f andThen wrap, recipients)
        }.toSet
        (newAttr, envelopes)

      case (_, _, d @ (HyperEdgeAttr(_), _), _) =>
        println("Type mismatch: running HyperVertexProcedure.vProg on a HyperEdge")
        d

      case (_, _, d, _) =>
        println("Type mismatch: running HyperVertexProcedure.vProg with " +
                "messages destined for a HyperEdge!")
        d
    }
  }

  private class HEProgramTransformer[InMsg, OutMsg](program: Program[HED, InMsg, OutMsg])
    extends ProgramTransformer[HED, InMsg, OutMsg](program) {

    type ToV = OutMsg
    type ToE = InMsg

    def envelopes(ec: EdgeContext[AugmentedVD, ED, M]): Set[(NodeId => M, Recipients)] =
      ec.dstAttr._2
    def dst(ec: EdgeContext[AugmentedVD, ED, M]): VertexId = ec.srcId
    def send(ec: EdgeContext[AugmentedVD, ED, M], msg: M): Unit = ec.sendToSrc(msg)

    // We need only the dst attribute here
    def tripletFields = new TripletFields(false, true, false)

    def mergeMsg: (M, M) => M = {
      case (MsgToV(m1), MsgToV(m2)) =>
        MsgToV(program.messageCombiner(m1, m2))
      case (m1, m2) =>
        require(false, s"HyperEdgeProcedure.mergeMsg called with incorrect types: ($m1, $m2)")
        m1
    }

    // First param is the superstep number
    val vProg: (Int, VertexId, AugmentedVD, M) => AugmentedVD = {
      case (ss, id, (HyperEdgeAttr(attr), _), MsgToE(msg)) =>
        val context = new HyperEdgeContext[OutMsg](attr)
        program.procedure(ss, id, attr, msg, context)
        val newAttr = HyperEdgeAttr(context.newAttr)
        val envelopes = context.outMessages.map {
          case (f, recipients) =>
            val wrap: OutMsg => M = MsgToV(_)
            (f andThen wrap, recipients)
        }.toSet
        (newAttr, envelopes)

      case (_, _, d @ (HyperVertexAttr(_), _), _) =>
        println("Type mismatch: running HyperEdgeProcedure.vProg on a HyperVertex")
        d

      case (_, _, d, _) =>
        println("Type mismatch: running HyperEdgeProcedure.vProg with " +
                "messages destined for a HyperVertex!")
        d
    }
  }

  def mapHyperEdges[HED2 : ClassTag](f: HyperEdge[HED] => HED2): HyperGraph[HVD, HED2] = {
    val newGraph: Graph[BipartiteAttr[HVD, HED2], Unit] = graph.mapVertices {
      case (vid, HyperEdgeAttr(attr)) => HyperEdgeAttr(f(HyperEdge(vid, attr)))
      case (_, HyperVertexAttr(attr)) => HyperVertexAttr(attr)
    }
    new BipartiteHyperGraph(newGraph)
  }

  def mapHyperVertices[HVD2 : ClassTag](f: HyperVertex[HVD] => HVD2): HyperGraph[HVD2, HED] = {
    val newGraph: Graph[BipartiteAttr[HVD2, HED], Unit] = graph.mapVertices {
      case (vid, HyperVertexAttr(attr)) => HyperVertexAttr(f(HyperVertex(vid, attr)))
      case (_, HyperEdgeAttr(attr)) => HyperEdgeAttr(attr)
    }
    new BipartiteHyperGraph(newGraph)
  }

  def toGraph[ED : ClassTag](mapHyperEdge: HED => ED)(combineEdges: (ED, ED) => ED): Graph[HVD, ED] = {
    val logger = Logger("BipartiteHyperGraph.toGraph")

    val flattenedHyperEdges: RDD[(NodeId, (ED, NodeId))] = graph.triplets.map(_.toTuple).collect {
      case ((vid, _), (hid, HyperEdgeAttr(heAttr)), _) =>
        val eAttr = mapHyperEdge(heAttr)
        hid -> (eAttr, vid)
    }
    logger.log("flattenedHyperEdges defined")

    val edges: RDD[Edge[ED]] = flattenedHyperEdges.fullOuterJoin(flattenedHyperEdges).collect {
      case (hid, (Some((eAttr, vid1)), Some((_, vid2)))) =>
        ((vid1, vid2), eAttr) 
    }.reduceByKey(combineEdges).map {
      case ((v1, v2), eAttr) =>
        Edge(v1, v2, eAttr)
    }

    logger.log("edges defined")

    val vertices: RDD[(VertexId, HVD)] = hyperVertices.map(hv => (hv.id, hv.attr))

    val g = Graph(vertices, edges)
    logger.log("Graph returned")
    g
  }

  def hyperVertices: HyperVertexRDD[HVD] = {
    val subgraph = graph.subgraph(vpred = (_, d) => d.isInstanceOf[HyperVertexAttr[HVD]])
    subgraph.vertices.collect {
      case (id, HyperVertexAttr(vAttr)) => HyperVertex(id, vAttr)
    }
  }

  def hyperEdges: HyperEdgeRDD[HED] = {
    val subgraph = graph.subgraph(vpred = (_, d) => d.isInstanceOf[HyperEdgeAttr[HED]])
    subgraph.vertices.collect {
      case (id, HyperEdgeAttr(eAttr)) => HyperEdge(id, eAttr)
    }
  }

  class BipartiteContext[T, OutMsg](attr: T) extends Context[T, OutMsg] {
    var outMessages = ArrayBuffer[(NodeId => OutMsg, Recipients)]()
    var newAttr = attr  // reuse the current attribute as a default
    def send(msgF: NodeId => OutMsg, recipients: Recipients): Unit = {
      outMessages += (msgF -> recipients)
    }
    def become(attr: T): Unit = {
      newAttr = attr
    }
  }

  class HyperVertexContext[OutMsg](attr: HVD) extends BipartiteContext[HVD, OutMsg](attr)
  class HyperEdgeContext[OutMsg](attr: HED) extends BipartiteContext[HED, OutMsg](attr)

  sealed trait Msg[+ToV, +ToE]
  case class MsgToV[ToV](msgToV: ToV) extends Msg[ToV, Nothing]
  case class MsgToE[ToE](msgToE: ToE) extends Msg[Nothing, ToE]

  def compute[ToE : ClassTag, ToV : ClassTag](
    maxIters: Int,
    initialMessage: ToV,
    hvProgram: Program[HVD, ToV, ToE],
    heProgram: Program[HED, ToE, ToV]): HyperGraph[HVD, HED] = {

    val logger = Logger("BipartiteHyperGraph.compute")

    // Get the low-level 'Pregel' building-block functions  
    val hvTransformer = new HVProgramTransformer(hvProgram)
    val hvSend = hvTransformer.sendMsg
    val hvProg = hvTransformer.vProg
    val hvMerge = hvTransformer.mergeMsg
    val hvTripletFields = hvTransformer.tripletFields

    val heTransformer = new HEProgramTransformer(heProgram)
    val heSend = heTransformer.sendMsg
    val heProg = heTransformer.vProg
    val heMerge = heTransformer.mergeMsg
    val heTripletFields = heTransformer.tripletFields

    // Transform the graph to augment attributes => (attributes, outgoingMessages)
    type M = Msg[ToV, ToE]
    type Envelope = (NodeId => M, Recipients)
    type AugmentedVD = (OriginalVD, Set[Envelope])

    val augmentedGraph: Graph[AugmentedVD, Unit] =
      augment(graph, Set.empty[Envelope])

    augmentedGraph.vertices.collect.foreach {
      case (hv) => logger.log(s"Initial state - ID: ${hv._1}, attr: ${hv._2._1}")
    }

    // Spawn the first superstep by sending initialMessage to all hyperVertices (not hyperEdges)
    var i = 0
    var g = augmentedGraph.mapVertices {
      case (vid, vd @ (HyperVertexAttr(_), _)) =>
        hvProg(i, vid, vd, MsgToV(initialMessage))

      case (_, vd) =>
        vd
    }.cache()

    logger.log("initialMessage applied at vertices")

    // Compute messages
    var messages: VertexRDD[Msg[ToV, ToE]] =
      g.aggregateMessages[M](hvSend, hvMerge, hvTripletFields)
    var activeMessages = messages.count()

    logger.log(s"messsages to hyperedges computed ($activeMessages active)")

    var prevG: Graph[AugmentedVD, Unit] = null

    // Similar to Pregel, but within a superstep, run vertices first, then hyperedges.  Note we've
    // already run vertices once by the time we get here.
    while (activeMessages > 0 && i < maxIters) {
      val heLogger = Logger(s"BipartiteHyperGraph.compute(iteration=$i, hyperEdges)")

      // Compute on hyperedges
      prevG = g
      g = g.outerJoinVertices(messages) {
        case (vid, vd, Some(msg)) => heProg(i, vid, vd, msg)
        case (_, (oldAttr, _), _) => oldAttr -> Set.empty[Envelope]
      }
      heLogger.log("hyperedges updated based on incoming messages")
      g.cache()
  
      val oldMessagesVerts = messages
      messages = g.aggregateMessages[M](heSend, heMerge, heTripletFields)
      activeMessages = messages.count()
      heLogger.log(s"messages to vertices computed ($activeMessages active)")

      oldMessagesVerts.unpersist(blocking = false)
      prevG.unpersist(blocking = false)

      // Within a superstep we first process vertices, then hyperedges, so we're done with
      // this superstep now.
      i += 1
      val hvLogger = Logger(s"BipartiteHyperGraph.compute(iteration=$i, hyperVertices)")
      if (i < maxIters) {
        // Compute on hyperVertices
        prevG = g
        g = g.outerJoinVertices(messages) {
          case (vid, vd, Some(msg)) => hvProg(i, vid, vd, msg)
          case (_, (oldAttr, _), _) => oldAttr -> Set.empty[Envelope]
        }
        heLogger.log("hypervertices updated based on incoming messages")
        g.cache()

        val oldMessagesEdges = messages
        messages =  g.aggregateMessages[M](hvSend, hvMerge, hvTripletFields)
        activeMessages = messages.count()
        hvLogger.log(s"messages to hyperedges computed ($activeMessages active)")
        
        oldMessagesEdges.unpersist(blocking=false)
        prevG.unpersist(blocking=false)
      }
    }

    logger.log("main loop completed")
    g.vertices.collect.foreach {
      case (hv) => logger.log(s"Final state - ID: ${hv._1}, attr: ${hv._2._1}")
    }

    // Transform the graph back to its unaugmented form by stripping off message buffers,
    // and return a new HyperGraph based on this result.   
    val unAugmentedGraph: Graph[BipartiteAttr[HVD, HED], Unit] =
      g.mapVertices {
        case (vid, (vd, _)) => vd
      }

    logger.log("unAugmentedGraph defined")
    new BipartiteHyperGraph(unAugmentedGraph)
  }

  def subHyperGraph(hvPred: HyperVertex[HVD] => Boolean = hv => true,
                    hePred: HyperEdge[HED] => Boolean = he => true,
                    retainIsolated: Boolean = false): HyperGraph[HVD, HED] = {
    val logger = Logger("BipartiteHyperGraph.subHyperGraph")

    val vpred: (VertexId, BipartiteAttr[HVD, HED]) => Boolean = {
      case (hid, HyperEdgeAttr(attr)) => hePred(HyperEdge(hid, attr))
      case (vid, HyperVertexAttr(attr)) => hvPred(HyperVertex(vid, attr))
    }
    val subgraph = graph.subgraph(vpred = vpred)
    val (numVertices, numEdges, subHg) = if (!retainIsolated) {
      val subgraphWithDegrees = augment(subgraph, 0).joinVertices(subgraph.degrees) {
        case (id, (attr, _), degree) => attr -> degree
      }
      val vpred: (VertexId, (BipartiteAttr[HVD, HED], Int)) => Boolean = {
        case (id, (_, degree)) => degree > 0
      }
      val subgraphWithoutIsolatedVertices = subgraphWithDegrees.subgraph(vpred = vpred)
      val stripped: Graph[BipartiteAttr[HVD, HED], Unit] =
        subgraphWithoutIsolatedVertices.mapVertices {
          case (vid, (vd, _)) => vd
        }
      (stripped.numVertices, stripped.numEdges, new BipartiteHyperGraph(stripped))
    }
    else {
      (subgraph.numVertices, subgraph.numEdges, new BipartiteHyperGraph(subgraph))
    }
    val numHyperVertices = subHg.hyperVertices.count
    val numHyperEdges = subHg.hyperEdges.count

    logger.log(s"size computed: numVertices=$numVertices, numEdges=$numEdges, " +
      s"numHyperVertices=$numHyperVertices, numHyperEdges=$numHyperEdges")

    subHg
  }

  def dual: HyperGraph[HED, HVD] = {
    val reversedEdges = graph.edges.map {
      case Edge(srcId, dstId, attr) => Edge(dstId, srcId, attr)
    }
    val newVertices: RDD[(VertexId, BipartiteAttr[HED, HVD])] = graph.vertices.map {
      case (vid, HyperVertexAttr(attr)) => (vid, HyperEdgeAttr(attr))
      case (vid, HyperEdgeAttr(attr)) => (vid, HyperVertexAttr(attr))
    }
    new BipartiteHyperGraph(Graph(newVertices, reversedEdges))
  }
}
