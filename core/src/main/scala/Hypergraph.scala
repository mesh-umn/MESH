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

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import scala.reflect.ClassTag
import scala.language.higherKinds

object HyperGraph {
  type NodeId = org.apache.spark.graphx.VertexId
  type HyperVertexRDD[HVD] = RDD[HyperVertex[HVD]]
  type HyperEdgeRDD[HED] = RDD[HyperEdge[HED]]

  sealed trait Node[T] {
    def id: NodeId
    def attr: T
  }
  case class HyperVertex[HVD](id: NodeId, attr: HVD) extends Node[HVD]
  case class HyperEdge[HED](id: NodeId, attr: HED) extends Node[HED]

  sealed trait Recipients
  case object All extends Recipients
  case class Only(include: Set[NodeId]) extends Recipients
  case class AllExcept(exclude: Set[NodeId]) extends Recipients

  /** Combine two messages into a single message */
  type MessageCombiner[Msg] = (Msg, Msg) => Msg

  /** Compute a new state and send messages from a Hyper{Vertex,Edge} */
  type Procedure[T, InMsg, OutMsg] = (Int, NodeId, T, InMsg, Context[T, OutMsg]) => Unit

  trait Program[T, InMsg, OutMsg] extends Serializable {
    def messageCombiner: MessageCombiner[OutMsg]
    def procedure: Procedure[T, InMsg, OutMsg]
  }

  trait Context[T, OutMsg] {
    def send(msgF: NodeId => OutMsg, recipients: Recipients): Unit
    def send(msg: OutMsg, recipients: Recipients): Unit = send(_ => msg, recipients)
    def broadcast(msg: OutMsg): Unit = send(msg, All)
    def become(attr: T): Unit
  }
}

import HyperGraph._

trait HyperGraph[HVD, HED] {
  def hyperVertices: HyperVertexRDD[HVD]

  def hyperEdges: HyperEdgeRDD[HED]

  private[hg] type HyperVertexContext[OutMsg] <: Context[HVD, OutMsg]

  private[hg] type HyperEdgeContext[OutMsg] <: Context[HED, OutMsg]

  def toGraph[ED : ClassTag](mapHyperEdge: HED => ED)(combineEdges: (ED, ED) => ED): Graph[HVD, ED]

  def mapHyperEdges[HED2 : ClassTag](f: HyperEdge[HED] => HED2): HyperGraph[HVD, HED2]

  def mapHyperVertices[HVD2 : ClassTag](f: HyperVertex[HVD] => HVD2): HyperGraph[HVD2, HED]

  def degrees(): HyperGraph[Int, Int]

  def subHyperGraph(hvPred: HyperVertex[HVD] => Boolean = hv => true,
                    hePred: HyperEdge[HED] => Boolean = he => true,
                    retainIsolated: Boolean = false): HyperGraph[HVD, HED]

  def dual: HyperGraph[HED, HVD]

  def compute[ToE : ClassTag, ToV : ClassTag](
    maxIters: Int,
    initialMessage: ToV,
    hvProgram: Program[HVD, ToV, ToE],
    heProgram: Program[HED, ToE, ToV]): HyperGraph[HVD, HED]
}
