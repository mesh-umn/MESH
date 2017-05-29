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

import dcsg.hg.AlgebirdProgram._
import dcsg.hg.HyperGraph
import dcsg.hg.HyperGraph._
import dcsg.hg.Util._
import org.apache.spark.graphx._

// use default mergeMessages

object LabelPropagation {

  def lp[HVD, HED](hg: HyperGraph[HVD, HED],
                   maxIters: Int): HyperGraph[(HVD, Long), (HED, Long)] = {

    type Community = VertexId
    type Msg = Map[Community, Int]
    type VAttr = (HVD, Community)
    type HEAttr = (HED, Community)

    def mostFrequent(msg: Msg): Community = msg.maxBy(_.swap)._1

    val lpVProc: Procedure[VAttr, Msg, Msg] =
      (ss, id, attr, msg, ctx) => {
        val (vd, _) = attr
        val newCommunity =
          if (ss == 0) id else mostFrequent(msg)
        ctx.become((vd, newCommunity))
        ctx.broadcast(Map(newCommunity -> 1))
      }

    val lpHeProc: Procedure[HEAttr, Msg, Msg] =
      (ss, id, attr, msg, ctx) => {
        val (hed, _) = attr
        val newCommunity = mostFrequent(msg)
        ctx.become((hed, newCommunity))
        ctx.broadcast(Map(newCommunity -> 1))
      }

    // Augment vertices and Hyperedges with their corresponding labels
    val initialCommunity: Community = 0
    val augmentedHg = augment(hg, initialCommunity, initialCommunity)

    val initialMessage: Msg = Map(initialCommunity -> 1)
    augmentedHg.compute[Msg, Msg](maxIters, initialMessage, lpVProc, lpHeProc)
  }
}
