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

import dcsg.hg.HyperGraph
import dcsg.hg.HyperGraph._
import dcsg.hg.Util._
import org.apache.spark.graphx.VertexId

object ConnectedComponents {
  def hvProgram[HVD] = new Program[(HVD, VertexId), VertexId, VertexId] {
    def messageCombiner: MessageCombiner[VertexId] = _ max _
    def procedure: Procedure[(HVD, VertexId), VertexId, VertexId] = (ss, id, attr, msg, ctx) => {
      // Set this vertex's identifier to the greatest id it's heard from
      val (oldVd, oldId) = attr
      val newId = if (ss == 0) id else msg max oldId
      val newAttr = (oldVd, newId)
      ctx.become(newAttr)

      // And if we're just getting started, or if we just changed states, then let
      // neighbors know
      if (msg > oldId || ss == 0) {
        ctx.broadcast(newId)
      }
    }
  }

  def heProgram[HED] = new Program[(HED, VertexId), VertexId, VertexId] {
    def messageCombiner: MessageCombiner[VertexId] = _ max _
    def procedure: Procedure[(HED, VertexId), VertexId, VertexId] = (ss, id, attr, msg, ctx) => {
      // Set this hyperedges's identifier to the greatest id it's heard from
      val (oldEd, oldId) = attr
      val newId = if (ss == 0) id else msg max oldId
      val newAttr = (oldEd, newId)
      ctx.become(newAttr)

      // And if we're just getting started, or if we just changed states, then let
      // neighbors know
      if (msg > oldId || ss == 0) {
        ctx.broadcast(newId)
      }
    }
  }

  def apply[HVD, HED](hg: HyperGraph[HVD, HED]): HyperGraph[(HVD, VertexId), (HED, VertexId)] = {
    hg.debugPrint("ORIGINAL hg")
    val payload: VertexId = 0
    val augmentedHg = augment(hg, payload, payload)
    augmentedHg.debugPrint("AUGMENTED hg")
    val initialMessage: VertexId = 0
    val computedHg = augmentedHg.compute(10, initialMessage, hvProgram[HVD], heProgram[HED])
    computedHg.debugPrint("COMPUTED hg")
    computedHg
  }
}
