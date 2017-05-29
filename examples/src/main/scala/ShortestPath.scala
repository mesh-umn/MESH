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

import dcsg.hg.HyperGraph._
import dcsg.hg.AlgebirdProgram._
import dcsg.hg.HyperGraph

object ShortestPath {

  def pr[HVD](hg: HyperGraph[HVD, (Int, Int)],
              maxIters: Int,
              sourceId: Int = 1): HyperGraph[(HVD, Double), ((Int, Int), Double)] = {

    // Set up initial shortest path graph
    val augmentedHg = hg.mapHyperVertices(hv =>
      if (hv.id == sourceId) hv.attr -> 0.0
      else hv.attr -> Double.PositiveInfinity
    ).mapHyperEdges(he => he.attr -> Double.PositiveInfinity)

    type ToV = (Double, Double)
    type ToE = Double

    val prHvProcedure: Procedure[(HVD, Double), ToV, ToE] = (ss, id, attr, msg, ctx) => {
      if (ss == 0) {
        val (hvd, dist) = attr
        val (_, newDist) = msg
        if (dist < newDist) {
          ctx.broadcast(dist + 1.0)
        }
      } else {
        val (hvd, dist) = attr
        val (totalWeight, update) = msg
        val newDist = update / totalWeight
        if (dist > newDist) {
          ctx.become((hvd, newDist))
          ctx.broadcast(newDist + 1.0)
        }
      }
    }

    val prHeProcedure: Procedure[((Int, Int), Double), ToE, ToV] = (ss, id, attr, msg, ctx) => {
      val ((cardinality, weight), dist) = attr
      val newDist = msg
      if (dist > newDist) {
        ctx.become((cardinality, weight), newDist)
        ctx.broadcast((weight, newDist))
      }
    }

    val initialMessage = (0.0, Double.PositiveInfinity)
    augmentedHg.compute[ToE, ToV](maxIters, initialMessage, prHvProcedure, prHeProcedure)
  }
}
