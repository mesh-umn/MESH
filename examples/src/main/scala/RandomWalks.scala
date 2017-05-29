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

object RandomWalks {

  def pr[HVD](hg: HyperGraph[HVD, (Int, Int)],
              maxIters: Int, startingIds: List[Int], restartPr: Double,
              epsilon: Double): HyperGraph[(HVD, Double), ((Int, Int), Double)] = {

    val augmentedHg = hg.mapHyperVertices(hv =>
      if (startingIds contains hv.id) hv.attr -> 1.0
      else hv.attr -> 0.0
    ).mapHyperEdges(he => he.attr -> 0.0)

    type ToV = (Double, Double)
    type ToE = Double

    val prHvProcedure: Procedure[(HVD, Double), ToV, ToE] = (ss, id, attr, msg, ctx) => {
      if (ss == 0) {
        val (_, v) = attr
        ctx.broadcast(v)
      } else {
        val (hvd, _) = attr
        val (totalWeight, updates) = msg
        val diff = (1 - restartPr) * updates
        if (startingIds contains id) {
          ctx.become(hvd, restartPr + diff)
        } else {
          ctx.become(hvd, diff)
        }
        if ((diff / totalWeight) > epsilon) {
          ctx.broadcast(diff / totalWeight)
        }
      }
    }

    val prHeProcedure: Procedure[((Int, Int), Double), ToE, ToV] = (ss, id, attr, msg, ctx) => {
      val ((cardinality, weight), _) = attr
      ctx.broadcast(weight, msg / cardinality)
    }

    val initialMessage = (1.0, 0.0)
    augmentedHg.compute[ToE, ToV](maxIters, initialMessage, prHvProcedure, prHeProcedure)
  }
}
