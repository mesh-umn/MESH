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

import scala.language.implicitConversions

import com.twitter.algebird._
import AlgebirdProgram._
import HyperGraph._
import Util._

object OneModeDegrees {
  val hll = new HyperLogLogMonoid(12)  // 12 bits for 1% error
  def plusFunc(hllLeft: HLL, hllRight: HLL): HLL = hll.plus(hllLeft, hllRight)
  implicit val hllSemigroup = Semigroup.from(plusFunc)

  val hvProcedure: Procedure[Approximate[Long], HLL, HLL] = (ss, id, attr, msg, ctx) => {
    if (ss == 0) {
      // Disregard incoming message and send out a 1-element (approximate) set
      // including our id.
      ctx.broadcast(hll(id.toString.getBytes()))
    }
    else if (ss == 1) {
      ctx.become(hll.sizeOf(msg))
    }
    else {
      require(false, "Should terminate after superstep 1")
    }
  }

  val heProcedure: Procedure[Unit, HLL, HLL] = (ss, id, attr, msg, ctx) => {
    if (ss == 0) {
      // Just forward.
      ctx.broadcast(msg)
    }
    else if (ss == 1) {
      // no-op
    }
    else {
      require(false, "Should terminate after superstep 1")
    }
  }

  def apply[HVD, HED](hg: HyperGraph[HVD, HED]): HyperVertexRDD[Approximate[Long]] = {
    val logger = Logger("OneModeDegrees.apply")

    // First strip of HE attrs, replace HV attrs with the (approximate)
    // count of neighbors
    val mappedHg =
      hg.mapHyperVertices(_ => Approximate.exact(0L))
        .mapHyperEdges(_ => ())
    val initialMessage = hll("Initial Message".getBytes())
    val computedHg = mappedHg.compute(2, initialMessage, hvProcedure, heProcedure)
    computedHg.hyperVertices
  }
}
