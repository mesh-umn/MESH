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

import dcsg.hg.CSV

class ShortestPathSpec extends SparkSpec {

  "ShortestPath" should "compute ShortestPath hops over a non-cyclic hypergraph" in withContext { implicit sc =>
    val hg = CSV.hypergraph("data/spdata.csv", 0, None)
    val numIters = 10
    val sourceId = 1
    val shortestPathHg = ShortestPath.pr(hg, numIters, sourceId)

    val hvHopsfromSource = shortestPathHg.hyperVertices.map(hv => hv.id -> hv.attr._2).collectAsMap
    val heRecentUpdates = shortestPathHg.hyperEdges.map(he => he.id -> he.attr._2).collectAsMap

    assert(hvHopsfromSource(1) === 0.0)
    assert(hvHopsfromSource(2) === 1.0)
    assert(hvHopsfromSource(3) === 1.0)
    assert(hvHopsfromSource(4) === 2.0)
    assert(hvHopsfromSource(5) === 2.0)
    assert(hvHopsfromSource(6) === 3.0)
    assert(hvHopsfromSource(7) === 3.0)
    assert(hvHopsfromSource(8) === 3.0)
    assert(hvHopsfromSource(9) === 4.0)
    assert(heRecentUpdates(10) === 1.0)
    assert(heRecentUpdates(11) === 2.0)
    assert(heRecentUpdates(12) === 3.0)
    assert(heRecentUpdates(13) === 4.0)

  }

  it should "compute ShortestPath hops over a cyclic hypergraph" in withContext { implicit sc =>
    val hg = CSV.hypergraph("data/spdata_cycle.csv", 0, None)
    val numIters = 10
    val sourceId = 1
    val shortestPathHg = ShortestPath.pr(hg, numIters, sourceId)

    val hvHopsfromSource = shortestPathHg.hyperVertices.map(hv => hv.id -> hv.attr._2).collectAsMap
    val heRecentUpdates = shortestPathHg.hyperEdges.map(he => he.id -> he.attr._2).collectAsMap

    assert(hvHopsfromSource(1) === 0.0)
    assert(hvHopsfromSource(2) === 1.0)
    assert(hvHopsfromSource(3) === 1.0)
    assert(hvHopsfromSource(4) === 2.0)
    assert(hvHopsfromSource(5) === 2.0)
    assert(hvHopsfromSource(6) === 3.0)
    assert(hvHopsfromSource(7) === 3.0)
    assert(hvHopsfromSource(8) === 3.0)
    assert(hvHopsfromSource(9) === 2.0)
    assert(hvHopsfromSource(10) === 1.0)
    assert(hvHopsfromSource(11) === 1.0)
    assert(heRecentUpdates(12) === 1.0)
    assert(heRecentUpdates(13) === 2.0)
    assert(heRecentUpdates(14) === 3.0)
    assert(heRecentUpdates(15) === 3.0)
    assert(heRecentUpdates(16) === 2.0)
    assert(heRecentUpdates(17) === 1.0)
  }
}
