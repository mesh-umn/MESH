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

class RandomWalksSpec extends SparkSpec {

  "RandomWalks" should "compute RandomWalks over a non-cyclic hypergraph" in withContext { implicit sc =>
    val hg = CSV.hypergraph("data/spdata.csv", 0, None)
    val numIters = 50
    val startingIds = List(1, 4)
    val restartPr = 0.15
    val epsilon  = 0.0001
    val randomWalksHg = RandomWalks.pr(hg, numIters, startingIds, restartPr, epsilon)

    val hvHopsfromSource = randomWalksHg.hyperVertices.map(hv => hv.id -> hv.attr._2).collectAsMap
    val heRecentUpdates = randomWalksHg.hyperEdges.map(he => he.id -> he.attr._2).collectAsMap

    assert(hvHopsfromSource(1) === 0.15009 +- epsilon)
    assert(hvHopsfromSource(2) === 0.00009 +- epsilon)
    assert(hvHopsfromSource(3) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(4) === 0.15009 +- epsilon)
    assert(hvHopsfromSource(5) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(6) === 0.00009 +- epsilon)
    assert(hvHopsfromSource(7) === 0.00009 +- epsilon)
    assert(hvHopsfromSource(8) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(9) === 0.00009 +- epsilon)
    assert(heRecentUpdates(10) === 0.0)
    assert(heRecentUpdates(11) === 0.0)
    assert(heRecentUpdates(12) === 0.0)
    assert(heRecentUpdates(13) === 0.0)

  }

  it should "compute ShortestPath hops over a cyclic hypergraph" in withContext { implicit sc =>
    val hg = CSV.hypergraph("data/spdata_cycle.csv", 0, None)
    val numIters = 50
    val startingIds = List(1, 4)
    val restartPr = 0.15
    val epsilon  = 0.0001
    val randomWalksHg = RandomWalks.pr(hg, numIters, startingIds, restartPr, epsilon)

    val hvHopsfromSource = randomWalksHg.hyperVertices.map(hv => hv.id -> hv.attr._2).collectAsMap
    val heRecentUpdates = randomWalksHg.hyperEdges.map(he => he.id -> he.attr._2).collectAsMap

    assert(hvHopsfromSource(1) === 0.15019 +- epsilon)
    assert(hvHopsfromSource(2) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(3) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(4) === 0.15009 +- epsilon)
    assert(hvHopsfromSource(5) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(6) === 0.00009 +- epsilon)
    assert(hvHopsfromSource(7) === 0.00009 +- epsilon)
    assert(hvHopsfromSource(8) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(9) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(10) === 0.00019 +- epsilon)
    assert(hvHopsfromSource(11) === 0.00009 +- epsilon)
    assert(heRecentUpdates(12) === 0.0)
    assert(heRecentUpdates(13) === 0.0)
    assert(heRecentUpdates(14) === 0.0)
    assert(heRecentUpdates(15) === 0.0)
    assert(heRecentUpdates(16) === 0.0)
    assert(heRecentUpdates(17) === 0.0)
  }
}
