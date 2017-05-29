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

import dcsg.hg.Util._
import umn.dcsg.tools.DBLP

class PageRankSpec extends SparkSpec {

  "PageRank" should "produce a 1-mode graph" in withContext { implicit sc =>
    val hg = DBLP.hypergraph("data/toy.json")
    val g = PageRank.graph(hg)

    // Map from vertex attr (i.e., name) to its id in the graph
    val vids = g.vertices.flip.collectAsMap

    // weights(vid1)(vid2) = weight in the graph of edge from vertex with
    // id vid1 to vertex with id vid2
    val weights =
    g.edges
      .groupBy(_.srcId)
      .mapValues(_.map(e => e.dstId -> e.attr).toMap)
      .collectAsMap

    // Weight of graph edge from vertex with name v1 to that with name v2
    def w(v1: String, v2: String): Double = weights(vids(v1))(vids(v2))

    val tolerance = 1e-3
    assert(w("v1", "v1") === (11.0 / 24) +- tolerance)
    assert(w("v1", "v2") === (11.0 / 24) +- tolerance)
    assert(w("v1", "v3") === (1.0 / 12) +- tolerance)

    assert(w("v2", "v1") === (11.0 / 36) +- tolerance)
    assert(w("v2", "v2") === (17.0 / 36) +- tolerance)
    assert(w("v2", "v3") === (2.0 / 9) +- tolerance)

    assert(w("v3", "v1") === (1.0 / 9) +- tolerance)
    assert(w("v3", "v2") === (4.0 / 9) +- tolerance)
    assert(w("v3", "v3") === (4.0 / 9) +- tolerance)
  }

  it should "compute entropy of a set" in {
    val tolerance = 1e-3
    assert(PageRank.entropy(Seq(1.0, 1.0)) === 1.0 +- tolerance)
    assert(PageRank.entropy(Seq(0.25, 0.75)) === 0.8113 +- tolerance)
    assert(PageRank.entropy(Seq(1, 3)) === 0.8113 +- tolerance)
  }

  it should "compute PageRank over a hypergraph" in withContext { implicit sc =>
    val hg = DBLP.hypergraph("data/toy.json")
    val rankedHg = PageRank.pr(hg, 5)

    val hvRanks = rankedHg.hyperVertices.map(hv => hv.id -> hv.attr._2).collectAsMap
    val heRanks = rankedHg.hyperEdges.map(he => he.id -> he.attr._2).collectAsMap

    val tolerance = 0.001
    assert(hvRanks(0) === 0.751 +- tolerance)
    assert(hvRanks(1) === 0.923 +- tolerance)
    assert(hvRanks(2) === 1.325 +- tolerance)

    assert(heRanks(3) === 0.943 +- tolerance)
    assert(heRanks(4) === 0.702 +- tolerance)
    assert(heRanks(5) === 1.355 +- tolerance)
  }
}