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
import org.apache.spark.{SparkConf, SparkContext}

import org.slf4j.LoggerFactory

object Util {
  implicit class FlipRDDOps[K, V](rdd: RDD[(K, V)]) {
    def flip(): RDD[(V, K)] = rdd.map {
      case (k, v) => (v, k)
    }
  }

  def makeSparkContext(appName: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    new SparkContext(conf)
  }

  implicit class DebuggableHyperGraph[HVD, HED](hg: HyperGraph[HVD, HED]) {
    def debugPrint(headerMessage: String): Unit = {
      val hvs = hg.hyperVertices.collect()
      val hes = hg.hyperEdges.collect()
      println(s"-------- $headerMessage ----------")
      println("  hyperVertices:")
      hvs foreach println
      println()
      println("  hyperEdges:")
      hes foreach println
      println()
    }
  }

  // TODO - provide 'strip' as an inverse function for augment

  // TODO - provide an implicit conversion to allow these to be called as methods on Graph
  // and HyperGraph, to allow chaining

  def augment[HVD, HED, V, E](hg: HyperGraph[HVD, HED], v: V, e: E): HyperGraph[(HVD, V), (HED, E)] =
    hg.mapHyperVertices(hv => hv.attr -> v).mapHyperEdges(he => he.attr -> e)

  def augment[VD, V, ED](g: Graph[VD, ED], v: V): Graph[(VD, V), ED] = g.mapVertices {
    case (vid, vd) =>
      vd -> v
  }

  case class Logger(prefix: String) {
    val logger = LoggerFactory.getLogger(prefix)
    log("logger created")
    def now(): Long = System.currentTimeMillis()
    def log(label: String): Unit = {
      logger.info(s"LOGGER,${now()},$prefix,$label")
    }
  }
}
