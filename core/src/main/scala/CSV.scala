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

import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import Util._

object CSV {

  type HID = Long
  type VID = Long
  type HAttr = (Int, Int)  // (Cardinality, weight=1)
  type VAttr = Unit
  type Structure = RDD[(HID, VID)]
  type HAttrs = RDD[(HID, HAttr)]
  type VAttrs = RDD[(VID, VAttr)]

  def hypergraph(filename: String,
                 fileArg: Int = 1,
                 partition: Option[(PartitionStrategy, (Int, Int))])
                (implicit sc: SparkContext): HyperGraph[VAttr, HAttr] = {
    val logger = Logger("CSV.hypergraph")

    val structure: Structure = sc.textFile(filename).map { line =>
      val Array(hid, vid) = line.split(",").map(_.toLong)
      hid -> vid
    }
    logger.log("structure defined")
    val heAttrs: HAttrs = structure.mapValues(_ => 1).reduceByKey(_ + _).mapValues(_ -> 1)
    val hvAttrs: VAttrs = structure.values.distinct.map(_ -> ())
    logger.log("heAttrs and hvAttrs defined")

    val hg = BipartiteHyperGraph(structure, hvAttrs, heAttrs, partition)
    logger.log("hg defined")

    val subHg = if (fileArg < 1) {
      val minHyperEdgeCardinality = -fileArg
      hg.subHyperGraph(hePred = _.attr._1 >= minHyperEdgeCardinality)
    } else if (fileArg > 0){
      val maxHyperEdgeCardinality = fileArg
      hg.subHyperGraph(hePred = _.attr._1 <= maxHyperEdgeCardinality)
    } else {
      hg
    }
    logger.log("subHg defined")
    subHg
  }
}
