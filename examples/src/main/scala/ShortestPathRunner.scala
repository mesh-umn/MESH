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
import dcsg.hg.{CSV, HyperGraph}
import org.apache.spark.graphx.PartitionStrategy._

object ShortestPathRunner {
  def main(args: Array[String]): Unit = {

    val logger = Logger("ShortestPathRunner")

    if (args.size != 8) {
      usage()
      println(args(0))
      System.exit(1)
    }

    val inputfile = args(0)
    val fileArg = args(1).toInt

    var partitionStrategy = args(2) match {
      case "1D-src" => EdgePartition1D
      case "1D-dst" => EdgePartition1DByDst
      case "2D" => EdgePartition2D
      case "GreedySrc" => GreedySrc
      case "GreedyDst" => GreedyDst
      case "HybridSrc" => HybridSrc
      case "HybridDst" => HybridDst
    }

    val numPartitions = args(3).toInt
    val threshold = args(4).toInt
    val partition = Some(partitionStrategy -> (numPartitions, threshold))

    val numIters = args(5).toInt
    val numRuns = args(6).toInt
    val sourceId = args(7).toInt

    val algorithm: HyperGraph[_, (Int, Int)] => Unit = ShortestPath.pr(_, numIters, sourceId)

    implicit val sc = makeSparkContext("ShortestPathRunner")
    try {
      val start = System.currentTimeMillis()
      val hg = CSV.hypergraph(inputfile, fileArg, partition)
      val end = System.currentTimeMillis()
      val partitionTime = end - start
      logger.log("Start shortest path")
      logger.log(s"Time taken for partitioning: $partitionTime ms")

      (1 to numRuns) foreach { i =>
        val start = System.currentTimeMillis()
        algorithm(hg)
        val end = System.currentTimeMillis()
        val executionTime = end - start
        logger.log(s"Time taken for run $i : $executionTime ms")
      }
      logger.log("End shortest path!")
    }
    finally {
      sc.stop()
    }
  }

  def usage(): Unit = {
    println("usage: ShortestPathRunner inputfile fileArg partitionStrategy numPartitions threshold numIters numRuns outputFlag sourceId")
  }
}
