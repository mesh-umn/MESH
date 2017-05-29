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

package umn.dcsg.tools

import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.native.JsonMethods._
import dcsg.hg.{BipartiteHyperGraph, HyperGraph}
import dcsg.hg.Util._

object DBLP {

  type HID = Long
  type VID = Long
  type HAttr = (Int, Int)  // (Cardinality, Total number of papers)
  type VAttr = String  // Author name
  type Structure = RDD[(HID, VID)]
  type HAttrs = RDD[(HID, HAttr)]
  type VAttrs = RDD[(VID, VAttr)]

  def hypergraph(filename: String,
                 initialYear: Int = 0,
                 partition: Option[(PartitionStrategy, (Int, Int))] = None)
                (implicit sc: SparkContext): HyperGraph[VAttr, HAttr] = {
    val logger = Logger("JSON.hypergraph")
    val ps = papers(filename, initialYear)
    val paperCount = ps.count
    logger.log(s"papers returned: paperCount=$paperCount")
    val (structure, heAttrs, hvAttrs) = hgRDDs(ps)
    logger.log(s"hgRDDs returned")
    BipartiteHyperGraph(structure, hvAttrs, heAttrs, partition)
  }

  def papers(filename: String, initialYear: Int = 0)
            (implicit sc: SparkContext): RDD[(String, Set[String])] = {
    // TODO - could simplify this a few ways:
    //   - turn exceptional method call into a partial function
    //   - use collect(partial) instead of flatMap
    sc.textFile(filename).flatMap { line =>
      try {
        Some(parse(line))
      }
      catch {
        case _: Exception => None
      }
    }.flatMap {
      case JArray(List(JString(title), JString(yearString), JArray(authorsJson))) =>
        try {
          val year = yearString.toInt
          if (year >= initialYear) {
            val authors = for { JString(author) <- authorsJson } yield author
            Some(title -> authors.toSet)
          }
          else None
        }
        catch {
          case _: Exception => None
        }
    }
  }

  def hgRDDs(papers: RDD[(String, Set[String])]): (Structure, HAttrs, VAttrs) = {
    // First, replace titles with unique IDs.  If we are concerned about the ability to
    // recover the titles later, we can also produce the translation here, and keep that
    // on hand.  For now we don't need that.
    val codedPapers: RDD[(Long, Set[String])] = papers.zipWithUniqueId.map {
      case ((_, authorNames), titleCode) => titleCode -> authorNames
    }
    codedPapers.cache()

    // Now computing vertex attributes is quite simple
    val authorIdsToAuthorNames: VAttrs = codedPapers.flatMap(_._2).distinct.zipWithUniqueId.flip

    // Next up, replace author strings with author IDs
    val authorNamesToTitleCodes: RDD[(String, Long)] = codedPapers.flatMap {
      case (titleCode, authorNames) =>
        authorNames.map(_ -> titleCode)
    }

    val authorIdsToTitleCodes: RDD[(VID, Long)] =
      authorNamesToTitleCodes.join(authorIdsToAuthorNames.flip).map {
        case (authorName, (titleCode, authorId)) =>
          authorId -> titleCode
      }

    val titleCodesToAuthorIdSets: RDD[(Long, Set[VID])] =
      authorIdsToTitleCodes.flip.groupByKey.mapValues(_.toSet)

    val authorSetsToTitleCodeSets: RDD[(HID, Set[VID], Set[Long])] =
      titleCodesToAuthorIdSets.flip.groupByKey.mapValues(_.toSet).zipWithUniqueId.map {
        case ((vids, titleCodes), hid) =>
          (hid, vids, titleCodes)
      }
    authorSetsToTitleCodeSets.cache()

    // Now we can easily extract the structure...
    val structure: Structure = authorSetsToTitleCodeSets.flatMap {
      case (hid, vids, _) =>
        vids.map(hid -> _)
    }

    // ...and hyperedge attributes
    val cardinalitiesAndPaperCounts: HAttrs = authorSetsToTitleCodeSets.map {
      case (hid, vids, titleCodes) =>
        val cardinality = vids.size
        val numPapers = titleCodes.size
        hid -> (cardinality, numPapers)
    }

    (structure, cardinalitiesAndPaperCounts, authorIdsToAuthorNames)
  }
}
