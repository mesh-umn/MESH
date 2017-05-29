# MESH: Minnesota Engine for Scalable Evolving Hypergraph Analysis

MESH is an open-source framework of algorithms and system components to support scalable analysis of evolving hypergraph structures. 


### Goals

* Support a rich API for ease of expressing hypergraph algorithms

* Provide scalability across machines and to large datasets

* Provide flexibility to support different datasets and algorithms

* Characterize and optimize for real-world hypergraphs

* Design new algorithms to capture real-world group phenomena


Learn more about MESH at [http://mesh.cs.umn.edu/](http://mesh.cs.umn.edu/)

In order to run MESH, you need to apply Graphx Patch for MESH. Patch file can be accessed from [here](Graphx-Patch/meshGraphx_branch-1.6).

## Building MESH
*Note we are currently supporting MESH only for Graphx 1.6. So, in order to work with MESH, download graphx from spark branch-1.6.*

Prerequisites for building MESH:

* Unix-like environment ( We like and use Ubuntu )
* git
* Maven (we recommend version 3.3.9)
* Java 7 or 8
* Scala version 2.10.5


Patch for Graphx - Before using MESH, apply patch on Graphx and build the graphx library. Patch file can be accessed from [here](Graphx-Patch/meshGraphx_branch-1.6.patch).

```
git clone https://github.com/apache/spark.git
cd spark
git checkout branch-1.6
git apply meshGraphx_branch-1.6.patch
```
To build graphx jar, follow the documentation from Spark source, which can be accessed from [here](http://spark.apache.org/docs/1.6.0/building-spark.html). 

Next, add graphx jar into local maven repository. To do that, run:
```
mvn install:install-file -Dfile=<path-to-file> -DgroupId=spark-graphx-mesh -DartifactId=spark-graphx-mesh -Dversion=1.6.0 -Dpackaging=jar
```
**Where:**

* path-to-file: Path to the graphx-JAR to install

MESH is built using Apache Maven. To built MESH and its examples, run:
```
mvn -DskipTests clean package
```
## About
![](img/nsf_100x100.png) 

This material is based upon work supported by the National Science Foundation under Grant No. IIS-1422802 ( NSF Award Abstract). Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation. 

**MESH is developed by [Distributed Computing Systems Group](http://dcsg.cs.umn.edu/) at University of Minnesota, Twin Cities**
