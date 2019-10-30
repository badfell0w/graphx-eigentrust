import java.util.Properties
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.abs
import scala.reflect.ClassTag


object EigenTrust {

  /**
   * Run a dynamic version of EigenTrust returning a graph with vertex attributes containing the
   * EigenTrust and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original satisfaction scores
   *
   * @param graph the graph on which to compute EigenTrust
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   * @param srcId the trusted peer Id
   *
   * @return the graph containing with each vertex containing the EigenTrust and each edge
   *         containing the normalized local trust values
   */
  def runUntilConvergence[VD: ClassTag, ED](
      graph: Graph[VD, Double], tol: Double, resetProb: Double,
      srcId: VertexId): Graph[Double, Double] =
  {
    require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    // create a temporary graph to set up graph
    val preGraph: Graph[(Double, Double), Double] = graph
      .partitionBy(PartitionStrategy.CanonicalRandomVertexCut)
      // collapse edges
      .groupEdges((one, two) => {
        (one + two)
      })
      // Set the vertex attributes to (initialTrust, delta)
      .mapVertices { (id, attr) =>
        if (id == srcId) (1.0, Double.PositiveInfinity) else (0.0, Double.PositiveInfinity)
      }
    .cache()

    // set edges to normalized local trust values
    val normedEdges = preGraph.collectEdges(EdgeDirection.Out).map( ve => {
      val normalization = ve._2.map( e => abs(e.attr):Double ).reduce( (a,b) => a + b )
      val normedEdges = ve._2.map( e => Edge(e.srcId, e.dstId, e.attr / normalization))
      normedEdges
    }).flatMap(identity(_))

    val outDegrees = preGraph.vertices.leftZipJoin(graph.outDegrees) { (vid, a, b) => b.getOrElse(0) }
    val sinks = outDegrees.filter( t => t._2 == 0).map( t => t._1 )
    val peerEdges = sinks.map( id => org.apache.spark.graphx.Edge(id, 1, 1.0))
    val allEdges = normedEdges ++ peerEdges
    val eigentrustGraph = Graph.fromEdges(allEdges, (0.0, Double.PositiveInfinity)).cache()

    def vertexProgram(id: VertexId, attr: (Double, Double),
      msgs: Array[(Long, Double)]): (Double, Double) = {
      val msgSum = msgs.map( e => {
          e._2
      }).reduce((a, b) => a + b )
      val newPR = if (id == srcId) {
        (1.0 - resetProb) * msgSum + resetProb
      } else {
        (1.0 - resetProb) * msgSum
      }
      val (oldPR, lastDelta) = attr
      (newPR, abs(newPR - oldPR))
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, Array((edge.srcId, edge.srcAttr._1 * edge.attr)) ))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Array[(Long, Double)], b: Array[(Long, Double)] ): Array[(Long, Double)] = a ++ b 

    // The initial message received by all vertices
    val initialMessage = Array((-1L, 1.0 / eigentrustGraph.numVertices ))

    val vp = {
      (id: VertexId, attr: (Double, Double), msgSum: Array[(Long, Double)]) =>
        vertexProgram(id, attr, msgSum)
    }

    val result = Pregel(eigentrustGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
    
    result
  }

}
