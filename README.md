# graphx-eigentrust
Implementation of EigenTrust for GraphX

Usage:
```scala
    val edges = sc.makeRDD(Array(
      Edge(1L, 2L, 1.0),
      Edge(2L, 3L, 0.8),
      Edge(2L, 4L, -1.0)
      ))
    val graph = Graph.fromEdges(edges, 0.0)
    val result = EigenTrust.runUntilConvergence(graph, 0.001, 0.1, 1L)
```
