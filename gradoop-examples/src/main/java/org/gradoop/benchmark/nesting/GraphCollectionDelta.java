package org.gradoop.benchmark.nesting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Created by vasistas on 08/04/17.
 */
public class GraphCollectionDelta {

  private final DataSet<Tuple3<String, Boolean, GraphHead>> heads;
  private final DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertices;
  private final DataSet<Tuple2<ImportEdge<String>, GradoopId>> edges;

  public GraphCollectionDelta(DataSet<Tuple3<String, Boolean, GraphHead>> heads,
    DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertices,
    DataSet<Tuple2<ImportEdge<String>, GradoopId>> edges) {
    this.heads = heads;
    this.edges = edges;
    this.vertices = vertices;
  }

  public DataSet<Tuple3<String, Boolean, GraphHead>> getHeads() {
    return heads;
  }

  public DataSet<Tuple2<ImportVertex<String>, GradoopId>> getVertices() {
    return vertices;
  }

  public DataSet<Tuple2<ImportEdge<String>, GradoopId>> getEdges() {
    return edges;
  }

}
