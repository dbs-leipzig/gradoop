package org.gradoop.flink.model.impl.nested.datastructures;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.nested.datastructures.equality.NormalizedGraphEquality;
import org.gradoop.flink.model.impl.nested.utils.MapGraphHeadAsVertex;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.util.GradoopFlinkConfig;
import sun.rmi.runtime.Log;

/**
 * The normalization process consists of creating a new Graph containing a vertex per graph
 * head. This is required by the new nested graph model, where to each graph head corresponds
 * a vertex.
 */
public class NormalizedGraph {

  private DataSet<GraphHead> heads;
  private DataSet<Vertex> vertices;
  private DataSet<Edge> edges;
  private GradoopFlinkConfig conf;

  /**
   * Creates a normalized graph from a graph collection.
   * @param lg    Graph Collection to be normalized
   */
  public NormalizedGraph(GraphCollection lg) {
    vertices = lg.getGraphHeads()
      .map(new MapGraphHeadAsVertex())
      .union(lg.getVertices())
      .distinct(new Id<>()); // Keep only 1 if such vertex already exists
    heads = lg.getGraphHeads();
    edges = lg.getEdges();
    conf = lg.getConfig();
  }

  /**
   * Creates a normalized graph from a logical graph.
   * @param lg  Logical Graph to be normalized
   */
  public NormalizedGraph(LogicalGraph lg) {
    vertices = lg.getGraphHead()
      .map(new MapGraphHeadAsVertex())
      .union(lg.getVertices())
      .distinct(new Id<>()); // Keep only 1 if such vertex already exists
    heads = lg.getGraphHead();
    edges = lg.getEdges();
    conf = lg.getConfig();
  }

  public NormalizedGraph(DataSet<GraphHead> heads, DataSet<Vertex> vertices, DataSet<Edge> edges,
    GradoopFlinkConfig conf) {
    this.heads = heads;
    this.vertices = vertices;
    this.edges = edges;
    this.conf = conf;
  }


  public DataSet<GraphHead> getGraphHeads() {
    return heads;
  }

  public DataSet<Vertex> getVertices() {
    return vertices;
  }

  public DataSet<Edge> getEdges() {
    return edges;
  }

  public GradoopFlinkConfig getConfig() {
    return conf;
  }

  public void updateEdgesWithUnion(DataSet<Edge> edges) {
    this.edges = this.edges.union(edges);
  }

  public DataSet<Boolean> equalsByData(NormalizedGraph research) {
      return new NormalizedGraphEquality(
        new GraphHeadToDataString(),
        new VertexToDataString(),
        new EdgeToDataString(), true).execute(this, research);
  }
}
