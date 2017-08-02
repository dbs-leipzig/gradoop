package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;

public interface LogicalGraphLayoutFactory {

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param vertices  Vertex dataset
   * @return Logical graph
   */
  LogicalGraphLayout fromDataSets(DataSet<Vertex> vertices, GradoopFlinkConfig config);

  /**
   * Creates a logical graph from the given argument.
   * <p>
   * The method creates a new graph head element and assigns the vertices and
   * edges to that graph.
   *
   * @param vertices Vertex DataSet
   * @param edges    Edge DataSet
   * @return Logical graph
   */
  LogicalGraphLayout fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges,
    GradoopFlinkConfig config);

  /**
   * Creates a logical graph from the given arguments.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *
   * @param graphHead   1-element GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Logical graph
   */
  LogicalGraphLayout fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges, GradoopFlinkConfig config);

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param graphHead Graph head associated with the logical graph
   * @param vertices  Vertex collection
   * @param edges     Edge collection
   * @return Logical graph
   */
  LogicalGraphLayout fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges, GradoopFlinkConfig config);

  /**
   * Creates a logical graph from the given arguments. A new graph head is
   * created and all vertices and edges are assigned to that graph.
   *
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Logical graph
   */
  LogicalGraphLayout fromCollections(Collection<Vertex> vertices, Collection<Edge> edges,
    GradoopFlinkConfig config);

  /**
   * Creates an empty graph.
   *
   * @return empty graph
   */
  LogicalGraphLayout createEmptyGraph(GradoopFlinkConfig config);
}
