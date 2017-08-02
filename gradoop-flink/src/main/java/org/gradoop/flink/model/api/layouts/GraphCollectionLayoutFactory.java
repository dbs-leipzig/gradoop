package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;

public interface GraphCollectionLayoutFactory {

  /**
   * Creates a collection layout from the given arguments.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @return Graph collection
   */
  GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    GradoopFlinkConfig config);

  /**
   * Creates a collection layout from the given arguments.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Graph collection
   */
  GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges, GradoopFlinkConfig config);

  /**
   * Creates a collection layout from the given collections.
   *
   * @param graphHeads  Graph Head collection
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Graph collection
   */
  GraphCollectionLayout fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges, GradoopFlinkConfig config);

  /**
   * Creates a graph collection from a given logical graph.
   *
   * @param logicalGraphLayout  input graph
   * @return 1-element graph collection
   */
  GraphCollectionLayout fromGraphLayout(LogicalGraphLayout logicalGraphLayout,
    GradoopFlinkConfig config);

  /**
   * Creates a graph collection from a graph transaction dataset.
   * Overlapping vertices and edge are merged by Id comparison only.
   *
   * @param transactions  transaction dataset
   * @return graph collection
   */
  GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GradoopFlinkConfig config);

  /**
   * Creates a graph collection layout from graph transactions.
   * Overlapping vertices and edge are merged using provided reduce functions.
   *
   * @param transactions        transaction dataset
   * @param vertexMergeReducer  vertex merge function
   * @param edgeMergeReducer    edge merge function
   * @return graph collection
   */
  GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer,
    GradoopFlinkConfig config);

  /**
   * Creates an empty graph collection layout.
   *
   * @return empty graph collection layout
   */
  GraphCollectionLayout createEmptyCollection(GradoopFlinkConfig config);
}
