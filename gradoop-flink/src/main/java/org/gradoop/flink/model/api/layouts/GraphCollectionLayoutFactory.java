package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Collection;

/**
 * Enables the construction of a {@link GraphCollectionLayout}.
 */
public interface GraphCollectionLayoutFactory extends BaseLayoutFactory {
  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @return Graph collection
   */
  GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices);

  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Graph collection
   */
  GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges);

  /**
   * Creates a collection layout from the given collections.
   *
   * @param graphHeads  Graph Head collection
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Graph collection
   */
  GraphCollectionLayout fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges);

  /**
   * Creates a graph collection layout from a given logical graph layout.
   *
   * @param logicalGraphLayout  input graph
   * @return 1-element graph collection
   */
  GraphCollectionLayout fromGraphLayout(LogicalGraphLayout logicalGraphLayout);

  /**
   * Creates a graph collection layout from a graph transaction dataset.
   *
   * Overlapping vertices and edge are merged by Id comparison only.
   *
   * @param transactions  transaction dataset
   * @return graph collection
   */
  GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions);

  /**
   * Creates a graph collection layout from graph transactions.
   *
   * Overlapping vertices and edge are merged using provided reduce functions.
   *
   * @param transactions        transaction dataset
   * @param vertexMergeReducer  vertex merge function
   * @param edgeMergeReducer    edge merge function
   * @return graph collection layout
   */
  GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer);

  /**
   * Creates an empty graph collection layout.
   *
   * @return empty graph collection layout
   */
  GraphCollectionLayout createEmptyCollection();
}
