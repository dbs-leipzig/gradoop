package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Objects;

/**
 * Responsible to create instances of {@link GraphCollection} based on a specific
 * {@link org.gradoop.flink.model.api.layouts.GraphCollectionLayout}.
 */
public class GraphCollectionFactory {
  /**
   * Creates the layout from given data.
   */
  private GraphCollectionLayoutFactory layoutFactory;

  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;

  public GraphCollectionFactory(GradoopFlinkConfig config) {
    this.config = config;
  }
  /**
   * Sets the layout factory that is responsible for creating a graph collection layout.
   *
   * @param layoutFactory graph collection layout factory
   */
  public void setLayoutFactory(GraphCollectionLayoutFactory layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  /**
   * Creates a collection from the given datasets.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @return Graph collection
   */
  public GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices) {
    return new GraphCollection(layoutFactory.fromDataSets(graphHeads, vertices), config);
  }

  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Graph collection
   */
  public GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new GraphCollection(layoutFactory.fromDataSets(graphHeads, vertices, edges), config);
  }

  /**
   * Creates a collection layout from the given collections.
   *
   * @param graphHeads  Graph Head collection
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Graph collection
   */
  public GraphCollection fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges) {
    return new GraphCollection(layoutFactory.fromCollections(graphHeads, vertices, edges), config);
  }

  /**
   * Creates a graph collection from a given logical graph.
   *
   * @param logicalGraphLayout  input graph
   * @return 1-element graph collection
   */
  public GraphCollection fromGraph(LogicalGraph logicalGraphLayout) {
    return new GraphCollection(layoutFactory.fromGraphLayout(logicalGraphLayout), config);
  }

  /**
   * Creates a graph collection from a graph transaction dataset.
   *
   * Overlapping vertices and edge are merged by Id comparison only.
   *
   * @param transactions  transaction dataset
   * @return graph collection
   */
  public GraphCollection fromTransactions(DataSet<GraphTransaction> transactions) {
    return new GraphCollection(layoutFactory.fromTransactions(transactions), config);
  }

  /**
   * Creates a graph collection layout from graph transactions.
   *
   * Overlapping vertices and edge are merged using provided reduce functions.
   *
   * @param transactions        transaction dataset
   * @param vertexMergeReducer  vertex merge function
   * @param edgeMergeReducer    edge merge function
   * @return graph collection
   */
  public GraphCollection fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer) {
    return new GraphCollection(layoutFactory
      .fromTransactions(transactions, vertexMergeReducer, edgeMergeReducer), config);
  }

  /**
   * Creates an empty graph collection layout.
   *
   * @return empty graph collection layout
   */
  public GraphCollection createEmptyCollection() {
    return new GraphCollection(layoutFactory.createEmptyCollection(), config);
  }
}
