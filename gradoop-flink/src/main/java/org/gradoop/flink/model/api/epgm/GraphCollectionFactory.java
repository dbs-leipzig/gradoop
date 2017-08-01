package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.transactional.GraphTransactions;

import java.util.Collection;

public interface GraphCollectionFactory {

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @return Graph collection
   */
  GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices);

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Graph collection
   */
  GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges);

  /**
   * Creates a new graph collection from the given collection.
   *
   * @param graphHeads  Graph Head collection
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Graph collection
   */
  GraphCollection fromCollections(Collection<GraphHead> graphHeads, Collection<Vertex> vertices,
    Collection<Edge> edges);

  /**
   * Creates a graph collection from a given logical graph.
   *
   * @param logicalGraph  input graph
   * @return 1-element graph collection
   */
  GraphCollection fromGraph(LogicalGraph logicalGraph);

  /**
   * Creates a graph collection from a graph transaction dataset.
   * Overlapping vertices and edge are merged by Id comparison only.
   *
   * @param transactions  transaction dataset
   * @return graph collection
   */
  GraphCollection fromTransactions(GraphTransactions transactions);

  /**
   * Creates a graph collection from a graph transaction dataset.
   * Overlapping vertices and edge are merged using provided reduce functions.
   *
   * @param transactions        transaction dataset
   * @param vertexMergeReducer  vertex merge function
   * @param edgeMergeReducer    edge merge function
   * @return graph collection
   */
  GraphCollection fromTransactions(
    GraphTransactions transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer);

  /**
   * Creates an empty graph collection.
   *
   * @return empty graph collection
   */
  GraphCollection createEmptyCollection();
}
