package org.gradoop.model.impl.algorithms.btg;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.algorithms.btg.functions.BTGMessageFunction;
import org.gradoop.model.impl.algorithms.btg.functions.BTGUpdateFunction;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGVertexValue;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Graph-BSP Implementation of the Business Transaction Graph (BTG) Extraction
 * algorithm described in "BIIIG: Enabling Business Intelligence with Integrated
 * Instance Graphs". The input graph is a so called Integrated Instance Graph
 * (IIG) which contains nodes belonging to two different classes: master data
 * and transactional data.
 * <p/>
 * A BTG is a sub-graph of the IIG which has only master data nodes as boundary
 * nodes and transactional data nodes as inner nodes. The algorithm finds all
 * BTGs inside a given IIG. In the business domain, a BTG describes a specific
 * case inside a set of business cases involving master data like Employees,
 * Customers and Products and transactional data like SalesOrders, ProductOffers
 * or Purchases.
 * <p/>
 * The algorithm is based on the idea of finding connected components by
 * communicating the minimum vertex id inside a connected sub-graph and storing
 * it. The minimum id inside a sub-graph is the BTG id. Only transactional data
 * nodes are allowed to send ids, so the master data nodes work as a
 * communication barrier between BTGs. The master data nodes receive messages
 * from transactional data nodes out of BTGs in which they are involved. They
 * store the minimum incoming BTG id by vertex id in a map and when the
 * algorithm terminates, the set of unique values inside this map is the set of
 * BTG ids the master data node is involved in.
 */
public class BTGAlgorithm implements
  GraphAlgorithm<GradoopId, BTGVertexValue, NullValue,
    Graph<GradoopId, BTGVertexValue, NullValue>> {
  /**
   * Max Iteration counter for the Algorithm
   */
  private int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations maximal Iterations of Algorithm
   */
  public BTGAlgorithm(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Graph<GradoopId, BTGVertexValue, NullValue> run(
    Graph<GradoopId, BTGVertexValue, NullValue> graph) throws Exception {
    Graph<GradoopId, BTGVertexValue, NullValue> undirectedGraph =
      graph.getUndirected();
    // initialize vertex values and run the Vertex Centric Iteration
    return undirectedGraph.runVertexCentricIteration(
      new BTGUpdateFunction(),
      new BTGMessageFunction(),
      maxIterations);
  }
}
