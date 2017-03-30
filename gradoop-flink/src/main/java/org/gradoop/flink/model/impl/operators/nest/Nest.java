package org.gradoop.flink.model.impl.operators.nest;

import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraph;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.model.FlatModel;
import org.gradoop.flink.model.impl.operators.nest.model.NestedIndexing;
import org.gradoop.flink.model.impl.operators.nest.model.NormalizedGraph;
import org.gradoop.flink.model.impl.operators.nest.operator.Nesting;
import org.gradoop.flink.model.impl.operators.nest.transformations
  .NestedIndexingToEPGMTransformations;

/**
 * Implements the nesting operator for the EPGM data model
 */
public class Nest implements GraphGraphCollectionToGraph {

  /**
   * The actual nesting operator over the data model
   */
  private final Nesting n;

  /**
   * Default constructor
   */
  public Nest() {
    this.n = new Nesting();
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public LogicalGraph execute(LogicalGraph left, GraphCollection collection) {
    /*
     * Creating the flat model containing the information of everything that happens, from the
     * left operand to the graph collection
     */
    GraphCollection groundTruth = GraphCollection.fromDataSets(
      left.getGraphHead().union(collection.getGraphHeads()),
      left.getVertices().union(collection.getVertices()),
      left.getEdges().union(collection.getEdges()),
      left.getConfig());
    FlatModel fm = new FlatModel(groundTruth);

    // The left operand defines which is the graph that is going to be nest
    NestedIndexing graph = new NestedIndexing(new NormalizedGraph(left));

    // The collection defines the elements that are going to be represented as an element
    NestedIndexing relations = new NestedIndexing(new NormalizedGraph(collection));

    // Running the actual operand
    NestedIndexing result = fm.run(n).with(graph,relations);

    // Converting the result to the standard EPGM model
    return NestedIndexingToEPGMTransformations.nestedIndexingToLogicalGraph(result,fm);
  }
}
