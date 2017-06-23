/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.nest;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.model.NestedModel;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingResult;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Implements the nest operator for the EPGM data model. Given a graph describing the groundtruth
 * information and a collection of graph representing the mined patterns, it returns a nested graph
 * where each vertex is either a vertex representing a graph in the graph collection that contains
 * at least one match with the ground truth or a non-matcher vertex. The edges from the former
 * graph are also inherited.
 */
public class Nesting extends NestingBase {

  /**
   * Nesting default constructor
   */
  public Nesting() {
    this(GradoopId.get());
  }

  /**
   * A default id is associated to the graph. No Graph Nestedmodel is set
   * @param graphId Id to be associated to the new graph
   */
  public Nesting(GradoopId graphId) {
    super(graphId);
  }

  /**
   * A default id is associated to the graph. No Graph Nestedmodel is set
   * @param graphId Id to be associated to the new graph
   * @param model   Model representing the nesting information
   */
  public Nesting(GradoopId graphId, NestedModel model) {
    super(graphId, model);
  }

  /**
   * Default internal initialization for the operator
   * @param graph        Graph to be nested
   * @param collection  Nesting information
   */
  protected void initialize(LogicalGraph graph, GraphCollection collection) {
    /*
     * Creating the flat model containing the information of everything that happens, from the
     * graph operand to the graph collection
     */
    if (model == null) {
      // Extracting the indexing structures for both graphs
      graphIndex = createIndex(graph);
      collectionIndex = createIndex(collection);

      model = generateModel(graph, collection, graphIndex, collectionIndex);
    }

    // At this step the FlatModel is never used, since I only change the index representation
    nest(model, graphIndex, collectionIndex, graphId);
  }


  /**
   * Returns…
   *
   * @return the intermediate indexing result, that could be used a next time for
   * updating the edges
   */
  public NestingResult getIntermediateResult() {
    return model.getPreviousResult();
  }

  /**
   * Returns…
   * @return  the indexing for the nest interface
   */
  public NestingIndex getCollectionIndex() {
    return collectionIndex;
  }

  /**
   * Returns…
   * @return  the id to be associated to the resulting graph
   */
  public GradoopId getGraphId() {
    return graphId;
  }

  /**
   * Returns…
   * @return the normalized graph containing the information for the whole graphs
   * within the graph
   */
  public LogicalGraph getFlattenedGraph() {
    return model.getFlattenedGraph();
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph, GraphCollection collection) {
    initialize(graph, collection);

    // Converting the result to the standard EPGM model
    return toLogicalGraph(model.getPreviousResult(), model.getFlattenedGraph());
  }

  @Override
  public DataSet<Hexaplet> getPreviousComputation() {
    return model == null ?
      null :
      model.getPreviousResult().getPreviousComputation();
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

}
