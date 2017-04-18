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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingResult;

/**
 * Extends the nest operation by adding the edges as in the Join Disjunctive Semantics
 *
 * @see Nesting
 */
public class NestingWithDisjunctiveCompact extends Nesting {

  /**
   * A default id is generated
   */
  public NestingWithDisjunctiveCompact() {
    super(GradoopId.get());
  }

  /**
   * A default id is associated to the graph
   * @param id Id to be associated to the new graph
   */
  public NestingWithDisjunctiveCompact(GradoopId id) {
    super(id);
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph, GraphCollection collection) {
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

    // Evaluates the disjunctive semantics
    NestingResult iaf = model.nestWithDisjoint(graphIndex, collectionIndex, graphId);

    // Returns the expected graph, which is the nested graph with the new edges
    return toLogicalGraph(iaf, model.getFlattenedGraph());
  }

}
