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

package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.fusion.functions.GenerateTheFusedVertex;
import org.gradoop.flink.model.impl.operators.fusion.functions.UpdateEdgesThoughToBeFusedVertices;

/**
 * Created by Giacomo Bergami on 19/01/17.
 *
 * Fusion is a binary operator taking two graphs: a search graph (first parameter) and a
 * pattern graph (second parameter) [This means that this is not a symmetric operator
 * (F(a,b) != F(b,a))]. The general idea of this operator is that everything that appears
 * in the pattern graph is fused into a single vertex into the search graph. A new logical graph
 * is returned.
 *
 * 1) If any search graph is empty, an empty result will always be returned
 * 2) If a pattern graph is empty, then the search graph will always be returned
 * 3) The vertices of the pattern graph appearing in the first parameter are replaced by a
 *    new vertex. The old edges of the search graph are updated in the final result so that
 *    each vertex, either pointing to a to-be-fused vertex or starting from a to-be-fused vertex,
 *    are respectively updated as either pointing or starting from the fused vertex.
 * 4) Pattern graph's edges are also taken into account: any edge between to-be-fused vertices
 *    appearing in the search graph that are not expressed in the pattern graph are
 *    rendered as hooks over the fused vertex
 *
 */
public class Fusion implements BinaryGraphToGraphOperator {

  /**
   * @return The operator's name
   */
  @Override
  public String getName() {
    return Fusion.class.getName();
  }


  /**
   * Given a searchGraph and a patternGraph, returns the fused graph where pattern is replaced
   * inside the search by a single vertex having both label and properties belonging to the
   * pattern graph.
   *
   * @param searchGraph  Graph containing the actual data
   * @param patternGraph Graph containing the graph to be replaced within the search graph
   * @return A search graph containing vertices and edges logically belonging to the
   * search graph
   */
  @Override
  public LogicalGraph execute(final LogicalGraph searchGraph, final LogicalGraph patternGraph) {
    // I assume that both searchGraph and patternGraph are not
    DataSet<Vertex> leftVertices = searchGraph.getVertices();

    /*
     * Collecting the vertices that have to be removed and replaced by the aggregation's
     * result
     */
    DataSet<Vertex> toBeReplaced =
      FusionUtils.areElementsInGraph(leftVertices, patternGraph, true);

    // But, even the vertices that belong only to the search graph, should be added
    DataSet<Vertex> finalVertices =
      FusionUtils.areElementsInGraph(leftVertices, patternGraph, false);

    final GradoopId vId = GradoopId.get();

    // Then I create the graph that substitute the vertices within toBeReplaced
    DataSet<Vertex> toBeAdded = searchGraph.getGraphHead()
      .first(1)
      .join(patternGraph.getGraphHead().first(1))
      .where((GraphHead g) -> 0).equalTo((GraphHead g) -> 0)
      .with(new GenerateTheFusedVertex(vId));

    /*
     * The newly created vertex v has to be created iff. we have some actual vertices to be
     * replaced, and then if toBeReplaced contains at least one element
     */
    DataSet<Vertex> addOnlyIfNecessary = toBeReplaced
      .first(1)
      .join(toBeAdded)
      .where((Vertex x) -> 0).equalTo((Vertex x) -> 0)
      .with(new RightSide<>());

    DataSet<Vertex> toBeReturned = finalVertices.union(addOnlyIfNecessary);

    //In the final graph, all the edges appearing only in the search graph should appear
    DataSet<Edge> leftEdges = searchGraph.getEdges();
    leftEdges = FusionUtils.areElementsInGraph(leftEdges, patternGraph, false);

    /*
     * Concerning the other edges, we have to eventually update them and to be linked
     * with the new vertex
     * The following expression could be formalized as follows:
     *
     * updatedEdges = map(E, x ->
     *      e' <- onNextUpdateof(x).newfrom(x);
     *      if (e'.src \in toBeReplaced) e'.src = vId
     *      end if
     *      if (e'.dst \in toBeReplaced) e'.dst = vId
     *      end if
     *      return e'
     * )
     *
     */
    DataSet<Edge> updatedEdges = leftEdges
      .fullOuterJoin(toBeReplaced)
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new UpdateEdgesThoughToBeFusedVertices(vId, true))
      .fullOuterJoin(toBeReplaced)
      .where(new TargetId<>()).equalTo(new Id<>())
      .with(new UpdateEdgesThoughToBeFusedVertices(vId, false));

    // All's well what ends well… farewell!
    return LogicalGraph.fromDataSets(searchGraph.getGraphHead(), toBeReturned, updatedEdges,
        searchGraph.getConfig());
  }

}
