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

package org.gradoop.flink.model.impl.operators.join.joinwithfusion;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.join.JoinUtils;
import org.gradoop.flink.model.impl.operators.join.common.tuples.DisambiguationTupleWithVertexId;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.containers.Subgraphs;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.FilterVerticesAccordingToSemanticsAndMatching;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.MergeGraphHeads;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions
  .UpdateEdgesThoughToBeFusedVertices;
import org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions.VertexNotInSubgraph;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions
  .Value0OfDisambiguationTuple;

/**
 * Created by vasistas on 15/02/17.
 */
public class Testing {

  /**
   * Filters a collection form a graph dataset performing either an intersection or a difference
   *
   * @param collection Collection to be filtered
   * @param g          Graph where to verify the containment operation
   * @param inGraph    If the value is true, then perform an intersection, otherwise a difference
   * @param <P>        e.g. either vertices or edges
   * @return The filtered collection
   */
  public static <P extends GraphElement> DataSet<P> fusionUtilsAreElementsInGraph(DataSet<P>
    collection,
    LogicalGraph g, boolean inGraph) {
    return collection
      .filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
      .withBroadcastSet(g.getGraphHead().map(new Id<>()), GraphContainmentFilterBroadcast.GRAPH_ID);
  }


  public static DataSet<DisambiguationTupleWithVertexId> generateDisambiguation(DataSet<Vertex> left,
    DataSet<Tuple2<GradoopId, Vertex>> demultiplexedVertices,JoinType vertexJoinType,
    boolean isLeft) {
    return JoinUtils.joinByVertexEdge(left,demultiplexedVertices,vertexJoinType,isLeft)
      .where(new Id<>()).equalTo(new DemultiplexedEPGMToId<>())
      .with(new FilterVerticesAccordingToSemanticsAndMatching());
  }

  public static void main(String args[]) {
    GraphCollection patterns = null;
    LogicalGraph left = null;
    LogicalGraph right = null;
    LogicalGraph union = new Combination().execute(left,right);
    Function<Tuple2<String, String>, String> concatenateGraphHeads = null;
    Function<Tuple2<Properties, Properties>, Properties> concatenateProperties = null;
    JoinType vertexJoinType = null;
    // The resulting graph id
    final GradoopId fusedGraphId = GradoopId.get();


    // ---------------
    // - Step 1
    // ---------------
    // Part of the work here could be reduced
    Subgraphs sg = new Subgraphs(patterns,union,fusedGraphId);

    // ---------------
    // - Step 2
    // ---------------
    // For each operand, we shall return the final vertices
    DataSet<DisambiguationTupleWithVertexId> leftVerticesNotInPattern =
      left.getVertices().filter(new NotInGraphBroadcast<>())
        .withBroadcastSet(patterns.getGraphHeads().map(new Id<>()),
          GraphContainmentFilterBroadcast.GRAPH_ID)
        .map(new VertexNotInSubgraph());
    DataSet<DisambiguationTupleWithVertexId> rightVerticesNotInPattern =
      left.getVertices().filter(new NotInGraphBroadcast<>())
        .withBroadcastSet(patterns.getGraphHeads().map(new Id<>()),
          GraphContainmentFilterBroadcast.GRAPH_ID)
        .map(new VertexNotInSubgraph());

    // The head belonging to the final graph
    DataSet<GraphHead> finalHead = left.getGraphHead()
      .first(1)
      .join(right.getGraphHead().first(1))
      .where((GraphHead x)->0).equalTo((GraphHead y)->0)
      .with(new MergeGraphHeads(fusedGraphId, concatenateGraphHeads, concatenateProperties));

    // The set of vertices belonging to the final graph
    DataSet<DisambiguationTupleWithVertexId> finalVertices = sg.getDisambiguatedUpperLevelView();

    // -----------------------------------------------------------------
    // The final vertices that have to be used into the edge update fase
    // -----------------------------------------------------------------
    DataSet<DisambiguationTupleWithVertexId> upperLevelVerticesToPeek = patterns.getConfig().getExecutionEnvironment().fromElements();
    if (vertexJoinType.equals(JoinType.FULL_OUTER)||
        vertexJoinType.equals(JoinType.LEFT_OUTER)) {
      upperLevelVerticesToPeek = finalVertices.union(leftVerticesNotInPattern);
    }
    if (vertexJoinType.equals(JoinType.FULL_OUTER)||
      vertexJoinType.equals(JoinType.RIGHT_OUTER)) {
      if (upperLevelVerticesToPeek == null) {
        upperLevelVerticesToPeek = rightVerticesNotInPattern;
      } else {
        upperLevelVerticesToPeek = upperLevelVerticesToPeek.union(rightVerticesNotInPattern);
      }
    }
    upperLevelVerticesToPeek.union(sg.getZoomOutViewAsVertex());

    //

    DataSet<Edge> finalEdges = patterns.getEdges()
      // From all the patterns, extract only those vertices that appear in the union
      .filter(new InGraphBroadcast<>())
      .withBroadcastSet(union.getGraphHead().map(new Id<>()),GraphContainmentFilterBroadcast.GRAPH_ID)
      .fullOuterJoin(finalVertices)
      .where(new SourceId<>()).equalTo(new Id<>())
      /// XXXX
      .with(new UpdateEdgesThoughToBeFusedVertices(true))
      .fullOuterJoin(finalVertices)
      .where(new TargetId<>()).equalTo(new Id<>())
      /// XXXX
      .with(new UpdateEdgesThoughToBeFusedVertices(false));;

    LogicalGraph toret = LogicalGraph
      .fromDataSets(finalHead, finalVertices.map(new Value0OfDisambiguationTuple()), finalEdges, patterns.getConfig());
  }
  
}
