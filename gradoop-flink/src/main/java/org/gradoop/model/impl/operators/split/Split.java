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

package org.gradoop.model.impl.operators.split;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.UnaryFunction;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.PairTupleWithNewId;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.tuple.Project2To1;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.operators.split.functions.InitGraphHead;
import org.gradoop.model.impl.operators.split.functions.AddNewGraphsToVertex;
import org.gradoop.model.impl.operators.split.functions.SplitValues;
import org.gradoop.model.impl.operators.split.functions.JoinEdgeTupleWithSourceGraphs;
import org.gradoop.model.impl.operators.split.functions.JoinEdgeTupleWithTargetGraphs;
import org.gradoop.model.impl.operators.split.functions.JoinVertexIdWithGraphIds;
import org.gradoop.model.impl.operators.split.functions.MultipleGraphIdsGroupReducer;
import org.gradoop.model.impl.operators.split.functions.AddNewGraphsToEdge;
import org.gradoop.model.impl.properties.PropertyValue;

import java.io.Serializable;
import java.util.List;

/**
 * Splits a LogicalGraph into a GraphCollection based on user-defined property
 * values. The operator supports overlapping logical graphs, where a vertex
 * can be in more than one logical graph. Edges, where source and target vertex
 * have no graphs in common, are removed from the resulting collection.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class Split
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E>, Serializable {

  /**
   * User-defined function for value extraction
   */
  private final UnaryFunction<V, List<PropertyValue>> function;

  /**
   * Constructor
   *
   * @param function user-defined function
   */
  public Split(UnaryFunction<V, List<PropertyValue>> function) {
    this.function = function;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    //--------------------------------------------------------------------------
    // compute vertices
    //--------------------------------------------------------------------------

    // build tuples of vertex and split value, which will determine in which
    // new graph the vertices lie
    DataSet<Tuple2<GradoopId, PropertyValue>> vertexIdWithSplitValues =
      graph.getVertices()
        .flatMap(new SplitValues<>(function));

    // extract the split properties into a dataset
    DataSet<Tuple1<PropertyValue>> distinctSplitValues = vertexIdWithSplitValues
      .map(new Project2To1<GradoopId, PropertyValue>())
      .distinct();

    // generate one new unique GraphId per distinct split property
    DataSet<Tuple2<PropertyValue, GradoopId>> splitValuesWithGraphIds =
      distinctSplitValues
        .map(new PairTupleWithNewId<PropertyValue>());

    // build a dataset of the vertex ids and the new associated graph ids
    DataSet<Tuple2<GradoopId, GradoopIdSet>> vertexIdWithGraphIds =
      vertexIdWithSplitValues
        .join(splitValuesWithGraphIds)
        .where(1).equalTo(0)
        .with(new JoinVertexIdWithGraphIds())
        .groupBy(0)
        .reduceGroup(new MultipleGraphIdsGroupReducer());

    // add new graph ids to the initial vertex set
    DataSet<V> vertices = graph.getVertices()
      .join(vertexIdWithGraphIds)
      .where(new Id<V>()).equalTo(0)
      .with(new AddNewGraphsToVertex<V>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    // extract graph ids into a dataset
    DataSet<Tuple1<GradoopId>> newGraphIds = splitValuesWithGraphIds
      .map(new Project2To1<PropertyValue, GradoopId>());

    // add new graph id's to the initial graph set
    DataSet<G> newGraphs = newGraphIds
      .map(new InitGraphHead<>(
        graph.getConfig().getGraphHeadFactory()));

    //--------------------------------------------------------------------------
    // compute edges
    //--------------------------------------------------------------------------

    // replace source and target id by the graph list the corresponding vertex
    DataSet<Tuple3<E, GradoopIdSet, GradoopIdSet>> edgeGraphIdsGraphIds =
      graph.getEdges()
        .join(vertexIdWithGraphIds)
        .where(new SourceId<E>()).equalTo(0)
        .with(new JoinEdgeTupleWithSourceGraphs<E>())
        .join(vertexIdWithGraphIds)
        .where("f0.targetId").equalTo(0)
        .with(new JoinEdgeTupleWithTargetGraphs<E>());

    // add new graph ids to the edges iff source and target are contained in the
    // same graph
    DataSet<E> edges = edgeGraphIdsGraphIds
      .flatMap(new AddNewGraphsToEdge<E>());

    //--------------------------------------------------------------------------
    // return new graph collection
    //--------------------------------------------------------------------------

    return GraphCollection.fromDataSets(
      newGraphs, vertices, edges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Split.class.getName();
  }
}
