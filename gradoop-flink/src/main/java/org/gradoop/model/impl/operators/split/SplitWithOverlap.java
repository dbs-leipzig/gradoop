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
import org.gradoop.model.impl.functions.tuple.Project2To1;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.split.functions.AddNewGraphsToGraphMapper;
import org.gradoop.model.impl.operators.split.functions.AddNewGraphsToVertexJoin;
import org.gradoop.model.impl.operators.split.functions.EdgeToTupleMapper;
import org.gradoop.model.impl.operators.split.functions.ExtractSplitValuesFlatMapper;
import org.gradoop.model.impl.operators.split.functions.GenerateNewGraphIdMapper;
import org.gradoop.model.impl.operators.split.functions.JoinEdgeTupleWithSourceGraphs;
import org.gradoop.model.impl.operators.split.functions.JoinEdgeTupleWithTargetGraphs;
import org.gradoop.model.impl.operators.split.functions.JoinVertexIdWithGraphIds;
import org.gradoop.model.impl.operators.split.functions.MultipleGraphIdsGroupReducer;
import org.gradoop.model.impl.operators.split.functions.AddNewGraphsToEdgeFlatMapper;
import org.gradoop.model.impl.properties.PropertyValue;

import java.io.Serializable;
import java.util.List;

/**
 * Split a LogicalGraph into an GraphCollection by a self defined property
 * value.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public class SplitWithOverlap
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E>, Serializable {

  /**
   * Self defined function for graph extraction
   */
  private final UnaryFunction<V, List<PropertyValue>> function;

  /**
   * Constructor
   *
   * @param function self defined function
   */
  public SplitWithOverlap(UnaryFunction<V, List<PropertyValue>> function) {
    this.function = function;
  }

  /**
   * Execute the operator, split the LogicalGraph into an EPGraphCollection
   * which graphs can be overlapping
   *
   * @param   graph the logicalGraph that will be split
   * @return  a GraphCollection containing the newly created LogicalGraphs
   */
  @Override
  public GraphCollection<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    //--------------------------------------------------------------------------
    // compute vertices
    //--------------------------------------------------------------------------

    // build tuple's of vertices and the split values, which will determine in
    // which new graph the vertices lie
    DataSet<Tuple2<GradoopId, PropertyValue>> vertexIdWithSplitValues =
      graph.getVertices().flatMap(new ExtractSplitValuesFlatMapper<>(function));

    // extract the split properties into a dataset
    DataSet<Tuple1<PropertyValue>> distinctSplitValues =
      vertexIdWithSplitValues
        .map(new Project2To1<GradoopId, PropertyValue>()).distinct();

    // generate one new unique GraphId per distinct split property
    DataSet<Tuple2<PropertyValue, GradoopId>> splitValuesWithGraphIds =
      distinctSplitValues.map(new GenerateNewGraphIdMapper());

    // build a dataset of the vertex id's and the new graph id's,
    // in which they lie
    DataSet<Tuple2<GradoopId, List<GradoopId>>> vertexIdWithGraphIds =
      vertexIdWithSplitValues
        .join(splitValuesWithGraphIds).where(1).equalTo(0)
        .with(new JoinVertexIdWithGraphIds()).groupBy(0)
        .reduceGroup(new MultipleGraphIdsGroupReducer());

    // add new graph id's to the initial vertex set
    DataSet<V> vertices = graph.getVertices()
      .join(vertexIdWithGraphIds)
      .where(new Id<V>()).equalTo(0)
      .with(new AddNewGraphsToVertexJoin<V>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    // extract graph id's into a dataset
    DataSet<Tuple1<GradoopId>> newGraphIds = splitValuesWithGraphIds
      .map(new Project2To1<PropertyValue, GradoopId>());

    // add new graph id's to the initial graph set
    DataSet<G> newGraphs = newGraphIds
      .map(new AddNewGraphsToGraphMapper<>(
        graph.getConfig().getGraphHeadFactory()));

    //--------------------------------------------------------------------------
    // compute edges
    //--------------------------------------------------------------------------

    // construct tuples of the edges with the ids of their source and target
    // vertices
    DataSet<Tuple3<E, GradoopId, GradoopId>> edgeSourceTarget =
      graph.getEdges().map(new EdgeToTupleMapper<E>());

    // replace the source vertex id by the graph list of this vertex
    DataSet<Tuple3<E, List<GradoopId>, GradoopId>> edgeGraphIdsTarget =
      edgeSourceTarget
        .join(vertexIdWithGraphIds)
        .where(1).equalTo(0)
        .with(new JoinEdgeTupleWithSourceGraphs<E>());

    // replace the target vertex id by the graph list of this vertex
    DataSet<Tuple3<E, List<GradoopId>, List<GradoopId>>> edgeGraphIdsGraphIds =
      edgeGraphIdsTarget
        .join(vertexIdWithGraphIds)
        .where(2).equalTo(0)
        .with(new JoinEdgeTupleWithTargetGraphs<E>());

    // add new graph id's to the edges iff source and target are part of the
    // same graph
    DataSet<E> edges = edgeGraphIdsGraphIds
      .flatMap(new AddNewGraphsToEdgeFlatMapper<E>());

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
    return SplitWithOverlap.class.getName();
  }
}
