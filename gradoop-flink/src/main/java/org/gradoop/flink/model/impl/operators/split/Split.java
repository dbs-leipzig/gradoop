/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.split;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.InitGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.PairTupleWithNewId;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.tuple.Project2To1;
import org.gradoop.flink.model.impl.operators.split.functions.AddNewGraphsToEdge;
import org.gradoop.flink.model.impl.operators.split.functions.AddNewGraphsToVertex;
import org.gradoop.flink.model.impl.operators.split.functions.JoinEdgeTupleWithSourceGraphs;
import org.gradoop.flink.model.impl.operators.split.functions.JoinEdgeTupleWithTargetGraphs;
import org.gradoop.flink.model.impl.operators.split.functions.JoinVertexIdWithGraphIds;
import org.gradoop.flink.model.impl.operators.split.functions.MultipleGraphIdsGroupReducer;
import org.gradoop.flink.model.impl.operators.split.functions.SplitValues;

import java.io.Serializable;
import java.util.List;

/**
 * Splits a LogicalGraph into a GraphCollection based on user-defined property
 * values. The operator supports overlapping logical graphs, where a vertex
 * can be in more than one logical graph. Edges, where source and target vertex
 * have no graphs in common, are removed from the resulting collection.
 */
public class Split implements UnaryGraphToCollectionOperator, Serializable {

  /**
   * User-defined function for value extraction
   */
  private final Function<Vertex, List<PropertyValue>> function;

  /**
   * Constructor
   *
   * @param function user-defined function
   */
  public Split(Function<Vertex, List<PropertyValue>> function) {
    this.function = function;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection execute(LogicalGraph graph) {

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
      .map(new Project2To1<>())
      .distinct();

    // generate one new unique GraphId per distinct split property
    DataSet<Tuple2<PropertyValue, GradoopId>> splitValuesWithGraphIds =
      distinctSplitValues
        .map(new PairTupleWithNewId<>());

    // build a dataset of the vertex ids and the new associated graph ids
    DataSet<Tuple2<GradoopId, GradoopIdSet>> vertexIdWithGraphIds =
      vertexIdWithSplitValues
        .join(splitValuesWithGraphIds)
        .where(1).equalTo(0)
        .with(new JoinVertexIdWithGraphIds())
        .groupBy(0)
        .reduceGroup(new MultipleGraphIdsGroupReducer());

    // add new graph ids to the initial vertex set
    DataSet<Vertex> vertices = graph.getVertices()
      .join(vertexIdWithGraphIds)
      .where(new Id<>()).equalTo(0)
      .with(new AddNewGraphsToVertex<>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    // extract graph ids into a dataset
    DataSet<Tuple1<GradoopId>> newGraphIds = splitValuesWithGraphIds
      .map(new Project2To1<>());

    // add new graph id's to the initial graph set
    DataSet<GraphHead> newGraphs = newGraphIds
      .map(new InitGraphHead(graph.getConfig().getGraphHeadFactory()));

    //--------------------------------------------------------------------------
    // compute edges
    //--------------------------------------------------------------------------

    // replace source and target id by the graph list the corresponding vertex
    DataSet<Tuple3<Edge, GradoopIdSet, GradoopIdSet>> edgeGraphIdsGraphIds =
      graph.getEdges()
        .join(vertexIdWithGraphIds)
        .where(new SourceId<>()).equalTo(0)
        .with(new JoinEdgeTupleWithSourceGraphs<>())
        .join(vertexIdWithGraphIds)
        .where("f0.targetId").equalTo(0)
        .with(new JoinEdgeTupleWithTargetGraphs<>());

    // add new graph ids to the edges iff source and target are contained in the
    // same graph
    DataSet<Edge> edges = edgeGraphIdsGraphIds
      .flatMap(new AddNewGraphsToEdge<>());

    //--------------------------------------------------------------------------
    // return new graph collection
    //--------------------------------------------------------------------------

    return graph.getConfig().getGraphCollectionFactory()
      .fromDataSets(newGraphs, vertices, edges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Split.class.getName();
  }
}
