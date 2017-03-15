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

package org.gradoop.flink.model.impl.operators.grouping;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildEdgeWithSuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperEdges;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceSuperEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.UpdateSuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.operators.SetInTupleKeySelector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeWithSuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

import java.util.List;
import java.util.Set;

/**
 *
 */
public class EdgeCentricalGrouping extends CentricalGrouping {

  /**
   * True, iff the source vertex shall be considered for grouping.
   */
  private boolean sourceSpecificGrouping;

  /**
   * True, iff the target vertex shall be considered for grouping.
   */
  private boolean targetSpecificGrouping;

  /**
   * Creates an edge grouping grouping operator instance.
   *
   * @param vertexGroupingKeys      property keys to group vertices
   * @param useVertexLabels         group on vertex label true/false
   * @param vertexAggregators       aggregate functions for grouped vertices
   * @param edgeGroupingKeys        property keys to group edges
   * @param useEdgeLabels           group on edge label true/false
   * @param edgeAggregators         aggregate functions for grouped edges
   * @param groupingStrategy        group by vertices or edges
   * @param sourceSpecificGrouping  set true to consider source on grouping
   * @param targetSpecificGrouping  set true to consider target on grouping
   */
  public EdgeCentricalGrouping(List<String> vertexGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> vertexAggregators, List<String> edgeGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> edgeAggregators,
    GroupingStrategy groupingStrategy, Boolean sourceSpecificGrouping,
    Boolean targetSpecificGrouping) {
    super(vertexGroupingKeys, useVertexLabels, vertexAggregators, edgeGroupingKeys,
      useEdgeLabels, edgeAggregators, groupingStrategy);
    this.sourceSpecificGrouping = sourceSpecificGrouping;
    this.targetSpecificGrouping = targetSpecificGrouping;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph groupReduce(LogicalGraph graph) {

    DataSet<EdgeWithSuperEdgeGroupItem> edgesForGrouping = graph.getEdges()
      // map edge to edge group item
      .map(new BuildEdgeWithSuperEdgeGroupItem(getEdgeGroupingKeys(), useEdgeLabels(),
        getEdgeAggregators()));

    // group edges by label / properties / both
    // additionally: source specific / target specific / both
    DataSet<SuperEdgeGroupItem> superEdgeGroupItems = groupSuperEdges(edgesForGrouping,
      sourceSpecificGrouping, targetSpecificGrouping)
      // apply aggregate function
      .reduceGroup(new ReduceSuperEdgeGroupItems(useEdgeLabels(), getEdgeAggregators(),
        sourceSpecificGrouping, targetSpecificGrouping));

    // vertexIds - superVId - edgeId
    DataSet<SuperVertexGroupItem> superVertexGroupItems = superEdgeGroupItems
      // get all resulting (maybe concatenated) vertices
      // vertexIds - superedgeId
      .flatMap(new FlatMapFunction<SuperEdgeGroupItem, Tuple2<Set<GradoopId>, GradoopId>>() {
        @Override
        public void flatMap(SuperEdgeGroupItem superEdgeGroupItem,
          Collector<Tuple2<Set<GradoopId>, GradoopId>> collector) throws Exception {
          Tuple2<Set<GradoopId>, GradoopId> reuseTuple;
          reuseTuple = new Tuple2<Set<GradoopId>, GradoopId>();
          reuseTuple.setFields(
            superEdgeGroupItem.getSourceIds(),
            superEdgeGroupItem.getEdgeId());
          collector.collect(reuseTuple);

          reuseTuple.setFields(
            superEdgeGroupItem.getTargetIds(),
            superEdgeGroupItem.getEdgeId()
          );
          collector.collect(reuseTuple);
        }
      })
      .groupBy(new SetInTupleKeySelector<Tuple2<Set<GradoopId>, GradoopId>, GradoopId>(0))
//      .groupBy(0)
      // assign supervertex id
      // vertexIds - superVId - edgeId - label - groupingVal - aggregatVal (last 3 are empty)
      .reduceGroup(new BuildSuperVertexGroupItem());

    // create super edges based on grouped edges and resulting super vertex ids
    DataSet<Edge> superEdges = superEdgeGroupItems
      .coGroup(superVertexGroupItems)
      // same super vertex id
      .where(0)
      .equalTo(2)
      // build super edges
      .with(new BuildSuperEdges(getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregators(),
        config.getEdgeFactory()));

    // store vertices, where the vertex is its own super vertex, so the vertex stays itself
    DataSet<Vertex> normalSuperVertices = superVertexGroupItems
      // filter ids where vertex is its own super vertex
      .filter(new FilterFunction<SuperVertexGroupItem>() {
        @Override
        public boolean filter(
          SuperVertexGroupItem superVertexGroupItem) throws
          Exception {
          return (superVertexGroupItem.f0.size() == 1) &&
            (superVertexGroupItem.f0.iterator().next() == superVertexGroupItem.f1);
        }
      })
      // take vertex respectively to the filtered id
      .rightOuterJoin(graph.getVertices())
      .where(1)
      .equalTo(new Id<>())
      .with(new JoinFunction<SuperVertexGroupItem, Vertex, Vertex>() {
        @Override
        public Vertex join(SuperVertexGroupItem superVertexGroupItem, Vertex vertex)
          throws Exception {
          return vertex;
        }
      });

    // get for a super vertex id each single relevant vertex id
    DataSet<VertexWithSuperVertex> vertexWithSuper = superVertexGroupItems
      // only real super vertices
      .filter(new FilterFunction<SuperVertexGroupItem>() {
        @Override
        public boolean filter(
          SuperVertexGroupItem tuple) throws
          Exception {
          return (tuple.f0.size() > 1);
        }
      })
      // assign the vertex ids
      .flatMap(new FlatMapFunction<SuperVertexGroupItem, VertexWithSuperVertex>() {
        VertexWithSuperVertex vertexWithSuperVertex = new VertexWithSuperVertex();
        @Override
        public void flatMap(SuperVertexGroupItem superVertexGroupItem,
          Collector<VertexWithSuperVertex> collector) throws Exception {

          vertexWithSuperVertex.setSuperVertexId(superVertexGroupItem.getSuperVertexId());
          for (GradoopId gradoopId : superVertexGroupItem.getVertexIds()) {
            vertexWithSuperVertex.setVertexId(gradoopId);
            collector.collect(vertexWithSuperVertex);
          }
        }
      });


    superVertexGroupItems = superVertexGroupItems
      // take all vertices by their id, which is stored in the super vertex group item
      .coGroup(vertexWithSuper
        .rightOuterJoin(graph.getVertices())
        .where(0).equalTo(new Id<>())
        .with(new JoinFunction<VertexWithSuperVertex, Vertex, Tuple2<GradoopId, Vertex>>() {
          @Override
          public Tuple2<GradoopId, Vertex> join(VertexWithSuperVertex vertexWithSuperVertex, Vertex
            vertex) throws Exception {
            return new Tuple2<GradoopId, Vertex>(vertexWithSuperVertex.getSuperVertexId(), vertex);
          }
        }))
      .where(1).equalTo(0)
      // and aggregate the super vertex group item based on the vertex aggregations
      .with(new UpdateSuperVertexGroupItem(getVertexGroupingKeys(), useVertexLabels(),
        getVertexAggregators()));


    DataSet<Vertex> superVertices = superVertexGroupItems
      // take super vertex ids where vertex is not its own super vertex
      .filter(new FilterFunction<SuperVertexGroupItem>() {
        @Override
        public boolean filter(
          SuperVertexGroupItem tuple) throws
          Exception {
          return (tuple.f0.size() > 1);
        }
      })
      // and create the vertex element
      .map(new BuildSuperVertices(getVertexGroupingKeys(), useVertexLabels(),
        getVertexAggregators(), config.getVertexFactory()))
      // union with vertices which stay as they are
      .union(normalSuperVertices);

    return LogicalGraph.fromDataSets(superVertices, superEdges, graph.getConfig());
  }

  @Override
  protected LogicalGraph groupCombine(LogicalGraph graph) {
    return null;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return EdgeCentricalGrouping.class.getName() + ":" + getGroupingStrategy();
  }
}