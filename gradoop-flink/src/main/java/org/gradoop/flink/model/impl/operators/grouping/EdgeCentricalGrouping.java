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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildEdgeWithSuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperEdges;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexWithSuperVertexAndEdge;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceSuperEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;



import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeWithSuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertexAndEdge;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class EdgeCentricalGrouping extends CentricalGrouping {

  private boolean sourceSpecificGrouping;

  private boolean targetSpecificGrouping;

  public EdgeCentricalGrouping(List<String> primaryGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> primaryAggregators, List<String> secondaryGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> secondaryAggregators,
    GroupingStrategy groupingStrategy, Boolean sourceSpecificGrouping,
    Boolean targetSpecificGrouping) {
    super(primaryGroupingKeys, useVertexLabels, primaryAggregators, secondaryGroupingKeys,
      useEdgeLabels, secondaryAggregators, groupingStrategy);
    this.sourceSpecificGrouping = sourceSpecificGrouping;
    this.targetSpecificGrouping = targetSpecificGrouping;
  }

  @Override
  protected LogicalGraph groupReduce(LogicalGraph graph) {

    DataSet<EdgeWithSuperEdgeGroupItem> edgesForGrouping = graph.getEdges()
      // map edg to edge group item
      .map(new BuildEdgeWithSuperEdgeGroupItem(getEdgeGroupingKeys(), useEdgeLabels(),
        getEdgeAggregators()));

    //group vertices by label / properties / both
    // additionally: source specific / target specific / both
    DataSet<SuperEdgeGroupItem> superEdgeGroupItems = groupSuperEdges(edgesForGrouping,
      sourceSpecificGrouping, targetSpecificGrouping)
      //apply aggregate function
      .reduceGroup(new ReduceSuperEdgeGroupItems(useEdgeLabels(), getEdgeAggregators(),
        sourceSpecificGrouping, targetSpecificGrouping));

    // TODO edgeitem -> set<GradoopId>+edgeid -> groupby set -> set,edgeid,superVid ->..
    DataSet<Tuple3<Set<GradoopId>, GradoopId, GradoopId>> vertexWithSuper = superEdgeGroupItems
      //get all resulting (maybe concatenated) vertices
      //vertexIds - superedgeId
      .flatMap(new FlatMapFunction<SuperEdgeGroupItem, Tuple2<Set<GradoopId>, GradoopId>>() {
        @Override
        public void flatMap(SuperEdgeGroupItem superEdgeGroupItem,
          Collector<Tuple2<Set<GradoopId>, GradoopId>> collector) throws Exception {
          collector.collect(new Tuple2<Set<GradoopId>, GradoopId>(superEdgeGroupItem.getSourceIds(), superEdgeGroupItem.getEdgeId()));
          collector.collect(new Tuple2<Set<GradoopId>, GradoopId>(superEdgeGroupItem.getTargetIds(), superEdgeGroupItem.getEdgeId()));
        }
      })
      .groupBy(1)
      //assign supervertex id
      //vertexIds - superVId - edgeId
      .reduceGroup(new GroupReduceFunction<Tuple2<Set<GradoopId>,GradoopId>,
        Tuple3<Set<GradoopId>, GradoopId, GradoopId>>() {
        @Override
        public void reduce(Iterable<Tuple2<Set<GradoopId>, GradoopId>> iterable,
          Collector<Tuple3<Set<GradoopId>, GradoopId, GradoopId>> collector) throws Exception {

          Tuple2<Set<GradoopId>, GradoopId> supervertex = new Tuple2<Set<GradoopId>, GradoopId>
            (iterable.iterator().next().f0, GradoopId.get());

          Iterator<Tuple2<Set<GradoopId>, GradoopId>> iterator = iterable.iterator();
          while (iterator.hasNext()) {
            collector.collect(new Tuple3<>(supervertex.f0, supervertex.f1, iterator.next().f1));
          }

        }
      });


    DataSet<Edge> superEdges = superEdgeGroupItems
      .coGroup(vertexWithSuper)
      .where(0)
      .equalTo(2)
      // build super edges
      .with(new BuildSuperEdges(getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregators(),
        config.getEdgeFactory()));

    //TODO replace with super vertices
    DataSet<Vertex> superVertices = graph.getVertices();

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
