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
package org.gradoop.flink.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.InitGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.PairElementWithNewId;
import org.gradoop.flink.model.impl.functions.tuple.Project2To1;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of4;
import org.gradoop.flink.model.impl.operators.subgraph.functions.AddGraphsToElements;
import org.gradoop.flink.model.impl.operators.subgraph.functions.AddGraphsToElementsCoGroup;
import org.gradoop.flink.model.impl.operators.subgraph.functions.EdgesWithNewGraphsTuple;
import org.gradoop.flink.model.impl.operators.subgraph.functions.ElementIdGraphIdTuple;
import org.gradoop.flink.model.impl.operators.subgraph.functions.FilterEdgeGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.IdSourceTargetGraphTuple;
import org.gradoop.flink.model.impl.operators.subgraph.functions.JoinTuplesWithNewGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.JoinWithSourceGraphIdSet;
import org.gradoop.flink.model.impl.operators.subgraph.functions.JoinWithTargetGraphIdSet;
import org.gradoop.flink.model.impl.operators.subgraph.functions.MergeEdgeGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.MergeTupleGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.SourceTargetIdGraphsTuple;

/**
 * Takes a collection of logical graphs and a user defined aggregate function as
 * input. The aggregate function is applied on each logical graph contained in
 * the collection and the aggregate is stored as an additional property at the
 * graphs.
 */
public class ApplySubgraph implements ApplicableUnaryGraphToGraphOperator {
  /**
   * Used to filter vertices from the logical graph.
   */
  private final FilterFunction<Vertex> vertexFilterFunction;
  /**
   * Used to filter edges from the logical graph.
   */
  private final FilterFunction<Edge> edgeFilterFunction;

  /**
   * Creates a new sub graph operator instance.
   * <p/>
   * If both parameters are not {@code null}, the operator returns the subgraph
   * defined by filtered vertices and edges.
   * <p/>
   * If the {@code edgeFilterFunction} is {@code null}, the operator returns the
   * vertex-induced subgraph.
   * <p/>
   * If the {@code vertexFilterFunction} is {@code null}, the operator returns
   * the edge-induced subgraph.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   */
  public ApplySubgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction) {
    if (vertexFilterFunction == null && edgeFilterFunction == null) {
      throw new IllegalArgumentException("No filter functions was given.");
    }
    this.vertexFilterFunction = vertexFilterFunction;
    this.edgeFilterFunction = edgeFilterFunction;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection executeForGVELayout(GraphCollection collection) {
    return vertexFilterFunction != null && edgeFilterFunction != null ?
      subgraph(collection) :
      vertexFilterFunction != null ? vertexInducedSubgraph(collection) :
        edgeInducedSubgraph(collection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection executeForTxLayout(GraphCollection collection) {
    return executeForGVELayout(collection);
  }

  /**
   * Returns one subgraph for each of the given super graphs.
   * The subgraphs are defined by the vertices that fulfil the vertex filter
   * function.
   *
   * @param collection collection of supergraphs
   * @return collection of vertex-induced subgraphs
   */
  private GraphCollection vertexInducedSubgraph(
    GraphCollection collection) {
    //--------------------------------------------------------------------------
    // compute a dictionary that maps the old graph ids to new ones
    //--------------------------------------------------------------------------

    DataSet<Tuple2<GradoopId, GradoopId>> graphIdDictionary = collection
      .getGraphHeads()
      .map(new Id<>())
      .map(new PairElementWithNewId<>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    EPGMGraphHeadFactory<GraphHead> graphFactory = collection.getConfig()
      .getGraphHeadFactory();

    DataSet<GraphHead> newGraphHeads = graphIdDictionary
      .map(new Project2To1<>())
      .map(new InitGraphHead(graphFactory));

    //--------------------------------------------------------------------------
    // compute pairs of a vertex id and the set of new graphs this vertex is
    // contained in
    // filter function is applied first to improve performance
    //--------------------------------------------------------------------------

    DataSet<Tuple2<GradoopId, GradoopIdSet>> vertexIdsWithNewGraphs =
      collection.getVertices()
        .filter(vertexFilterFunction)
        .flatMap(new ElementIdGraphIdTuple<>())
        .join(graphIdDictionary)
        .where(1)
        .equalTo(0)
        .with(new JoinTuplesWithNewGraphs())
        .groupBy(0)
        .reduceGroup(new MergeTupleGraphs());

    //--------------------------------------------------------------------------
    // compute new vertices
    //--------------------------------------------------------------------------

    DataSet<Vertex> newVertices = vertexIdsWithNewGraphs
        .join(collection.getVertices())
        .where(0)
        .equalTo(new Id<>())
        .with(new AddGraphsToElements<>());

    //--------------------------------------------------------------------------
    // build tuples4 for each edge, containing
    // edge id, source id, target id, set of new graph ids
    //--------------------------------------------------------------------------

    DataSet<Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdSet>> edgeTuple =
      collection.getEdges()
        .flatMap(new IdSourceTargetGraphTuple<>())
        .join(graphIdDictionary)
        .where(3).equalTo(0)
        .with(new EdgesWithNewGraphsTuple())
        .groupBy(new Value0Of4<>())
        .reduceGroup(new MergeEdgeGraphs());

    //--------------------------------------------------------------------------
    // join the tuple4 with vertex tuples, keeping the graph sets
    // apply a "filter" by using a flat map function to check for each edge
    // if a graph exists that this edge, its source and its target are contained
    // in
    // the result tuple consists of
    // edge id, new edge graphs
    //--------------------------------------------------------------------------

    DataSet<Tuple2<GradoopId, GradoopIdSet>> edgeIdsWithNewGraphs =
      edgeTuple
        .join(vertexIdsWithNewGraphs)
        .where(1).equalTo(0)
        .with(new JoinWithSourceGraphIdSet())
        .join(vertexIdsWithNewGraphs)
        .where(2).equalTo(0)
        .with(new JoinWithTargetGraphIdSet())
        .flatMap(new FilterEdgeGraphs());

    //--------------------------------------------------------------------------
    // compute the new edges
    //--------------------------------------------------------------------------

    DataSet<Edge> newEdges = edgeIdsWithNewGraphs
      .join(collection.getEdges())
      .where(0)
      .equalTo(new Id<>())
      .with(new AddGraphsToElements<>());

    return collection.getConfig().getGraphCollectionFactory()
      .fromDataSets(newGraphHeads, newVertices, newEdges);
  }

  /**
   * Returns one subgraph for each of the given supergraphs.
   * The subgraphs are defined by the edges that fulfil the vertex filter
   * function.
   *
   * @param collection collection of supergraphs
   * @return collection of edge-induced subgraphs
   */
  private GraphCollection edgeInducedSubgraph(
    GraphCollection collection) {
    //--------------------------------------------------------------------------
    // compute a dictionary that maps the old graph ids to new ones
    //--------------------------------------------------------------------------

    DataSet<Tuple2<GradoopId, GradoopId>> graphIdDictionary = collection
      .getGraphHeads()
      .map(new Id<>())
      .map(new PairElementWithNewId<>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    EPGMGraphHeadFactory<GraphHead> graphFactory = collection.getConfig()
      .getGraphHeadFactory();

    DataSet<GraphHead> newGraphHeads = graphIdDictionary
      .map(new Project2To1<>())
      .map(new InitGraphHead(graphFactory));

    //--------------------------------------------------------------------------
    // compute new edges by applying the filter function,
    // building tuples containing the edge id and the new graphs this edge is
    // contained in and finally adding the new graphs to the edges
    //--------------------------------------------------------------------------

    DataSet<Edge> newEdges = collection
      .getEdges()
      .filter(edgeFilterFunction)
      .flatMap(new ElementIdGraphIdTuple<>())
      .join(graphIdDictionary)
      .where(1)
      .equalTo(0)
      .with(new JoinTuplesWithNewGraphs())
      .groupBy(0)
      .reduceGroup(new MergeTupleGraphs())
      .join(collection.getEdges())
      .where(0)
      .equalTo(new Id<>())
      .with(new AddGraphsToElements<>());

    //--------------------------------------------------------------------------
    // compute the new vertices
    // first, tuples 2 containing the sources and targets of all new edges in
    // the first field and the new graphs this edge is contained in is created
    // this is then joined with the input vertices and the new graphs
    // are added to the vertices
    //--------------------------------------------------------------------------

    DataSet<Vertex> newVertices = newEdges
      .flatMap(new SourceTargetIdGraphsTuple<>())
      .distinct(0)
      .coGroup(collection.getVertices())
      .where(0)
      .equalTo(new Id<>())
      .with(new AddGraphsToElementsCoGroup<>());

    return collection.getConfig().getGraphCollectionFactory()
      .fromDataSets(newGraphHeads, newVertices, newEdges);
  }

  /**
   * Returns one subgraph for each of the given supergraphs.
   * The subgraphs are defined by the vertices that fulfil the vertex filter
   * function and edges that fulfill the edge filter function.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph.
   *
   * @param collection collection of supergraphs
   * @return collection of subgraphs
   */
  private GraphCollection subgraph(
    GraphCollection collection) {

    //--------------------------------------------------------------------------
    // compute a dictionary that maps the old graph ids to new ones
    //--------------------------------------------------------------------------

    DataSet<Tuple2<GradoopId, GradoopId>> graphIdDictionary = collection
      .getGraphHeads()
      .map(new Id<>())
      .map(new PairElementWithNewId<>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    EPGMGraphHeadFactory<GraphHead> graphFactory = collection.getConfig()
      .getGraphHeadFactory();

    DataSet<GraphHead> newGraphHeads = graphIdDictionary
      .map(new Project2To1<>())
      .map(new InitGraphHead(graphFactory));

    //--------------------------------------------------------------------------
    // compute pairs of a vertex and the set of new graphs this vertex is
    // contained in
    // filter function is applied first to improve performance
    //--------------------------------------------------------------------------

    DataSet<Vertex> newVertices =
      collection.getVertices()
        .filter(vertexFilterFunction)
        .flatMap(new ElementIdGraphIdTuple<>())
        .join(graphIdDictionary)
        .where(1).equalTo(0)
        .with(new JoinTuplesWithNewGraphs())
        .groupBy(0)
        .reduceGroup(new MergeTupleGraphs())
        .join(collection.getVertices())
        .where(0)
        .equalTo(new Id<>())
        .with(new AddGraphsToElements<>());

    //--------------------------------------------------------------------------
    // compute pairs of an edge and the set of new graphs this edge is
    // contained in
    // filter function is applied first to improve performance
    //--------------------------------------------------------------------------

    DataSet<Edge> newEdges = collection.getEdges()
      .filter(edgeFilterFunction)
      .flatMap(new ElementIdGraphIdTuple<>())
      .join(graphIdDictionary)
      .where(1)
      .equalTo(0)
      .with(new JoinTuplesWithNewGraphs())
      .groupBy(0)
      .reduceGroup(new MergeTupleGraphs())
      .join(collection.getEdges())
      .where(0)
      .equalTo(new Id<>())
      .with(new AddGraphsToElements<>());

    return collection.getConfig().getGraphCollectionFactory()
      .fromDataSets(newGraphHeads, newVertices, newEdges);
  }

  @Override
  public String getName() {
    return ApplySubgraph.class.getName();
  }
}
