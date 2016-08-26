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
package org.gradoop.flink.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators
  .ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.tuple.Project2To1;
import org.gradoop.flink.model.impl.operators.split.functions.InitGraphHead;
import org.gradoop.flink.model.impl.operators.subgraph.functions
  .AddGraphsToElementsCoGroup;
import org.gradoop.flink.model.impl.operators.subgraph.functions.FilterEdgeGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.JoinWithTargetGraphIdSet;
import org.gradoop.flink.model.impl.operators.subgraph.functions.SourceTargetIdGraphsTuple;

import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.PairElementWithNewId;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.operators.subgraph.functions.AddGraphsToElements;

import org.gradoop.flink.model.impl.operators.subgraph.functions.EdgesWithNewGraphsTuple;
import org.gradoop.flink.model.impl.operators.subgraph.functions.ElementIdGraphIdTuple;
import org.gradoop.flink.model.impl.operators.subgraph.functions.IdSourceTargetGraphTuple;
import org.gradoop.flink.model.impl.operators.subgraph.functions.JoinTuplesWithNewGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.JoinWithSourceGraphIdSet;
import org.gradoop.flink.model.impl.operators.subgraph.functions.MergeEdgeGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.MergeTupleGraphs;

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

  @Override
  public GraphCollection execute(GraphCollection superGraph) {
    return vertexFilterFunction != null && edgeFilterFunction != null ?
      subgraph(superGraph) :
      vertexFilterFunction != null ? vertexInducedSubgraph(superGraph) :
        edgeInducedSubgraph(superGraph);
  }

  /**
   * Returns one subgraph for each of the given supergraphs.
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
      .map(new Id<GraphHead>())
      .map(new PairElementWithNewId<GradoopId>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    GraphHeadFactory graphFactory = collection.getConfig()
      .getGraphHeadFactory();

    DataSet<GraphHead> newGraphHeads = graphIdDictionary
      .map(new Project2To1<GradoopId, GradoopId>())
      .map(new InitGraphHead(graphFactory));

    //--------------------------------------------------------------------------
    // compute pairs of a vertex id and the set of new graphs this vertex is
    // contained in
    // filter function is applied first to improve performance
    //--------------------------------------------------------------------------

    DataSet<Tuple2<GradoopId, GradoopIdSet>> vertexIdsWithNewGraphs =
      collection.getVertices()
        .filter(vertexFilterFunction)
        .flatMap(new ElementIdGraphIdTuple<Vertex>())
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
        .equalTo(new Id<Vertex>())
        .with(new AddGraphsToElements<Vertex>());

    //--------------------------------------------------------------------------
    // build tuples4 for each edge, containing
    // edge id, source id, target id, set of new graph ids
    //--------------------------------------------------------------------------

    DataSet<Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdSet>> edgeTuple =
      collection.getEdges()
        .flatMap(new IdSourceTargetGraphTuple<Edge>())
        .join(graphIdDictionary)
        .where(3).equalTo(0)
        .with(new EdgesWithNewGraphsTuple())
        .groupBy(new Value0Of4<GradoopId, GradoopId, GradoopId, GradoopId>())
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
      .equalTo(new Id<Edge>())
      .with(new AddGraphsToElements<Edge>());

    return GraphCollection.fromDataSets(newGraphHeads, newVertices, newEdges,
      collection.getConfig());
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
      .map(new Id<GraphHead>())
      .map(new PairElementWithNewId<GradoopId>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    GraphHeadFactory graphFactory = collection.getConfig()
      .getGraphHeadFactory();

    DataSet<GraphHead> newGraphHeads = graphIdDictionary
      .map(new Project2To1<GradoopId, GradoopId>())
      .map(new InitGraphHead(graphFactory));

    //--------------------------------------------------------------------------
    // compute new edges by applying the filter function,
    // building tuples containing the edge id and the new graphs this edge is
    // contained in and finally adding the new graphs to the edges
    //--------------------------------------------------------------------------

    DataSet<Edge> newEdges = collection
      .getEdges()
      .filter(edgeFilterFunction)
      .flatMap(new ElementIdGraphIdTuple<Edge>())
      .join(graphIdDictionary)
      .where(1)
      .equalTo(0)
      .with(new JoinTuplesWithNewGraphs())
      .groupBy(0)
      .reduceGroup(new MergeTupleGraphs())
      .join(collection.getEdges())
      .where(0)
      .equalTo(new Id<Edge>())
      .with(new AddGraphsToElements<Edge>());

    //--------------------------------------------------------------------------
    // compute the new vertices
    // first, tuples 2 containing the sources and targets of all new edges in
    // the first field and the new graphs this edge is contained in is created
    // this is then joined with the input vertices and the new graphs
    // are added to the vertices
    //--------------------------------------------------------------------------

    DataSet<Vertex> newVertices = newEdges
      .flatMap(new SourceTargetIdGraphsTuple<Edge>())
      .distinct(0)
      .coGroup(collection.getVertices())
      .where(0)
      .equalTo(new Id<Vertex>())
      .with(new AddGraphsToElementsCoGroup<Vertex>());

    return GraphCollection.fromDataSets(newGraphHeads, newVertices, newEdges,
      collection.getConfig());
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
      .map(new Id<GraphHead>())
      .map(new PairElementWithNewId<GradoopId>());

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    GraphHeadFactory graphFactory = collection.getConfig()
      .getGraphHeadFactory();

    DataSet<GraphHead> newGraphHeads = graphIdDictionary
      .map(new Project2To1<GradoopId, GradoopId>())
      .map(new InitGraphHead(graphFactory));

    //--------------------------------------------------------------------------
    // compute pairs of a vertex and the set of new graphs this vertex is
    // contained in
    // filter function is applied first to improve performance
    //--------------------------------------------------------------------------

    DataSet<Vertex> newVertices =
      collection.getVertices()
        .filter(vertexFilterFunction)
        .flatMap(new ElementIdGraphIdTuple<Vertex>())
        .join(graphIdDictionary)
        .where(1).equalTo(0)
        .with(new JoinTuplesWithNewGraphs())
        .groupBy(0)
        .reduceGroup(new MergeTupleGraphs())
        .join(collection.getVertices())
        .where(0)
        .equalTo(new Id<Vertex>())
        .with(new AddGraphsToElements<Vertex>());

    //--------------------------------------------------------------------------
    // compute pairs of an edge and the set of new graphs this edge is
    // contained in
    // filter function is applied first to improve performance
    //--------------------------------------------------------------------------

    DataSet<Edge> newEdges = collection.getEdges()
      .filter(edgeFilterFunction)
      .flatMap(new ElementIdGraphIdTuple<Edge>())
      .join(graphIdDictionary)
      .where(1)
      .equalTo(0)
      .with(new JoinTuplesWithNewGraphs())
      .groupBy(0)
      .reduceGroup(new MergeTupleGraphs())
      .join(collection.getEdges())
      .where(0)
      .equalTo(new Id<Edge>())
      .with(new AddGraphsToElements<Edge>());

    return GraphCollection.fromDataSets(newGraphHeads, newVertices, newEdges,
      collection.getConfig());
  }

  @Override
  public String getName() {
    return ApplySubgraph.class.getName();
  }
}
