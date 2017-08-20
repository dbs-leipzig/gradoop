/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.neighborhood.Neighborhood;

import java.util.List;

/**
 * Defines the operators that are available on a {@link LogicalGraph}.
 */
public interface LogicalGraphOperators extends GraphBaseOperators {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * Note, that this method used no statistics about the data graph which may result in bad
   * runtime performance. Use {@link LogicalGraphOperators#cypher(String, GraphStatistics)} to
   * provide statistics for the query planner.
   *
   * @param query Cypher query
   * @return graph collection containing matching subgraphs
   */
  GraphCollection cypher(String query);

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * @param query Cypher query
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection cypher(String query, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query Cypher query
   * @param attachData  attach original vertex and edge data to the result
   * @param vertexStrategy morphism setting for vertex mapping
   * @param edgeStrategy morphism setting for edge mapping
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection cypher(String query, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics);

  /**
   * Evaluates the given GDL query using the Traverser query engine.
   *
   * @param pattern  GDL graph pattern
   *
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection match(String pattern);

  /**
   * Evaluates the given GDL query using the Traverser query engine.
   *
   * This method allows to control if the original vertex and edge data
   * (labels and properties) shall be attached to the resulting subgraphs.
   * Note that this requires additional JOIN operations.
   *
   * @param pattern     GDL graph pattern
   * @param attachData  attach original vertex and edge data to the result
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection match(String pattern, boolean attachData);

  /**
   * Evaluates the given GDL query using the Traverser query engine.
   *
   * This method allows to control the match strategy. This influences mostly
   * if vertices and edges can be matched to multiple vertices/edges in the
   * query.
   *
   * @param pattern           GDL graph pattern
   * @param attachData        attach original vertex and edge data to the result
   * @param matchStrategy     strategy for vertex and edge mappings
   * @param iterationStrategy strategy for internal iteration
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection match(String pattern, boolean attachData,
    MatchStrategy matchStrategy, TraverserStrategy iterationStrategy);

  /**
   * Creates a copy of the logical graph.
   *
   * Note that this method creates new graph head, vertex and edge instances.
   *
   * @return projected logical graph
   */
  LogicalGraph copy();

  /**
   * Transforms the elements of the logical graph using the given transformation
   * functions. The identity of the elements is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @param vertexTransformationFunction    vertex transformation function
   * @param edgeTransformationFunction      edge transformation function
   * @return transformed logical graph
   */
  LogicalGraph transform(
    TransformationFunction<GraphHead> graphHeadTransformationFunction,
    TransformationFunction<Vertex> vertexTransformationFunction,
    TransformationFunction<Edge> edgeTransformationFunction);

  /**
   * Transforms the graph head of the logical graph using the given
   * transformation function. The identity of the graph is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @return transformed logical graph
   */
  LogicalGraph transformGraphHead(
    TransformationFunction<GraphHead> graphHeadTransformationFunction);

  /**
   * Transforms the vertices of the logical graph using the given transformation
   * function. The identity of the vertices is preserved.
   *
   * @param vertexTransformationFunction vertex transformation function
   * @return transformed logical graph
   */
  LogicalGraph transformVertices(TransformationFunction<Vertex> vertexTransformationFunction);

  /**
   * Transforms the edges of the logical graph using the given transformation
   * function. The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed logical graph
   */
  LogicalGraph transformEdges(TransformationFunction<Edge> edgeTransformationFunction);

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the
   * given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph as a new logical graph
   */
  LogicalGraph vertexInducedSubgraph(FilterFunction<Vertex> vertexFilterFunction);

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given
   * filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph as a new logical graph
   */
  LogicalGraph edgeInducedSubgraph(FilterFunction<Edge> edgeFilterFunction);

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function
   * respectively.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction  vertex filter function
   * @param edgeFilterFunction    edge filter function
   * @return  logical graph which fulfils the given predicates and is a subgraph
   *          of that graph
   */
  LogicalGraph subgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction);

  /**
   * Applies the given aggregate function to the logical graph and stores the
   * result of that function at the resulting graph using the given property
   * key.
   *
   * @param aggregateFunc computes an aggregate on the logical graph
   * @return logical graph with additional property storing the aggregate
   */
  LogicalGraph aggregate(AggregateFunction aggregateFunc);

  /**
   * Creates a new graph from a randomly chosen subset of nodes and their
   * associated edges.
   *
   * @param sampleSize relative amount of nodes in the result graph
   * @return logical graph with random nodes and their associated edges
   */
  LogicalGraph sampleRandomNodes(float sampleSize);

  /**
   * Creates a condensed version of the logical graph by grouping vertices based on the specified
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices.
   *
   * Note: To group vertices by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   *
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(List<String> vertexGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys.
   *
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys property keys to group edges
   *
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys. Furthermore, one can
   * specify sets of vertex and edge aggregate functions which are applied on vertices/edges
   * represented by the same super vertex/edge.
   *
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys property keys to group edges
   * @param edgeAggregateFunctions aggregate functions to apply on super edges
   * @param groupingStrategy execution strategy for vertex grouping
   *
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(
    List<String> vertexGroupingKeys, List<PropertyValueAggregator> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<PropertyValueAggregator> edgeAggregateFunctions,
    GroupingStrategy groupingStrategy);

  /**
   * Sets the aggregation result of the given function as property for each vertex. All edges where
   * the vertex is relevant get joined first and then grouped. The relevant edges are specified
   * using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   *
   * @return logical graph where vertices store aggregated information about connected edges
   */
  LogicalGraph reduceOnEdges(
    EdgeAggregateFunction function, Neighborhood.EdgeDirection edgeDirection);

  /**
   * Sets the aggregation result of the given function as property for each vertex. All vertices
   * of relevant edges get joined first and then grouped by the vertex. The relevant edges are
   * specified using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   *
   * @return logical graph where vertices store aggregated information about connected vertices
   */
  LogicalGraph reduceOnNeighbors(
    VertexAggregateFunction function, Neighborhood.EdgeDirection edgeDirection);

  /**
   * Checks, if another logical graph contains exactly the same vertices and
   * edges (by id) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  DataSet<Boolean> equalsByElementIds(LogicalGraph other);

  /**
   * Checks, if another logical graph contains vertices and edges with the same
   * attached data (i.e. label and properties) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  DataSet<Boolean> equalsByElementData(LogicalGraph other);

  /**
   * Checks, if another logical graph has the same attached data and contains
   * vertices and edges with the same attached data as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  DataSet<Boolean> equalsByData(LogicalGraph other);

  /**
   * Applies a given drill-up (transformation) function on a single vertex property of the input
   * graph. The property is identified by the specified property key. The previous version of the
   * property value is stored at the vertex. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param propertyKey property key
   * @param function drill up function
   * @return graph with drilled up properties
   */
  LogicalGraph drillUpVertex(String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-up (transformation) function on a single vertex property of the input
   * graph. The vertices are selected by their label and the property is identified by the
   * specified property key. The previous version of the property value is stored at the vertex.
   * The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param vertexLabel vertex label
   * @param propertyKey property key
   * @param function drill up function
   * @return graph with drilled up properties
   */
  LogicalGraph drillUpVertex(String vertexLabel, String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-up (transformation) function on a single vertex property of the input
   * graph. The vertices are selected by their label and the property is identified by the
   * specified property key. The new version of the property value is stored at the vertex
   * under the new property key. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param vertexLabel vertex label
   * @param propertyKey property key
   * @param function drill up function
   * @param newPropertyKey new property key
   * @return graph with drilled up properties
   */
  LogicalGraph drillUpVertex(
    String vertexLabel, String propertyKey, DrillFunction function, String newPropertyKey);

  /**
   * Applies a given drill-up (transformation) function on a single edge property of the input
   * graph. The property is identified by the specified property key. The previous version of the
   * property value is stored at the edge. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param propertyKey property key
   * @param function drill up function
   * @return graph with drilled up properties
   */
  LogicalGraph drillUpEdge(String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-up (transformation) function on a single edge property of the input
   * graph. The edges are selected by their label and the property is identified by the
   * specified property key. The previous version of the property value is stored at the edge.
   * The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param edgeLabel edge label
   * @param propertyKey property key
   * @param function drill up function
   * @return graph with drilled up properties
   */
  LogicalGraph drillUpEdge(String edgeLabel, String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-up (transformation) function on a single edge property of the input
   * graph. The edges are selected by their label and the property is identified by the
   * specified property key. The new version of the property value is stored at the edge
   * under the new property key. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param edgeLabel edge label
   * @param propertyKey property key
   * @param function drill up function
   * @param newPropertyKey new property key
   * @return graph with drilled up properties
   */
  LogicalGraph drillUpEdge(
    String edgeLabel, String propertyKey, DrillFunction function, String newPropertyKey);

  /**
   * Applies a drill-down operation on a single vertex property of the input graph. The property
   * is identified by the specified property key. The detailed version of the property value has
   * to be stored at the vertex as a result from a previous drill-up operation.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param propertyKey property key
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownVertex(String propertyKey);

  /**
   * Applies a given drill-down operation on a single vertex property of the input graph. The
   * vertices are selected by their label and the property is identified by the specified
   * property key. The detailed version of the property value has to be stored at the vertex as a
   * result from a previous drill-up operation. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param vertexLabel vertex label
   * @param propertyKey property key
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownVertex(String vertexLabel, String propertyKey);

  /**
   * Applies a given drill-down (transformation) function on a single vertex property of the input
   * graph. The property is identified by the specified property key. The previous version of the
   * property value is overwritten. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param propertyKey property key
   * @param function roll up function
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownVertex(String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-down (transformation) function on a single vertex property of the input
   * graph. The vertices are selected by their label and the property is identified by the
   * specified property key. The previous version of the property value is overwritten. The
   * structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param vertexLabel vertex label
   * @param propertyKey property key
   * @param function roll up function
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownVertex(String vertexLabel, String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-down (transformation) function on a single vertex property of the input
   * graph. The vertices are selected by their label and the property is identified by the
   * specified property key. The new version of the property value is stored at the vertex
   * under the new property key while the old one is removed. The structure of the graph remains
   * unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param vertexLabel vertex label
   * @param propertyKey property key
   * @param function roll up function
   * @param newPropertyKey new property key
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownVertex(
    String vertexLabel, String propertyKey, DrillFunction function, String newPropertyKey);

  /**
   * Applies a drill-down operation on a single edge property of the input graph. The property
   * is identified by the specified property key. The detailed version of the property value has
   * to be stored at the edge as a result from a previous drill-up operation.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param propertyKey property key
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownEdge(String propertyKey);

  /**
   * Applies a given drill-down operation on a single edge property of the input graph. The
   * edges are selected by their label and the property is identified by the specified
   * property key. The detailed version of the property value has to be stored at the edge as a
   * result from a previous drill-up operation. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param edgeLabel edge label
   * @param propertyKey property key
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownEdge(String edgeLabel, String propertyKey);

  /**
   * Applies a given drill-down (transformation) function on a single edge property of the input
   * graph. The property is identified by the specified property key. The previous version of the
   * property value is overwritten. The structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param propertyKey property key
   * @param function roll up function
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownEdge(String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-down (transformation) function on a single edge property of the input
   * graph. The edges are selected by their label and the property is identified by the
   * specified property key. The previous version of the property value is overwritten. The
   * structure of the graph remains unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param edgeLabel edge label
   * @param propertyKey property key
   * @param function roll up function
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownEdge(String edgeLabel, String propertyKey, DrillFunction function);

  /**
   * Applies a given drill-down (transformation) function on a single edge property of the input
   * graph. The edges are selected by their label and the property is identified by the
   * specified property key. The new version of the property value is stored at the edge
   * under the new property key while the old one is removed. The structure of the graph remains
   * unchanged.
   *
   * This is a convenience function for
   * {@link LogicalGraph#transformVertices(TransformationFunction)} and can be used as a
   * pre-processor for {@link LogicalGraph#groupBy(List)} which performs the structural aggregation.
   *
   * @param edgeLabel edge label
   * @param propertyKey property key
   * @param function roll up function
   * @param newPropertyKey new property key
   * @return graph with rolled up properties
   */
  LogicalGraph drillDownEdge(
    String edgeLabel, String propertyKey, DrillFunction function, String newPropertyKey);

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a new logical graph by combining the vertex and edge sets of
   * this graph and the given graph. Vertex and edge equality is based on their
   * identifiers.
   *
   * @param otherGraph logical graph to combine this graph with
   * @return logical graph containing all vertices and edges of the
   * input graphs
   */
  LogicalGraph combine(LogicalGraph otherGraph);

  /**
   * Creates a new logical graph containing the overlapping vertex and edge
   * sets of this graph and the given graph. Vertex and edge equality is
   * based on their identifiers.
   *
   * @param otherGraph logical graph to compute overlap with
   * @return logical graph that contains all vertices and edges that exist in
   * both input graphs
   */
  LogicalGraph overlap(LogicalGraph otherGraph);

  /**
   * Creates a new logical graph containing only vertices and edges that
   * exist in that graph but not in the other graph. Vertex and edge equality
   * is based on their identifiers.
   *
   * @param otherGraph logical graph to exclude from that graph
   * @return logical that contains only vertices and edges that are not in
   * the other graph
   */
  LogicalGraph exclude(LogicalGraph otherGraph);

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * Splits the graph into multiple logical graphs using the property value
   * which is assigned to the given property key. Vertices and edges that do
   * not have this property will be removed from the resulting collection.
   *
   * @param propertyKey split property key
   * @return graph collection
   */
  GraphCollection splitBy(String propertyKey);

  /**
   * Creates a logical graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  LogicalGraph callForGraph(UnaryGraphToGraphOperator operator);

  /**
   * Creates a logical graph from that graph and the input graph using the
   * given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  LogicalGraph callForGraph(BinaryGraphToGraphOperator operator, LogicalGraph otherGraph);

  /**
   * Creates a graph collection from that grpah using the given unary graph
   * operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  GraphCollection callForCollection(UnaryGraphToCollectionOperator operator);
}
