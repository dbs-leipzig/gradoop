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

package org.gradoop.model.api.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.api.functions.TransformationFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.grouping.Grouping;

import java.util.List;

/**
 * Describes all operators that can be applied on a single logical graph in the
 * EPGM.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public interface LogicalGraphOperators
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends GraphBaseOperators<G, V, E> {

  /**
   * Returns a dataset containing a single graph head associated with that
   * logical graph.
   *
   * @return 1-element dataset
   */
  DataSet<G> getGraphHead();

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Returns a graph collection containing all subgraphs of the input graph
   * that match the given graph pattern.
   *
   * @param pattern  GDL graph pattern
   *
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection<G, V, E> match(String pattern);

  /**
   * Returns a graph collection containing all subgraphs of the input graph
   * that match the given graph pattern.
   *
   * This method allows to control if the original vertex and edge data
   * (labels and properties) shall be attached to the resulting subgraphs.
   * Note that this requires additional JOIN operations.
   *
   * @param pattern     GDL graph pattern
   * @param attachData  attach original vertex and edge data to the result
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection<G, V, E> match(String pattern, boolean attachData);

  /**
   * Creates a copy of the logical graph.
   *
   * Note that this method creates new graph head, vertex and edge instances.
   *
   * @return projected logical graph
   */
  LogicalGraph<G, V, E> copy();

  /**
   * Transforms the elements of the logical graph using the given transformation
   * functions. The identity of the elements is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @param vertexTransformationFunction    vertex transformation function
   * @param edgeTransformationFunction      edge transformation function
   * @return transformed logical graph
   */
  LogicalGraph<G, V, E> transform(
    TransformationFunction<G> graphHeadTransformationFunction,
    TransformationFunction<V> vertexTransformationFunction,
    TransformationFunction<E> edgeTransformationFunction);

  /**
   * Transforms the graph head of the logical graph using the given
   * transformation function. The identity of the graph is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @return transformed logical graph
   */
  LogicalGraph<G, V, E> transformGraphHead(
    TransformationFunction<G> graphHeadTransformationFunction);

  /**
   * Transforms the vertices of the logical graph using the given transformation
   * function. The identity of the vertices is preserved.
   *
   * @param vertexTransformationFunction vertex transformation function
   * @return transformed logical graph
   */
  LogicalGraph<G, V, E> transformVertices(
    TransformationFunction<V> vertexTransformationFunction);

  /**
   * Transforms the edges of the logical graph using the given transformation
   * function. The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed logical graph
   */
  LogicalGraph<G, V, E> transformEdges(
    TransformationFunction<E> edgeTransformationFunction);

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the
   * given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph as a new logical graph
   */
  LogicalGraph<G, V, E> vertexInducedSubgraph(
    FilterFunction<V> vertexFilterFunction);

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given
   * filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph as a new logical graph
   */
  LogicalGraph<G, V, E> edgeInducedSubgraph(
    FilterFunction<E> edgeFilterFunction);

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
  LogicalGraph<G, V, E> subgraph(FilterFunction<V> vertexFilterFunction,
    FilterFunction<E> edgeFilterFunction);

  /**
   * Applies the given aggregate function to the logical graph and stores the
   * result of that function at the resulting graph using the given property
   * key.
   *
   * @param propertyKey   used to store result of aggregate func
   * @param aggregateFunc computes an aggregate on the logical graph
   * @return logical graph with additional property storing the aggregate
   */
  LogicalGraph<G, V, E> aggregate(String propertyKey,
    AggregateFunction<G, V, E> aggregateFunc);

  /**
   * Creates a new graph from a randomly chosen subset of nodes and their
   * associated edges.
   *
   * @param sampleSize relative amount of nodes in the result graph
   * @return logical graph with random nodes and their associated edges
   */
  LogicalGraph<G, V, E> sampleRandomNodes(Float sampleSize);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by the given property key. Edges are grouped
   * accordingly. Vertices with missing property key are represented by an
   * additional vertex.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupBy(List<String> vertexGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by the given property key, edges are grouped by the
   * given property key. Vertices and edges with missing property key are
   * represented by an additional vertex.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys   property keys to group edges
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupBy(List<String> vertexGroupingKeys,
    List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by their label. Edges are grouped accordingly.
   *
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexLabel();

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by their label and the given property key. Edges are
   * grouped accordingly. Vertices with missing property key are represented
   * by an additional vertex.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexLabelAndVertexProperties(
    List<String> vertexGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by their label. Edges are grouped by the given
   * property key. Edges with missing property key are represented by an
   * additional edge.
   *
   * @param edgeGroupingKeys property keys to group edges
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexLabelAndEdgeProperties(
    List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label and the given property key. Edges are
   * grouped the given property key.
   * Vertices and edges with missing property keys are represented by an
   * additional vertex / edge.
   *
   * @param vertexGroupingKeys  property keys to group vertices
   * @param edgeGroupingKeys    property keys to group edges
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexLabel(List<String> vertexGroupingKeys,
    List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label, edges are grouped by their label.
   *
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexAndEdgeLabel();

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label and the  given property key. Edges are
   * grouped by their label. Vertices with missing property key are
   * represented by an additional vertex.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexAndEdgeLabelAndVertexProperties(
    List<String> vertexGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label. Edges are grouped by their label
   * and the given property key. Edges with missing property key are represented
   * by an additional edge.
   *
   * @param edgeGroupingKeys property keys to group edges
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexAndEdgeLabelAndEdgeProperties(
    List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices and edges are grouped by their label and the given property keys.
   * Vertices and edges with missing property key are represented by an
   * additional vertex / edge.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys   property keys to group edges
   * @return grouped logical graph
   * @see Grouping
   */
  LogicalGraph<G, V, E> groupByVertexAndEdgeLabel(
    List<String> vertexGroupingKeys, List<String> edgeGroupingKeys);

  /**
   * Checks, if another logical graph contains exactly the same vertices and
   * edges (by id) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  DataSet<Boolean> equalsByElementIds(LogicalGraph<G, V, E> other);

  /**
   * Checks, if another logical graph contains vertices and edges with the same
   * attached data (i.e. label and properties) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  DataSet<Boolean> equalsByElementData(LogicalGraph<G, V, E> other);

  /**
   * Checks, if another logical graph has the same attached data and contains
   * vertices and edges with the same attached data as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  DataSet<Boolean> equalsByData(LogicalGraph<G, V, E> other);

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
  LogicalGraph<G, V, E> combine(LogicalGraph<G, V, E> otherGraph);

  /**
   * Creates a new logical graph containing the overlapping vertex and edge
   * sets of this graph and the given graph. Vertex and edge equality is
   * based on their identifiers.
   *
   * @param otherGraph logical graph to compute overlap with
   * @return logical graph that contains all vertices and edges that exist in
   * both input graphs
   */
  LogicalGraph<G, V, E> overlap(LogicalGraph<G, V, E> otherGraph);

  /**
   * Creates a new logical graph containing only vertices and edges that
   * exist in that graph but not in the other graph. Vertex and edge equality
   * is based on their identifiers.
   *
   * @param otherGraph logical graph to exclude from that graph
   * @return logical that contains only vertices and edges that are not in
   * the other graph
   */
  LogicalGraph<G, V, E> exclude(LogicalGraph<G, V, E> otherGraph);

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
  GraphCollection<G, V, E> splitBy(String propertyKey);

  /**
   * Creates a logical graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  LogicalGraph<G, V, E> callForGraph(
    UnaryGraphToGraphOperator<G, V, E> operator);

  /**
   * Creates a logical graph from that graph and the input graph using the
   * given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  LogicalGraph<G, V, E> callForGraph(
    BinaryGraphToGraphOperator<G, V, E> operator,
    LogicalGraph<G, V, E> otherGraph);

  /**
   * Creates a graph collection from that grpah using the given unary graph
   * operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  GraphCollection<G, V, E> callForCollection(
    UnaryGraphToCollectionOperator<G, V, E> operator);
}
