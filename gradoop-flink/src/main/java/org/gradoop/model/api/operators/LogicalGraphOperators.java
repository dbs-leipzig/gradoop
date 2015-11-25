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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.Predicate;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.Summarization;

/**
 * Describes all operators that can be applied on a single logical graph in the
 * EPGM.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public interface LogicalGraphOperators
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /*
  unary operators take one graph as input and return a single graph or a
  graph collection
   */

  /**
   * Returns a graph collection containing all logical graph that match the
   * given graph pattern.
   *
   * @param graphPattern  graph pattern
   * @param predicateFunc predicate describing the semantic properties of the
   *                      pattern
   * @return logical graphs that match the given graph pattern
   */
  GraphCollection<G, V, E> match(String graphPattern,
    Predicate<LogicalGraph> predicateFunc);

  /**
   * Creates a projected version of the logical graph using the given vertex
   * and edge data projection functions.
   *
   * @param vertexFunction vertex data projection function
   * @param edgeFunction   edge data projection function
   * @return projected logical graph
   */
  LogicalGraph<G, V, E> project(UnaryFunction<V, V> vertexFunction,
    UnaryFunction<E, E> edgeFunction) throws Exception;

  /**
   * Applies the given aggregate function to the logical graph and stores the
   * result of that function at the resulting graph using the given property
   * key.
   *
   * @param propertyKey   used to store result of aggregate func
   * @param aggregateFunc computes an aggregate on the logical graph
   * @param <O>           output type of the aggregate function
   * @return logical graph with additional property storing the aggregate
   * @throws Exception
   */
  <O extends Number> LogicalGraph<G, V, E> aggregate(String propertyKey,
    UnaryFunction<LogicalGraph<G, V, E>, O> aggregateFunc) throws Exception;

  /**
   * Creates a new graph from a randomly chosen subset of nodes and their
   * associated edges.
   *
   * @param sampleSize relative amount of nodes in the result graph
   * @return logical graph with random nodes and their associated edges
   * @throws Exception
   */
  LogicalGraph<G, V, E> sampleRandomNodes(Float sampleSize) throws Exception;

  /* Summarization */

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by the given property key. Edges are grouped
   * accordingly. Vertices with missing property key are represented by an
   * additional vertex.
   *
   * @param vertexGroupingKey property key to group vertices
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarize(String vertexGroupingKey) throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by the given property key, edges are grouped by the
   * given property key. Vertices and edges with missing property key are
   * represented by an additional vertex.
   *
   * @param vertexGroupingKey vertex property key
   * @param edgeGroupingKey   edge property key
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarize(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by their label. Edges are grouped accordingly.
   *
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexLabel() throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by their label and the given property key. Edges are
   * grouped accordingly. Vertices with missing property key are represented
   * by an additional vertex.
   *
   * @param vertexGroupingKey vertex property key
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   *
   * Vertices are grouped by their label. Edges are grouped by the given
   * property key. Edges with missing property key are represented by an
   * additional edge.
   *
   * @param edgeGroupingKey edge property key
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label and the given property key. Edges are
   * grouped the given property key.
   * Vertices and edges with missing property keys are represented by an
   * additional vertex / edge.
   *
   * @param vertexGroupingKey vertex property key
   * @param edgeGroupingKey   edge property key
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexLabel(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label, edges are grouped
   * by their label.
   *
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexAndEdgeLabel() throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label and the  given property key. Edges are
   * grouped by their label. Vertices with missing property key are
   * represented by an additional vertex.
   *
   * @param vertexGroupingKey vertex property key
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexAndEdgeLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices are grouped by their label. Edges are grouped by their label
   * and the given property key.
   * Edges with missing property key are represented by an
   * additional edge.
   *
   * @param edgeGroupingKey edge property key
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexAndEdgeLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception;

  /**
   * Creates a condensed version of the logical graph by grouping vertices
   * and edges.
   * <p/>
   * Vertices and edges are grouped by their label and the given property keys.
   * Vertices and edges with missing property key are represented by an
   * additional vertex / edge.
   *
   * @param vertexGroupingKey vertex property key
   * @param edgeGroupingKey   edge property key
   * @return summarized logical graph
   * @throws Exception
   * @see Summarization
   */
  LogicalGraph<G, V, E> summarizeOnVertexAndEdgeLabel(
    String vertexGroupingKey, String edgeGroupingKey) throws Exception;

  /*
  binary operators take two graphs as input and return a single graph
   */

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

  /*
  auxiliary operators
   */

  /**
   * Creates a logical graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   * @throws Exception
   */
  LogicalGraph<G, V, E> callForGraph(
    UnaryGraphToGraphOperator<V, E, G> operator) throws Exception;

  /**
   * Creates a logical graph from that graph and the input graph using the
   * given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  LogicalGraph<G, V, E> callForGraph(
    BinaryGraphToGraphOperator<V, E, G> operator,
    LogicalGraph<G, V, E> otherGraph);

  /**
   * Creates a graph collection from that grpah using the given unary graph
   * operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  GraphCollection<G, V, E> callForCollection(
    UnaryGraphToCollectionOperator<V, E, G> operator) throws Exception;

  /**
   * Writes the logical graph into three separate JSON files. {@code
   * vertexFile} contains the vertex data, {@code edgeFile} contains the edge
   * data and {@code graphFile} contains the graph data of that logical graph.
   * <p/>
   * Operation uses Flink to write the internal datasets, thus writing to
   * local file system ({@code file://}) as well as HDFS ({@code hdfs://}) is
   * supported.
   *
   * @param vertexFile vertex data output file
   * @param edgeFile   edge data output file
   * @param graphFile  graph data output file
   * @throws Exception
   */
  void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception;

  /**
   * Checks, if another logical graph contains exactly the same vertices and
   * edges (by id) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  DataSet<Boolean> equalsByElementIds(LogicalGraph<G, V, E> other);

  /**
   * Convenience method for collected result of
   * {@link #equalsByElementIds(LogicalGraph)}
   *
   * @param other other graph
   * @return true, if equal by element ids
   * @throws Exception
   */
  Boolean equalsByElementIdsCollected(LogicalGraph<G, V, E> other) throws
    Exception;

  /**
   * Checks, if another logical graph contains vertices and edges with the same
   * attached data (i.e. label and properties) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by element data
   */
  DataSet<Boolean> equalsByElementData(LogicalGraph<G, V, E> other);

  /**
   * Convenience method for collected result of
   * {@link #equalsByElementData(LogicalGraph)}
   *
   * @param other other graph
   * @return true, if equal by element data
   * @throws Exception
   */
  Boolean equalsByElementDataCollected(LogicalGraph<G, V, E> other) throws
    Exception;
}
