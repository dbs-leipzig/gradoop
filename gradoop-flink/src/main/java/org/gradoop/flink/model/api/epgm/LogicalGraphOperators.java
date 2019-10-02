/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.operators.BaseGraphOperatorSupport;
import org.gradoop.flink.model.api.operators.GraphsToGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.CAPFQueryResult;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;

import java.io.IOException;
import java.util.List;

/**
 * Defines the operators that are available on a {@link LogicalGraph}.
 */
public interface LogicalGraphOperators extends BaseGraphOperatorSupport<LogicalGraph, GraphCollection> {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Evaluates the given cypher query using CAPF (Cypher for Apache Flink). CAPF implements the
   * default cypher morphism strategies, which is vertex homomorphism and edge isomorphism.
   * The result is a {@link CAPFQueryResult}, containing a flink table that can be converted to a
   * {@link GraphCollection}, if it contains vertices or edges.
   *
   * @param query    the query string
   * @param metaData metaData object
   * @return the result, containing a flink table and possibly a GraphCollection
   * @throws Exception on failure
   */
  CAPFQueryResult cypher(String query, MetaData metaData) throws Exception;

  /**
   * Evaluates the given cypher query using CAPF (Cypher for Apache Flink). CAPF implements the
   * default cypher morphism strategies, which is vertex homomorphism and edge isomorphism.
   * The result is a CAPFQueryResult, containing a flink table that can be converted to a GraphCollection,
   * if it contains vertices or edges.
   * <p>
   * In this overloaded function, the property maps are constructed automatically. This is
   * a lot slower and actually requires the job to be split in two parts to collect the property maps.
   *
   * @param query the query string
   * @return the result, containing a flink table and possibly a GraphCollection
   * @throws Exception on failure
   */
  CAPFQueryResult cypher(String query) throws Exception;

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad
   * runtime performance. Use {@link LogicalGraphOperators#query(String, GraphStatistics)} to
   * provide statistics for the query planner.
   *
   * @param query Cypher query
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query);

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad
   * runtime performance. Use {@link LogicalGraphOperators#query(String, GraphStatistics)} to
   * provide statistics for the query planner.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation
   * of new graph elements based on variable bindings of the match pattern. Consider the following example:
   * <p>
   * {@code graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")}
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper.
   * The construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query string
   * @param constructionPattern Construction pattern
   * @return graph collection containing the output of the construct pattern
   */
  GraphCollection query(String query, String constructionPattern);

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * @param query           Cypher query
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation
   * of new graph elements based on variable bindings of the match pattern. Consider the following example:
   * <p>
   * {@code graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")}
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper.
   * The construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query
   * @param constructionPattern Construction pattern
   * @param graphStatistics     statistics about the data graph
   * @return graph collection containing the output of the construct pattern
   */
  GraphCollection query(String query, String constructionPattern, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query           Cypher query
   * @param attachData      attach original vertex and edge data to the result
   * @param vertexStrategy  morphism setting for vertex mapping
   * @param edgeStrategy    morphism setting for edge mapping
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query, boolean attachData, MatchStrategy vertexStrategy,
    MatchStrategy edgeStrategy, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query               Cypher query
   * @param constructionPattern Construction pattern
   * @param attachData          attach original vertex and edge data to the result
   * @param vertexStrategy      morphism setting for vertex mapping
   * @param edgeStrategy        morphism setting for edge mapping
   * @param graphStatistics     statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query, String constructionPattern, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
    GraphStatistics graphStatistics);

  /**
   * Creates a new graph from a randomly chosen subset of nodes and their associated edges.
   *
   * @param algorithm used sampling algorithm
   * @return logical graph with random nodes and their associated edges
   */
  LogicalGraph sample(SamplingAlgorithm algorithm);

  /**
   * Checks, if another logical graph contains exactly the same vertices and edges (by id) as this graph.
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
   * Generates all combinations of the supplied vertex grouping keys according to the definition of
   * the rollUp operation in SQL and uses them together with all edge grouping keys for separate
   * grouping operations. For example, specifying the vertex grouping keys A, B and C leads to
   * three differently grouped graphs {A,B,C},{A,B},{A} within the resulting graph collection.
   *
   * @param vertexGroupingKeys       grouping keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         grouping keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @return graph collection containing all resulting graphs
   */
  GraphCollection groupVerticesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions);

  /**
   * Generates all combinations of the supplied edge grouping keys according to the definition of
   * the rollUp operation in SQL and uses them together with all vertex grouping keys for separate
   * grouping operations. For example, specifying the edge grouping keys A, B and C leads to
   * three differently grouped graphs {A,B,C},{A,B},{A} within the resulting graph collection.
   *
   * @param vertexGroupingKeys       grouping keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         grouping keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @return graph collection containing all resulting graphs
   */
  GraphCollection groupEdgesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions);

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
   * Creates a logical graph from that graph and other graphs using the given operator.
   *
   * @param operator    multi graph to graph operator
   * @param otherGraphs other graphs
   * @return result of given operator
   */
  LogicalGraph callForGraph(GraphsToGraphOperator operator, LogicalGraph... otherGraphs);

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which indicates if the graph is empty.
   *
   * A graph is considered empty, if it contains no vertices.
   *
   * @return  1-element dataset containing {@code true}, if the collection is
   *          empty or {@code false} if not
   */
  DataSet<Boolean> isEmpty();

  /**
     * Writes the graph to given data sink.
     *
     * @param dataSink The data sink to which the graph should be written.
     * @throws IOException if the graph can't be written to the sink
     */
  void writeTo(DataSink dataSink) throws IOException;

  /**
     * Writes the graph to given data sink with an optional overwrite option.
     *
     * @param dataSink The data sink to which the graph should be written.
     * @param overWrite determines whether existing files are overwritten
     * @throws IOException if the graph can't be written to the sink
     */
  void writeTo(DataSink dataSink, boolean overWrite) throws IOException;
}
