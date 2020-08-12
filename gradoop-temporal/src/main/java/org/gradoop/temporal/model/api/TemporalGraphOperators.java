/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.api;

import org.gradoop.flink.model.api.epgm.BaseGraphOperators;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.functions.predicates.AsOf;
import org.gradoop.temporal.model.impl.functions.predicates.Between;
import org.gradoop.temporal.model.impl.functions.predicates.ContainedIn;
import org.gradoop.temporal.model.impl.functions.predicates.CreatedIn;
import org.gradoop.temporal.model.impl.functions.predicates.DeletedIn;
import org.gradoop.temporal.model.impl.functions.predicates.FromTo;
import org.gradoop.temporal.model.impl.functions.predicates.ValidDuring;
import org.gradoop.temporal.model.impl.operators.diff.Diff;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.dummy.DummyTemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CypherTemporalPatternMatching;
import org.gradoop.temporal.model.impl.operators.snapshot.Snapshot;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Defines the operators that are available on a {@link TemporalGraph}.
 */
public interface TemporalGraphOperators extends BaseGraphOperators<TemporalGraphHead, TemporalVertex,
  TemporalEdge, TemporalGraph, TemporalGraphCollection> {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Extracts a snapshot of this temporal graph using a given temporal predicate. The predicate is applied
   * on the valid time dimension by default. To use the transaction time dimension, use
   * {@link TemporalGraphOperators#snapshot(TemporalPredicate, TimeDimension)} instead.
   * This operator will calculate the subgraph induced by the predicate.
   *
   * @param predicate the temporal predicate to apply on the valid times
   * @return the snapshot as a temporal graph
   */
  default TemporalGraph snapshot(TemporalPredicate predicate) {
    return snapshot(predicate, TimeDimension.VALID_TIME);
  }

  /**
   * Extracts a snapshot of this temporal graph using a given temporal predicate. The predicate is applied
   * on the given time dimension.
   * This operator will calculate the subgraph induced by the predicate.
   *
   * @param predicate the temporal predicate to apply
   * @param dimension the dimension that is used
   * @return the snapshot as a temporal graph
   */
  default TemporalGraph snapshot(TemporalPredicate predicate, TimeDimension dimension) {
    return callForGraph(new Snapshot(predicate, dimension));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate {@code AS OF timestamp}
   * where {@code timestamp} is a timestamp in milliseconds.
   *
   * @param timestamp the timestamp in milliseconds to query
   * @return the snapshot as a temporal graph
   * @see Snapshot
   * @see AsOf
   */
  default TemporalGraph asOf(long timestamp) {
    return snapshot(new AsOf(timestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code FROM fromTimestamp TO toTimestamp} where both values are timestamps in milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp   the to timestamp in milliseconds to query
   * @return the snapshot as a temporal graph
   * @see Snapshot
   * @see FromTo
   */
  default TemporalGraph fromTo(long fromTimestamp, long toTimestamp) {
    return snapshot(new FromTo(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code BETWEEN fromTimestamp AND toTimestamp} where both values are timestamps in milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp   the to timestamp in milliseconds to query
   * @return the snapshot as a temporal graph
   * @see Snapshot
   * @see Between
   */
  default TemporalGraph between(long fromTimestamp, long toTimestamp) {
    return snapshot(new Between(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code CONTAINED IN (fromTimestamp, toTimestamp)} where both values are timestamps in milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp   the to timestamp in milliseconds to query
   * @return the snapshot as a temporal graph
   * @see Snapshot
   * @see ContainedIn
   */
  default TemporalGraph containedIn(long fromTimestamp, long toTimestamp) {
    return snapshot(new ContainedIn(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code VALID DURING (fromTimestamp, toTimestamp)} where both values are timestamps in milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp   the to timestamp in milliseconds to query
   * @return the snapshot as a temporal graph
   * @see Snapshot
   * @see ValidDuring
   */
  default TemporalGraph validDuring(long fromTimestamp, long toTimestamp) {
    return snapshot(new ValidDuring(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code CREATED IN (fromTimestamp, toTimestamp)} where both values are timestamps in milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp   the to timestamp in milliseconds to query
   * @return the snapshot as a temporal graph
   * @see Snapshot
   * @see CreatedIn
   */
  default TemporalGraph createdIn(long fromTimestamp, long toTimestamp) {
    return snapshot(new CreatedIn(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code DELETED IN (fromTimestamp, toTimestamp)} where both values are timestamps in milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp   the to timestamp in milliseconds to query
   * @return the snapshot as a temporal graph
   * @see Snapshot
   * @see DeletedIn
   */
  default TemporalGraph deletedIn(long fromTimestamp, long toTimestamp) {
    return snapshot(new DeletedIn(fromTimestamp, toTimestamp));
  }

  /**
   * Compares two snapshots of this graph. Given two temporal predicates, this operation
   * will check if a graph element (vertex or edge) was added, removed or persists in the second
   * snapshot compared to the first snapshot. The predicates are applied on the valid times. To use
   * transaction time dimension, use
   * {@link TemporalGraphOperators#diff(TemporalPredicate, TemporalPredicate, TimeDimension)}.
   * <p>
   * This operation returns the union of both snapshots with the following changes:
   * A property with key {@value Diff#PROPERTY_KEY}
   * will be set on each graph element. Its value will be set to
   * <ul>
   *   <li>{@code 0}, if the element is present in both snapshots.</li>
   *   <li>{@code 1}, if the element is present in the second, but not the first snapshot
   *   (i.e. it was added since the first snapshot).</li>
   *   <li>{@code -1}, if the element is present in the first, but not the second snapshot
   *   (i.e. it was removed since the first snapshot).</li>
   * </ul>
   * Graph elements present in neither snapshot will be discarded.
   * The resulting graph will not be verified, i.e. dangling edges could occur. Use the
   * {@code verify()} operator to validate the graph. The graph head is preserved.
   *
   * @param firstSnapshot  The predicate used to determine the first snapshot.
   * @param secondSnapshot The predicate used to determine the second snapshot.
   * @return A temporal graph containing the union of vertex and edge sets of both snapshots,
   * defined by the given two predicate functions. A property with key
   * {@link Diff#PROPERTY_KEY} is set on each graph element with a numerical value (-1, 0, 1) defined above.
   */
  default TemporalGraph diff(TemporalPredicate firstSnapshot, TemporalPredicate secondSnapshot) {
    return diff(firstSnapshot, secondSnapshot, TimeDimension.VALID_TIME);
  }

  /**
   * Compares two snapshots of this graph. Given two temporal predicates, this operation will check if a
   * graph element (vertex or edge) was added, removed or persists in the second snapshot compared to the
   * first snapshot. The predicates are applied on the specified time dimension.
   * <p>
   * This operation returns the union of both snapshots with the following changes:
   * A property with key {@value Diff#PROPERTY_KEY} will be set on each graph element.
   * Its value will be set to
   * <ul>
   *   <li>{@code 0}, if the element is present in both snapshots.</li>
   *   <li>{@code 1}, if the element is present in the second, but not the first snapshot
   *   (i.e. it was added since the first snapshot).</li>
   *   <li>{@code -1}, if the element is present in the first, but not the second snapshot
   *   (i.e. it was removed since the first snapshot).</li>
   * </ul>
   * Graph elements present in neither snapshot will be discarded.
   * The resulting graph will not be verified, i.e. dangling edges could occur. Use the
   * {@code verify()} operator to validate the graph. The graph head is preserved.
   *
   * @param firstSnapshot  The predicate used to determine the first snapshot.
   * @param secondSnapshot The predicate used to determine the second snapshot.
   * @param dimension      The time dimension that will be considered by the predicates.
   * @return A temporal graph containing the union of vertex and edge sets of both snapshots, defined by the
   * given two predicate functions. A property with key {@link Diff#PROPERTY_KEY} is set on each graph
   * element with a numerical value (-1, 0, 1) defined above.
   */
  default TemporalGraph diff(TemporalPredicate firstSnapshot, TemporalPredicate secondSnapshot,
                             TimeDimension dimension) {
    return callForGraph(new Diff(firstSnapshot, secondSnapshot, dimension));
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad runtime
   * performance. Use {@link #query(String, GraphStatistics)} to provide statistics for the query planner.
   *
   * @param query Cypher query
   * @return graph collection containing matching subgraphs
   */
  default TemporalGraphCollection query(String query) {
    return query(query, new DummyTemporalGraphStatistics());
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad runtime
   * performance. Use {@link #query(String, String, GraphStatistics)} to provide statistics for the query planner.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation of new graph
   * elements based on variable bindings of the match pattern. Consider the following example:
   * <br>
   * {@code graph.query(
   * "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   * "(a)-[:CO_AUTHOR]->(b)")}
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper.
   * The construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query string
   * @param constructionPattern Construction pattern
   * @return graph collection containing the output of the construct pattern
   */
  default TemporalGraphCollection query(String query, String constructionPattern) {
    return query(query, constructionPattern, new DummyTemporalGraphStatistics());
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   *
   * @param query           Cypher query
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  default TemporalGraphCollection query(String query, TemporalGraphStatistics graphStatistics) {
    return query(query, true, MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM,
      graphStatistics);
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation of new graph
   * elements based on variable bindings of the match pattern. Consider the following example:
   * <br>
   * {@code graph.query(
   * "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   * "(a)-[:CO_AUTHOR]->(b)")}
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper.
   * The construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query
   * @param constructionPattern Construction pattern
   * @param graphStatistics     statistics about the data graph
   * @return graph collection containing the output of the construct pattern
   */
  default TemporalGraphCollection query(String query, String constructionPattern,
                                        TemporalGraphStatistics graphStatistics) {
    return query(query, constructionPattern, true, MatchStrategy.ISOMORPHISM,
      MatchStrategy.ISOMORPHISM, graphStatistics);
  }

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
  default TemporalGraphCollection query(String query, boolean attachData, MatchStrategy vertexStrategy,
                                        MatchStrategy edgeStrategy, TemporalGraphStatistics graphStatistics) {
    return query(query, null, attachData, vertexStrategy, edgeStrategy, graphStatistics);
  }

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query               Cypher query
   * @param constructionPattern Construction pattern
   * @param attachData          attach original vertex and edge data to the result
   * @param vertexStrategy      morphism setting for vertex mapping
   * @param edgeStrategy        morphism setting for edge mapping
   * @param stats               statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  default TemporalGraphCollection query(String query, String constructionPattern, boolean attachData,
                                        MatchStrategy vertexStrategy,
                                        MatchStrategy edgeStrategy, TemporalGraphStatistics stats) {
    return callForCollection(new CypherTemporalPatternMatching(query, constructionPattern, attachData,
      vertexStrategy, edgeStrategy, stats, new CNFPostProcessing()));
  }

  //----------------------------------------------------------------------------
  // Utilities
  //----------------------------------------------------------------------------

  /**
   * Converts the {@link TemporalGraph} to a {@link LogicalGraph} instance by discarding all
   * temporal information from the graph elements. All Ids (graphs, vertices, edges) are kept
   * during the transformation.
   *
   * @return the logical graph instance
   */
  LogicalGraph toLogicalGraph();

}
