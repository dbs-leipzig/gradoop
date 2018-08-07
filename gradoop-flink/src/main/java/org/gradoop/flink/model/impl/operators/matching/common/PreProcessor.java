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
package org.gradoop.flink.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.functions.BuildIdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.functions.BuildTripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingEdges;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingPairs;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingTriples;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithSourceEdgeCandidates;

/**
 * Provides methods for filtering vertices, edges, pairs (vertex + edge) and
 * triples based on a given query.
 */
public class PreProcessor {

  /**
   * Filters vertices based on the given GDL query. The resulting dataset only
   * contains vertex ids and their candidates that match at least one vertex in
   * the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @return dataset with matching vertex ids and their candidates
   */
  public static DataSet<IdWithCandidates<GradoopId>> filterVertices(
    LogicalGraph graph, final String query) {
    return graph.getVertices()
      .filter(new MatchingVertices<>(query))
      .map(new BuildIdWithCandidates<>(query));
  }

  /**
   * Filters edges based on the given GDL query. The resulting dataset only
   * contains edges that match at least one edge in the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @return dataset with matching edge triples and their candidates
   */
  public static DataSet<TripleWithCandidates<GradoopId>> filterEdges(
    LogicalGraph graph, final String query) {
    return graph.getEdges()
      .filter(new MatchingEdges<>(query))
      .map(new BuildTripleWithCandidates<>(query));
  }

  /**
   * Filters vertex-edge pairs based on the given GDL query. The resulting
   * dataset only contains vertex-edge pairs that match at least one vertex-edge
   * pair in the query graph.
   *
   * @param g     data graph
   * @param query query graph
   * @return dataset with matching vertex-edge pairs
   */
  public static DataSet<TripleWithSourceEdgeCandidates<GradoopId>> filterPairs(
    LogicalGraph g, final String query) {
    return filterPairs(g, query, filterVertices(g, query));
  }

  /**
   * Filters vertex-edge pairs based on the given GDL query. The resulting
   * dataset only contains vertex-edge pairs that match at least one vertex-edge
   * pair in the query graph and their corresponding candidates
   *
   * @param graph             data graph
   * @param query             query graph
   * @param filteredVertices  used for the edge join

   * @return dataset with matching vertex-edge pairs and their candidates
   */
  public static DataSet<TripleWithSourceEdgeCandidates<GradoopId>> filterPairs(
    LogicalGraph graph, final String query,
    DataSet<IdWithCandidates<GradoopId>> filteredVertices) {
    return filteredVertices
      .join(filterEdges(graph, query))
      .where(0).equalTo(1)
      .with(new MatchingPairs(query));
  }

  /**
   * Filters vertex-edge-vertex pairs based on the given GDL query. The
   * resulting dataset only contains triples that match at least one triple in
   * the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @return dataset with matching triples
   */
  public static DataSet<TripleWithCandidates<GradoopId>> filterTriplets(
    LogicalGraph graph, final String query) {
    return filterTriplets(graph, query, filterVertices(graph, query));
  }

  /**
   * Filters vertex-edge-vertex pairs based on the given GDL query. The
   * resulting dataset only contains triples that match at least one triple in
   * the query graph.
   *
   * @param graph             data graph
   * @param query             query graph
   * @param filteredVertices  used for the edge join
   * @return dataset with matching triples and their candidates
   */
  public static DataSet<TripleWithCandidates<GradoopId>> filterTriplets(
    LogicalGraph graph, final String query,
    DataSet<IdWithCandidates<GradoopId>> filteredVertices) {
    return filterPairs(graph, query, filteredVertices)
      .join(filteredVertices)
      .where(3).equalTo(0)
      .with(new MatchingTriples(query));
  }
}
