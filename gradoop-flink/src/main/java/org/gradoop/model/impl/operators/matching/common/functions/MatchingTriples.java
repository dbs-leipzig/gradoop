package org.gradoop.model.impl.operators.matching.common.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.matching.common.matching.EntityMatcher;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingPair;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.List;

/**
 * Filters a {@link MatchingTriple} based on its occurrence in the given GDL
 * query pattern.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class MatchingTriples<V extends EPGMVertex, E extends EPGMEdge>
  extends RichFlatJoinFunction<MatchingPair<V, E>, V, MatchingTriple> {

  private final String query;

  private transient QueryHandler queryHandler;

  private final MatchingTriple reuseTriple;

  public MatchingTriples(final String query) {
    this.query = query;
    this.reuseTriple = new MatchingTriple();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public void join(MatchingPair<V, E> matchingPair, V targetVertex,
    Collector<MatchingTriple> collector) throws Exception {

    Collection<Vertex> queryVertices = queryHandler
      .getVerticesByLabel(targetVertex.getLabel());

    List<Long> candidates = Lists.newArrayList();

    for (Vertex queryTargetVertex : queryVertices) {
      Collection<Edge> edges = queryHandler
        .getEdgesByTargetVertexId(queryTargetVertex.getId());
      if (edges != null) {
        for (Edge queryEdge : edges) {
          Vertex querySourceVertex = queryHandler
            .getVertexById(queryEdge.getSourceVertexId());
          boolean match =
            EntityMatcher.match(matchingPair.getVertex(), querySourceVertex) &&
              EntityMatcher.match(matchingPair.getEdge(), queryEdge) &&
              EntityMatcher.match(targetVertex, queryTargetVertex);

//          System.out.println(String.format(
//            "(%d:%s)-[%2d:%s]->(%d:%s) == (%d:%s)-[%2d:%s]->(%d:%s) => %s",
//            matchingPair.getVertex().getPropertyValue("id").getInt(),
//            matchingPair.getVertex().getLabel(),
//            matchingPair.getEdge().getPropertyValue("id").getInt(),
//            matchingPair.getEdge().getLabel(),
//            targetVertex.getPropertyValue("id").getInt(),
//            targetVertex.getLabel(),
//            querySourceVertex.getId(), querySourceVertex.getLabel(),
//            queryEdge.getId(), queryEdge.getLabel(),
//            queryTargetVertex.getId(), queryTargetVertex.getLabel(), match));

          if (match) {
            candidates.add(queryEdge.getId());
          }
        }
      }
    }

    if (candidates.size() > 0) {
      reuseTriple.setEdgeId(matchingPair.getEdge().getId());
      reuseTriple.setSourceVertexId(matchingPair.getVertex().getId());
      reuseTriple.setTargetVertexId(targetVertex.getId());
      reuseTriple.setQueryCandidates(candidates);
      collector.collect(reuseTriple);
    }
  }
}
