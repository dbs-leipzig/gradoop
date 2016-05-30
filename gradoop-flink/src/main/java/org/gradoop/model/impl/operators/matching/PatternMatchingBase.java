package org.gradoop.model.impl.operators.matching;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.PairElementWithPropertyValue;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.debug.PrintIdWithCandidates;
import org.gradoop.model.impl.operators.matching.common.debug.PrintTripleWithCandidates;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Base class for pattern matching implementations.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public abstract class PatternMatchingBase
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {
  /**
   * GDL based query string
   */
  protected final String query;
  /**
   * If true, the original vertex and edge data gets attached to the resulting
   * vertices and edges.
   */
  protected final boolean attachData;
  /**
   * Vertex mapping used for debug
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> vertexMapping;
  /**
   * Edge mapping used for debug
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> edgeMapping;

  /**
   * Constructor
   *
   * @param query       GDL query graph
   * @param attachData  true, if original data shall be attached to the result
   */
  public PatternMatchingBase(String query, boolean attachData) {
    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query            = query;
    this.attachData       = attachData;
  }

  protected void initDebugMappings(LogicalGraph<G, V, E> graph) {
    vertexMapping = graph.getVertices()
      .map(new PairElementWithPropertyValue<V>("id"));
    edgeMapping = graph.getEdges()
      .map(new PairElementWithPropertyValue<E>("id"));
  }

  protected DataSet<TripleWithCandidates> printTriplesWithCandidates(
    DataSet<TripleWithCandidates> edges) {
    return edges
      .map(new PrintTripleWithCandidates())
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
  }

  protected DataSet<IdWithCandidates> printIdWithCandidates(
    DataSet<IdWithCandidates> vertices) {
    return vertices
      .map(new PrintIdWithCandidates())
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
  }
}
