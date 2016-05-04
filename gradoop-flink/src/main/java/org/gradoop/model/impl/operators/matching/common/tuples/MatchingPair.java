package org.gradoop.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

/**
 * Represents a vertex and a single incident edge.
 *
 * f0: source vertex
 * f1: outgoing edge
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class MatchingPair<V extends EPGMVertex, E extends EPGMEdge>
  extends Tuple2<V, E> {

  public V getVertex() {
    return f0;
  }

  public void setVertex(V v) {
    f0 = v;
  }

  public E getEdge() {
    return f1;
  }

  public void setEdge(E e) {
    f1 = e;
  }
}
