package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;

/**
 * Creates a (usually 1-element) Boolean dataset based on two input graphs.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 * @param <T> value type
 */
public interface BinaryCollectionToValueOperator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge, T>
  extends Operator {
  /**
   * Executes the operator.
   *
   * @param firstCollection  first input collection
   * @param secondCollection second input collection
   * @return operator result
   */
  DataSet<T> execute(
    GraphCollection<V, E, G> firstCollection,
    GraphCollection<V, E, G> secondCollection
  );
}
