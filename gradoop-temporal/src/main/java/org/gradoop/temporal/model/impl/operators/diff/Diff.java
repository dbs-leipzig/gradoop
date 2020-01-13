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
package org.gradoop.temporal.model.impl.operators.diff;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.diff.functions.DiffPerElement;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.Objects;

/**
 * Calculates the difference between two snapshots of a graph by comparing the temporal attributes
 * of the graph elements.
 * <p>
 * The snapshots are extracted through two given temporal predicates. The result is a temporal graph
 * containing the union of both graph element sets. Each element gets a new property named
 * {@link Diff#PROPERTY_KEY} whose value will be a number indicating that an element is either
 * equal in both snapshots (0) or added (1) or removed (-1) in the second snapshot.
 * Elements not present in either snapshots will be discarded.
 * <p>
 * The resulting graph will not be verified, i.e. dangling edges could occur. Use the
 * {@link TemporalGraph#verify()} operator to validate the graph. The graph head is preserved.
 */
public class Diff implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {
  /**
   * The property key used to store the diff result on each element.
   */
  public static final String PROPERTY_KEY = "_diff";

  /**
   * The property value used to indicate that an element was added in the second snapshot.
   */
  public static final PropertyValue VALUE_ADDED = PropertyValue.create(1);

  /**
   * The property value used to indicate that an element is equal in both snapshots.
   */
  public static final PropertyValue VALUE_EQUAL = PropertyValue.create(0);

  /**
   * The property value used to indicate that an element was removed in the second snapshot.
   */
  public static final PropertyValue VALUE_REMOVED = PropertyValue.create(-1);

  /**
   * The predicate used to determine the first snapshot.
   */
  private final TemporalPredicate firstPredicate;

  /**
   * The predicate used to determine the second snapshot.
   */
  private final TemporalPredicate secondPredicate;

  /**
   * Specifies the time dimension that will be considered by the operator.
   */
  private TimeDimension dimension;

  /**
   * Create an instance of the TPGM diff operator, setting the two predicates used to determine the snapshots.
   * By default, valid times will be used by the predicate. To use transaction times, use
   * {@link Diff#Diff(TemporalPredicate, TemporalPredicate, TimeDimension)} instead.
   *
   * @param firstPredicate  The predicate used for the first snapshot.
   * @param secondPredicate The predicate used for the second snapshot.
   */
  public Diff(TemporalPredicate firstPredicate, TemporalPredicate secondPredicate) {
    this(firstPredicate, secondPredicate, TimeDimension.VALID_TIME);
  }
  /**
   * Create an instance of the TPGM diff operator, setting the two predicates used to determine the snapshots.
   *
   * @param firstPredicate The predicate used for the first snapshot.
   * @param secondPredicate The predicate used for the second snapshot.
   * @param dimension The time dimension that will be used.
   */
  public Diff(TemporalPredicate firstPredicate, TemporalPredicate secondPredicate, TimeDimension dimension) {
    this.firstPredicate = Objects.requireNonNull(firstPredicate, "No first predicate given.");
    this.secondPredicate = Objects.requireNonNull(secondPredicate, "No second predicate given.");
    this.dimension = Objects.requireNonNull(dimension, "No time dimension given.");
  }

  @Override
  public TemporalGraph execute(TemporalGraph graph) {
    DataSet<TemporalVertex> transformedVertices = graph.getVertices()
      .flatMap(new DiffPerElement<>(firstPredicate, secondPredicate, dimension));
    DataSet<TemporalEdge> transformedEdges = graph.getEdges()
      .flatMap(new DiffPerElement<>(firstPredicate, secondPredicate, dimension));
    return graph.getFactory().fromDataSets(graph.getGraphHead(), transformedVertices, transformedEdges);
  }
}
