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
package org.gradoop.temporal.model.impl.operators.snapshot;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.snapshot.functions.ByTemporalPredicate;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.Objects;

/**
 * Extracts a snapshot of a temporal graph using a given temporal predicate.
 * This will calculate the subgraph of a temporal graph induced by the predicate.
 * <p>
 * The graph head is preserved and will not be affected by the operator.
 * <p>
 * The resulting graph will not be verified, i.e. dangling edges could occur. Use the
 * {@link TemporalGraph#verify()} operator to validate the graph.
 */
public class Snapshot implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {

  /**
   * Used temporal predicate.
   */
  private final TemporalPredicate temporalPredicate;

  /**
   * Specifies the time dimension that will be considered by the operator.
   */
  private TimeDimension dimension;

  /**
   * Creates an instance of the snapshot operator with the given temporal predicate.
   * The predicate is applied on the valid time dimension by default.
   *
   * @param predicate The temporal predicate.
   */
  public Snapshot(TemporalPredicate predicate) {
    this(predicate, TimeDimension.VALID_TIME);
  }

  /**
   * Creates an instance of the snapshot operator with the given temporal predicate.
   *
   * @param predicate The temporal predicate.
   * @param dimension The time dimension that will be considered by the operator.
   */
  public Snapshot(TemporalPredicate predicate, TimeDimension dimension) {
    this.temporalPredicate = Objects.requireNonNull(predicate, "No predicate given.");
    this.dimension = Objects.requireNonNull(dimension, "No time dimension given.");
  }

  @Override
  public TemporalGraph execute(TemporalGraph superGraph) {
    DataSet<TemporalVertex> vertices = superGraph.getVertices()
      // Filter vertices
      .filter(new ByTemporalPredicate<>(temporalPredicate, dimension));
    DataSet<TemporalEdge> edges = superGraph.getEdges()
      // Filter edges
      .filter(new ByTemporalPredicate<>(temporalPredicate, dimension));

    return superGraph.getFactory().fromDataSets(superGraph.getGraphHead(), vertices, edges);
  }
}
