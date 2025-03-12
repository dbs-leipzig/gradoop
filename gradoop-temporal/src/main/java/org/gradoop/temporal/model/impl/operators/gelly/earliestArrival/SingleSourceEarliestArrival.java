/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.gelly.earliestArrival;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.functions.gelly.TemporalGellyAlgorithm;
import org.gradoop.temporal.model.impl.functions.gelly.functions.TemporalEdgeToGellyEdgeWithTimeValue;
import org.gradoop.temporal.model.impl.functions.gelly.functions.TemporalVertexToGellyVertexWithTimeValue;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;


/**
 * A gradoop operator for Single Source Earliest Arrival (SSEA).
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 */
public class SingleSourceEarliestArrival<
        G extends TemporalGraphHead,
        V extends TemporalVertex,
        E extends TemporalEdge,
        LG extends TemporalGraph,
        GC extends TemporalGraphCollection>
        extends TemporalGellyAlgorithm<G, V, E, LG, GC, Tuple2<Long, Long>, Tuple2<Long, Long>> {

  /**
   * ID of the source vertex
   */
  private final GradoopId srcVertexId;

  /**
   * Number of iterations.
   */
  private final int maxIterations;

  /**
   * Property key to store SSEA time in.
  */
  private final String vertexProperty;

  /**
   * use valid time as interval or only start timestamp
   */
  private final boolean interval;

  /**
   * starttime at source vertex
   */
  private final Long starttime;

  /**
   * overlap of interval allowed
   */
  private final boolean overlap;

  /**
   *Constructor for single source earliest arrival
   *
   *
   * @param srcVertexId       Id of the source vertex.
   * @param maxIterations     maximum number of iterations
   * @param vertexProperty    vertex property to store SSEA time
   * @param interval          edge valid time is interval (true) or timestamp (false)
   * @param starttime         start time on source vertex
   * @param overlap           valid time can overlap (true) or not (false)
   */

  public SingleSourceEarliestArrival(GradoopId srcVertexId, int maxIterations, String vertexProperty,
                                     boolean interval, Long starttime, boolean overlap) {
    super(
            new TemporalEdgeToGellyEdgeWithTimeValue<>(),
            new TemporalVertexToGellyVertexWithTimeValue<>());
    this.srcVertexId = srcVertexId;
    this.maxIterations = maxIterations;
    this.vertexProperty = vertexProperty;
    this.interval = interval;
    this.starttime = starttime;
    this.overlap = overlap;
  }

  @Override
  public TemporalGraph executeInGelly(Graph gellyGraph) throws Exception {
    DataSet<TemporalVertex> newVerticies =
            new SingleSourceEarliestArrivalAlgorithm<GradoopId, Double>(
                    maxIterations, srcVertexId, interval, starttime, overlap)
            .run(gellyGraph)
            .join(currentGraph.getVertices())
            .where(0)
            .equalTo(new Id<>())
            .with(new SingleSourceEarliestArrivalAttribute(vertexProperty));

    return currentGraph.getFactory().fromDataSets(
            currentGraph.getGraphHead(), newVerticies, currentGraph.getEdges()
    );
  }
}
