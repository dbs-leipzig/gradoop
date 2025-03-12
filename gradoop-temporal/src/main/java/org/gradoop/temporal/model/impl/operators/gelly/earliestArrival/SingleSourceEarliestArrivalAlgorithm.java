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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;

/**
 *
 * This is an implementation of the Single-Source-Earliest Arrival algorithm,
 * using a scatter-gather iteration.
 *
 *@param <K>    Type of ID
 *@param <VV>   Gelly Edge Data Type
 */


public class SingleSourceEarliestArrivalAlgorithm<K, VV> implements GraphAlgorithm<K, VV, Tuple2<Long, Long>, DataSet<VV>> {


  /**
   * Number of iterations.
   */
  private final int maxIterations;

  /**
   * ID of the source vertex
   */
  private final K srcVertexId;

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
   * Creates an instance of the SingleSourceEarliestArrival algorithm.
   * @param maxIterations Number of iterations.
   * @param srcVertex ID of the source vertex
   * @param interval use valid time as interval or only start timestamp
   * @param starttime starttime at source vertex
   * @param overlap overlap of interval allowed
   */
  public SingleSourceEarliestArrivalAlgorithm(
          int maxIterations, K srcVertex, boolean interval, Long starttime, boolean overlap) {
    this.maxIterations = maxIterations;
    this.srcVertexId = srcVertex;
    this.interval = interval;
    this.starttime = starttime;
    this.overlap = overlap;
  }

  @Override
  public DataSet<VV> run(Graph<K, VV, Tuple2<Long, Long>> input) throws Exception {
    return input.mapVertices(new InitVerticesMapper<>(srcVertexId, starttime)).runScatterGatherIteration(
            new MinDistanceMessenger<>(interval, overlap), new VertexDistanceUpdater(), maxIterations)
            .getVertices();
  }

  /**
   * init Vertices with start values
   * @param <K> Type of ID
   * @param <VV> Gelly Edge Data Type
   */

  private static final class InitVerticesMapper<K, VV> implements MapFunction<Vertex<K, VV>, Long> {

    /**
     * Vertex ID
     */
    private K srcVertexId;

    /**
     * Startime of start vertex
     */
    private Long starttime;

    /**
     * Constructor of Init Mapper
     * @param srcId Typ of ID
     * @param starttime Startime of start vertex
     */
    InitVerticesMapper(K srcId, Long starttime) {
      this.srcVertexId = srcId;
      this.starttime = starttime;
    }

    /**
     * Mapping of start values
     * @param value The input value.
     * @return inicialised vertex
     */
    public Long map(Vertex<K, VV> value) {
      if (value.f0.equals(srcVertexId)) {
        return starttime;
      } else {
        return Long.MAX_VALUE;
      }
    }
  }


  /**
   * Create Message and send to other Vertices
   * @param <K> Type of ID
   */
  public static final class MinDistanceMessenger<K> extends
          ScatterFunction<K, Long, Long, Tuple2<Long, Long>> {

    /**
     * use valid time as interval or only start timestamp
     */
    private final boolean interval;

    /**
     * overlap of interval allowed
     */
    private final boolean overlap;

    /**
     * Constructor for messenger
     * @param interval use valid time as interval or only start timestamp
     * @param overlap overlap of interval allowed
     */
    public MinDistanceMessenger(boolean interval, boolean overlap) {
      this.interval = interval;
      this.overlap = overlap;
    }

    /**
     * send message to next vertex
     * @param vertex currently to be processed vertex
     *
     */
    public void sendMessages(Vertex<K, Long> vertex) {

      for (Edge<K, Tuple2<Long, Long>> edge : getEdges()) {
        if (!vertex.getValue().equals(Long.MAX_VALUE)) {
          if (interval) {
            if (overlap) {
              if (vertex.getValue() <= edge.getValue().f1) {
                Long value = edge.getValue().f0;
                if (value < vertex.getValue()) {
                  value = vertex.getValue();
                }
                sendMessageTo(edge.getTarget(), value);
              }
            } else {
              if (vertex.getValue() <= edge.getValue().f0) {
                sendMessageTo(edge.getTarget(), edge.getValue().f1);
              }
            }
          } else {
            if (vertex.getValue() <= edge.getValue().f0) {
              sendMessageTo(edge.getTarget(), edge.getValue().f0);
            }
          }
        }
      }
    }
  }

  /**
   * receive messages and update value on vertex
   * @param <K> Type of ID
   */
  public static final class VertexDistanceUpdater<K> extends GatherFunction<K, Long, Long> {

    /**
     * receive messages and update value on vertex
     * @param vertex Gelly vertex
     * @param inMessages The incoming messages to this vertex.
     *
     */
    public void updateVertex(Vertex<K, Long> vertex, MessageIterator<Long> inMessages) {
      long minTime = Long.MAX_VALUE;

      for (long msg : inMessages) {
        if (msg < minTime) {
          minTime = msg;
        }
      }

      if (vertex.getValue() > minTime) {
        setNewVertexValue(minTime);
      }
    }
  }
}
