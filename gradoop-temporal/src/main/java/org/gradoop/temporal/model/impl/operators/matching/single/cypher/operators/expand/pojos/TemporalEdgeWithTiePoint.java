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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.io.Serializable;

/**
 * Represents an Edge with an extracted tie point
 * <p>
 * {@code f0 -> edge join key}<br>
 * {@code f1 -> edge id}<br>
 * {@code f2 -> edge expand key}
 * The temporal information of the edge and its end point are retained, as they can be relevant
 * for expanding an edge
 */
public class TemporalEdgeWithTiePoint implements Serializable {

  /**
   * The Gradoop ID of the edge's source vertex
   */
  private GradoopId source;
  /**
   * The Gradoop ID of the edge itself
   */
  private GradoopId edge;
  /**
   * The Gradoop ID of the edge's target vertex
   */
  private GradoopId target;
  /**
   * The time data of the edge's source vertex in the form [tx_from, tx_to, valid_from, valid_to]
   */
  private Long[] sourceTimeData;
  /**
   * The time data of the edge in the form [tx_from, tx_to, valid_from, valid_to]
   */
  private Long[] edgeTimeData;
  /**
   * The time data of the edge's target vertex in the form [tx_from, tx_to, valid_from, valid_to]
   */
  private Long[] targetTimeData;

  /**
   * Creates an empty Object
   */
  public TemporalEdgeWithTiePoint() {
  }

  /**
   * Creates a new Edge with extracted tie point and time data from a given temporal edge embedding
   * @param edgeEmb edge embedding to create the edge with tie point from
   */
  public TemporalEdgeWithTiePoint(EmbeddingTPGM edgeEmb) {
    source = edgeEmb.getId(0);
    edge = edgeEmb.getId(1);
    target = edgeEmb.getId(2);
    sourceTimeData = edgeEmb.getTimes(0);
    edgeTimeData = edgeEmb.getTimes(1);
    targetTimeData = edgeEmb.getTimes(2);
  }

  /**
   * Get source id
   *
   * @return source id
   */
  public GradoopId getSource() {
    return source;
  }

  /**
   * Set source id
   *
   * @param id source id
   */
  public void setSource(GradoopId id) {
    source = id;
  }

  /**
   * Get edge id
   *
   * @return edge id
   */
  public GradoopId getEdge() {
    return edge;
  }

  /**
   * Set edge id
   *
   * @param id edge id
   */
  public void setEdge(GradoopId id) {
    edge = id;
  }

  /**
   * Get target id
   *
   * @return target id
   */
  public GradoopId getTarget() {
    return target;
  }

  /**
   * Set target id
   *
   * @param id target id
   */
  public void setTarget(GradoopId id) {
    target = id;
  }

  /**
   * get source vertex's time data
   *
   * @return source vertex's time data
   */
  public Long[] getSourceTimeData() {
    return sourceTimeData.clone();
  }

  /**
   * set source vertex's time data
   *
   * @param timeData time data for the source vertex (array of length 4 in the form [tx_from, tx_to,
   *                 valid_from, valid_to]
   */
  public void setSourceTimeData(Long[] timeData) {
    sourceTimeData = timeData.clone();
  }

  /**
   * get the edge's time data
   *
   * @return edge's time data
   */
  public Long[] getEdgeTimeData() {
    return edgeTimeData.clone();
  }

  /**
   * set edge's time data
   *
   * @param timeData time data for the edge (array of length 4 in the form [tx_from, tx_to,
   *                 valid_from, valid_to]
   */
  public void setEdgeTimeData(Long[] timeData) {
    edgeTimeData = timeData.clone();
  }

  /**
   * get the target vertex's time data
   *
   * @return target vertex's time data
   */
  public Long[] getTargetTimeData() {
    return targetTimeData.clone();
  }

  /**
   * set target vertex's time data
   *
   * @param timeData time data for the target vertex (array of length 4 in the form [tx_from, tx_to,
   *                 valid_from, valid_to]
   */
  public void setTargetTimeData(Long[] timeData) {
    targetTimeData = timeData.clone();
  }

  /**
   * Returns the maximum tx_from value of edge and target
   *
   * @return maximum tx_from value of edge and target
   */
  public Long getMaxTxFrom() {
    return Math.max(edgeTimeData[0], targetTimeData[0]);
  }

  /**
   * Returns the minimum tx_to value of edge and target
   *
   * @return minimum tx_to value of edge and target
   */
  public Long getMinTxTo() {
    return Math.min(edgeTimeData[1], targetTimeData[1]);
  }

  /**
   * Returns the maximum valid_from value of edge and target
   *
   * @return maximum valid_from value of edge and target
   */
  public Long getMaxValidFrom() {
    return Math.max(edgeTimeData[2], targetTimeData[2]);
  }

  /**
   * Returns the minimum valid_to value of edge and target
   *
   * @return minimum valid_to value of edge and target
   */
  public Long getMinValidTo() {
    return Math.min(edgeTimeData[3], targetTimeData[3]);
  }
}
