/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.gspan.pojos;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * Simple edge triple representation with integer ids and labels.
 */
public class GSpanEdge implements Serializable, Comparable<GSpanEdge> {

  /**
   * source vertex id
   */
  private final int sourceId;
  /**
   * source vertex label
   */
  private final int sourceLabel;
  /**
   * edge id
   */
  private final int edgeId;
  /**
   * edge label
   */
  private final int label;
  /**
   * target vertex id
   */
  private final int targetId;
  /**
   * target vertex label
   */
  private final int targetLabel;

  /**
   * Constructor.
   *
   * @param sourceId source vertex id
   * @param sourceLabel source vertex label
   * @param edgeId edge id
   * @param label edge label
   * @param targetId target vertex id
   * @param targetLabel target vertex label
   */
  public GSpanEdge(
    int sourceId, int sourceLabel,
    int edgeId, int label,
    int targetId, int targetLabel) {

    this.sourceId = sourceId;
    this.sourceLabel = sourceLabel;
    this.edgeId = edgeId;
    this.label = label;
    this.targetId = targetId;
    this.targetLabel = targetLabel;
  }

  /**
   * Convenience method for loop checking.
   * @return true, if loop, false, otherwise
   */
  public boolean isLoop() {
    return sourceId == targetId;
  }

  /**
   * Convenience method to access the minimum vertex label.
   * @return minimum vertex label
   */
  private int getMinVertexLabel() {
    return sourceIsMinimumLabel() ? sourceLabel : targetLabel;
  }

  /**
   * Convenience method to check if source vertex label is minimum vertex label.
   * @return true, if source vertex label is smaller than target vertex
   * label, false otherwise
   */
  public boolean sourceIsMinimumLabel() {
    return sourceLabel <= targetLabel;
  }

  public int getSourceId() {
    return sourceId;
  }

  public int getSourceLabel() {
    return sourceLabel;
  }

  public int getEdgeId() {
    return edgeId;
  }

  public int getLabel() {
    return label;
  }

  public int getTargetId() {
    return targetId;
  }

  public int getTargetLabel() {
    return targetLabel;
  }

  @Override
  public int compareTo(GSpanEdge other) {
    // min vertex label
    int comparison = this.getMinVertexLabel() - other.getMinVertexLabel();

    if (comparison == 0) {
      // same minimum vertex label

      if (this.isLoop() != other.isLoop()) {
        // loop is smaller
        if (this.isLoop()) {
          comparison = -1;
        } else {
          comparison = 1;
        }
      }

      if (comparison == 0) {
        // both loop or both no loop
        comparison = this.getSourceLabel() - other.getSourceLabel();

        if (comparison == 0) {
          // both start from minimum vertex label
          comparison = this.getLabel() - other.getLabel();

          if (comparison == 0) {
            // same edge label
            comparison = this.getTargetLabel() - other.getTargetLabel();
          }
        }
      }
    }

    return comparison;
  }

  @Override
  public boolean equals(Object obj) {
    boolean equals = obj == this;

    if (!equals && obj != null && obj.getClass() == getClass()) {

      GSpanEdge other = (GSpanEdge) obj;

      EqualsBuilder builder = new EqualsBuilder();

      builder.append(this.getSourceId(), other.getSourceId());
      builder.append(this.getEdgeId(), other.getEdgeId());
      builder.append(this.getTargetId(), other.getTargetId());

      equals = builder.isEquals();
    }

    return equals;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    builder.append(getSourceId());
    builder.append(getEdgeId());
    builder.append(getTargetId());

    return builder.hashCode();
  }

  @Override
  public String toString() {
    return "(" + getSourceId() + ":" + getSourceLabel() + ")-[" +
      getEdgeId() + ":" + getLabel() +
      "]->(" + getTargetId() + ":" + getTargetLabel() + ")";
  }
}
