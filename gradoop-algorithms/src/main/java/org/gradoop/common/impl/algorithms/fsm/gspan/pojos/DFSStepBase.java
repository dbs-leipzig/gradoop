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

package org.gradoop.model.impl.algorithms.fsm.gspan.pojos;

/**
 * abstract class for representing a DFS traversal step
 */
public abstract class DFSStepBase implements DFSStep {
  /**
   * discovery time of traversal start vertex
   */
  protected final int fromTime;
  /**
   * label of traversal start vertex
   */
  protected final int fromLabel;
  /**
   * label of the traversed edge
   */
  protected final int edgeLabel;
  /**
   * discovery time of traversal end vertex
   */
  protected final int toTime;
  /**
   * label of traversal end vertex
   */
  protected final int toLabel;

  /**
   * Constructor
   *
   * @param fromLabel label of traversal start vertex
   * @param edgeLabel label of the traversed edge
   * @param toLabel label of traversal end vertex
   * @param toTime discovery time of traversal end vertex
   * @param fromTime discovery time of traversal start vertex
   */
  public DFSStepBase(Integer fromLabel, Integer edgeLabel, Integer toLabel,
    int toTime, int fromTime) {
    this.fromLabel = fromLabel;
    this.edgeLabel = edgeLabel;
    this.toLabel = toLabel;
    this.toTime = toTime;
    this.fromTime = fromTime;
  }

  @Override
  public int getFromTime() {
    return fromTime;
  }

  @Override
  public Integer getFromLabel() {
    return fromLabel;
  }

  @Override
  public Integer getEdgeLabel() {
    return edgeLabel;
  }

  @Override
  public int getToTime() {
    return toTime;
  }

  @Override
  public Integer getToLabel() {
    return toLabel;
  }

  @Override
  public Boolean isLoop() {
    return fromTime == toTime;
  }

  @Override
  public Boolean isForward() {
    return getFromTime() < getToTime();
  }

  @Override
  public Boolean isBackward() {
    return !isForward();
  }

  @Override
  public int getMinVertexLabel() {
    return fromLabel < toLabel ? fromLabel : toLabel;
  }
}
