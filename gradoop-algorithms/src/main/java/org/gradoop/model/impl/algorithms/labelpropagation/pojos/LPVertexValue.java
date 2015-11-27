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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.labelpropagation.pojos;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Custom vertex used by {@link org.gradoop.model.impl.algorithms
 * .labelpropagation.LabelPropagationAlgorithm}.
 */
public class LPVertexValue {
  /**
   * Vertex ID
   */
  private GradoopId id;
  /**
   * Current community ID
   */
  private GradoopId currentCommunity;
  /**
   * Last community ID
   */
  private GradoopId lastCommunity;
  /**
   * Stabilization counter
   */
  private int stabilizationCounter;
  /**
   * Change Maxima Todo: use Broadcast var!
   */
  private int changeMax;

  /**
   * Constructor
   *
   * @param id    actual vertex id
   * @param value actual vertex value
   */
  public LPVertexValue(GradoopId id, GradoopId value) {
    this.id = id;
    this.currentCommunity = value;
    this.lastCommunity = GradoopId.MAX_VALUE;
    this.stabilizationCounter = 0;
    this.changeMax = 19;
  }

  /**
   * Method to get the Vertex id
   *
   * @return actual vertex id
   */
  public GradoopId getId() {
    return id;
  }

  /**
   * Method to set the Vertex id
   *
   * @param id vertex id
   */
  public void setId(GradoopId id) {
    this.id = id;
  }

  /**
   * Method to get the current Community
   *
   * @return current community id
   */
  public GradoopId getCurrentCommunity() {
    return currentCommunity;
  }

  /**
   * Method to set the current community id
   *
   * @param currentCommunity id of the current community
   */
  public void setCurrentCommunity(GradoopId currentCommunity) {
    this.currentCommunity = currentCommunity;
  }

  /**
   * Method to get the last community id
   *
   * @return last community id
   */
  public GradoopId getLastCommunity() {
    return lastCommunity;
  }

  /**
   * Method to set the last community id
   *
   * @param lastCommunity last community id
   */
  public void setLastCommunity(GradoopId lastCommunity) {
    this.lastCommunity = lastCommunity;
  }

  /**
   * Method to get the Stabilization counter
   *
   * @return the actual counter
   */
  public int getStabilizationCounter() {
    return stabilizationCounter;
  }

  /**
   * Method to set the Stabilization Counter
   *
   * @param stabilizationCounter actual counter
   */
  public void setStabilizationCounter(int stabilizationCounter) {
    this.stabilizationCounter = stabilizationCounter;
  }

  /**
   * Method to get the max changes parameter
   *
   * @return the actual parameter
   */
  public int getChangeMax() {
    return changeMax;
  }

  /**
   * Method to set the max changes parameter
   *
   * @param max number of the parameter
   */
  public void setChangeMax(int max) {
    this.changeMax = max;
  }
}
