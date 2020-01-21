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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Applies forces to vertices.
 * Uses simulated annealing with exponentially decreasing temperature.
 * Confines new coordinates to the layouting-space
 */
public class FRForceApplicator extends RichJoinFunction<LVertex, Force, LVertex> {

  /**
   * Width of the layouting-space
   */
  protected int layoutWidth;
  /**
   * Height of the layouting space
   */
  protected int layoutHeight;
  /**
   * cache the last computed temperature and re-use if possible to reduce needed computing-power
   */
  protected int lastIteration = -1;
  /**
   * cache the last computed temperature and re-use if possible to reduce needed computing-power
   */
  protected double lastSpeedLimit = 0;
  /**
   * Speed at which the cooling-schedule starts
   */
  private double startSpeed;
  /**
   * Speed at which the cooling-schedule ends
   */
  private double endSpeed;
  /**
   * Base of the exponentially-decreasing function for the speed
   */
  private double base;
  /**
   * Maximum number of iterations
   */
  private int maxIterations;
  /**
   * Calculate temperature as if this number of iterations had already passed
   */
  private int previousIterations = 0;

  /**
   * Create new FRForceApplicator
   *
   * @param width         The width of the layouting-space
   * @param height        The height of the layouting-space
   * @param k             A parameter of the FR-Algorithm.
   * @param maxIterations Number of iterations the FR-Algorithm will have
   */
  public FRForceApplicator(int width, int height, double k, int maxIterations) {
    this.layoutWidth = width;
    this.layoutHeight = height;
    this.startSpeed = Math.sqrt((double) width * (double) width + (double) height * (double) height) / 2.0;
    this.endSpeed = k / 10.0;
    this.maxIterations = maxIterations;
    calculateBase();
  }

  /**
   * Gets startSpeed
   *
   * @return value of startSpeed
   */
  public double getStartSpeed() {
    return startSpeed;
  }

  /**
   * Sets startSpeed (overrides default)
   *
   * @param startSpeed the new value
   */
  public void setStartSpeed(double startSpeed) {
    this.startSpeed = startSpeed;
    calculateBase();
  }

  /**
   * Gets endSpeed
   *
   * @return value of endSpeed
   */
  public double getEndSpeed() {
    return endSpeed;
  }

  /**
   * Sets endSpeed (overrides default)
   *
   * @param endSpeed the new value
   */
  public void setEndSpeed(double endSpeed) {
    this.endSpeed = endSpeed;
    calculateBase();
  }

  /**
   * Gets previousIterations
   *
   * @return value of previousIterations
   */
  public int getPreviousIterations() {
    return previousIterations;
  }

  /**
   * Sets previousIterations
   *
   * @param previousIterations the new value
   */
  public void setPreviousIterations(int previousIterations) {
    this.previousIterations = previousIterations;
    calculateBase();
  }

  /**
   * Gets maxIterations
   *
   * @return value of maxIterations
   */
  public int getMaxIterations() {
    return maxIterations;
  }

  /**
   * Sets maxIterations
   *
   * @param maxIterations the new value
   */
  public void setMaxIterations(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * Calculate the base for the exponential function
   */
  private void calculateBase() {
    this.base = Math.pow(endSpeed / startSpeed, 1.0 / (maxIterations + previousIterations - 1));
  }

  /**
   * Calculate the desired speed for a given iteration
   *
   * @param iteration Iteration to calculate speed for
   * @return Desired speed
   */
  public double speedForIteration(int iteration) {
    iteration += previousIterations;
    // cache last result to avoid costly pow
    if (iteration != lastIteration) {
      lastSpeedLimit = startSpeed * Math.pow(base, iteration);
      lastIteration = iteration;
    }
    return lastSpeedLimit;
  }


  /**
   * Apply force to vertex.
   * Honors speedLimit and vertex-mass. Confines position to layout-area.
   *
   * @param vertex     Vertex to move
   * @param force      Force acting on the vertex
   * @param speedLimit Current speedLimit
   */
  public void apply(LVertex vertex, Force force, double speedLimit) {
    Vector position = vertex.getPosition();
    Vector movement = force.getValue();
    apply(position, movement.div(vertex.getCount()), speedLimit);
    vertex.setForce(movement);
    vertex.setPosition(position);
  }


  /**
   * Raw version of apply() using just vectors.
   *
   * @param position   Position to modify
   * @param movement   Movement to apply
   * @param speedLimit Current speedLimit
   */
  public void apply(Vector position, Vector movement, double speedLimit) {
    position.mAdd(movement.clamped(speedLimit));
    position.mConfined(0, layoutWidth - 1, 0, layoutHeight - 1);
  }

  @Override
  public LVertex join(LVertex first, Force second) throws Exception {
    apply(first, second, speedForIteration(getIterationRuntimeContext().getSuperstepNumber()));
    return first;
  }

}
