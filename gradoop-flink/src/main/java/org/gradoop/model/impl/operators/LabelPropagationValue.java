package org.gradoop.model.impl.operators;

/**
 * Value for LabelPropagationAlgorithm
 */
public class LabelPropagationValue {
  /**
   * Vertex ID
   */
  private long id;
  /**
   * Current community ID
   */
  private long currentCommunity;
  /**
   * Last community ID
   */
  private long lastCommunity;
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
  public LabelPropagationValue(long id, long value) {
    this.id = id;
    this.currentCommunity = value;
    this.lastCommunity = Long.MAX_VALUE;
    this.stabilizationCounter = 0;
    this.changeMax = 19;
  }

  /**
   * Method to get the Vertex id
   *
   * @return actual vertex id
   */
  public long getId() {
    return id;
  }

  /**
   * Method to set the Vertex id
   *
   * @param id vertex id
   */
  public void setId(long id) {
    this.id = id;
  }

  /**
   * Method to get the current Community
   *
   * @return current community id
   */
  public long getCurrentCommunity() {
    return currentCommunity;
  }

  /**
   * Method to set the current community id
   *
   * @param currentCommunity id of the current community
   */
  public void setCurrentCommunity(long currentCommunity) {
    this.currentCommunity = currentCommunity;
  }

  /**
   * Method to get the last community id
   *
   * @return last community id
   */
  public long getLastCommunity() {
    return lastCommunity;
  }

  /**
   * Method to set the last community id
   *
   * @param lastCommunity last community id
   */
  public void setLastCommunity(long lastCommunity) {
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
