package org.gradoop.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom vertex used by {@link org.gradoop.algorithms
 * .LabelPropagationComputation}.
 */
public class LabelPropagationValue implements Writable {
  /**
   * The desired partition the vertex want to migrate to
   */
  private long currentCommunity;
  /**
   * The actual partition
   */
  private long lastCommunity;
  /**
   * Stabilization Rounds
   */
  private long stabilizationRounds;

  /**
   * Constructor
   *
   * @param currentCommunity    currentCommunity
   * @param lastCommunity       lastCommunity
   * @param stabilizationRounds stabilizationRounds
   */
  public LabelPropagationValue(LongWritable currentCommunity,
    LongWritable lastCommunity, long stabilizationRounds) {
    this.currentCommunity = currentCommunity.get();
    this.lastCommunity = lastCommunity.get();
    this.stabilizationRounds = stabilizationRounds;
  }

  /**
   * Default Constructor
   */
  public LabelPropagationValue() {
  }

  /**
   * Method to set the current partition
   *
   * @param lastCommunity current partition
   */
  public void setLastCommunity(LongWritable lastCommunity) {
    this.lastCommunity = lastCommunity.get();
  }

  /**
   * Method to set the lastValue of the vertex
   *
   * @param currentCommunity the desired Partition
   */
  public void setCurrentCommunity(LongWritable currentCommunity) {
    this.currentCommunity = currentCommunity.get();
  }

  /**
   * Method to set the stabilization round counter of the vertex
   *
   * @param stabilizationRounds counter
   */
  public void setStabilizationRounds(long stabilizationRounds) {
    this.stabilizationRounds = stabilizationRounds;
  }

  /**
   * Get method to get the desired partition
   *
   * @return the desired Partition
   */
  public LongWritable getCurrentCommunity() {
    return new LongWritable(this.currentCommunity);
  }

  /**
   * Get the current partition
   *
   * @return the current partition
   */
  public LongWritable getLastCommunity() {
    return new LongWritable(this.lastCommunity);
  }

  /**
   * Method to get the stabilization round counter
   *
   * @return the actual counter
   */
  public long getStabilizationRounds() {
    return stabilizationRounds;
  }

  /**
   * Serializes the content of the vertex object.
   *
   * @param dataOutput data to be serialized
   * @throws IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.currentCommunity);
    dataOutput.writeLong(this.lastCommunity);
  }

  /**
   * Deserializes the content of the vertex object.
   *
   * @param dataInput data to be deserialized
   * @throws IOException
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.currentCommunity = dataInput.readLong();
    this.lastCommunity = dataInput.readLong();
  }
}
