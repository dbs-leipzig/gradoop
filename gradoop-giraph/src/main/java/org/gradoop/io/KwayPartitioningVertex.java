package org.gradoop.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom vertex used by {@link org.gradoop.algorithms
 * .KwayPartitioningComputation}.
 */
public class KwayPartitioningVertex implements Writable {

  /**
   * The vertex value which the vertex had in the previous superstep
   */
  private int lastValue;

  /**
   * The actual vertex value
   */
  private int currentValue;

  /**
   * Method to set the currentValue of the vertex
   *
   * @param currentVertexValue the currentValue
   */
  public void setCurrentVertexValue(IntWritable currentVertexValue) {
    this.currentValue = currentVertexValue.get();
  }

  /**
   * Method to set the lastValue of the vertex
   *
   * @param lastVertexValue the lastValue
   */
  public void setLastVertexValue(IntWritable lastVertexValue) {
    this.lastValue = lastVertexValue.get();
  }

  /**
   * Get the lastValue    // vertex lastvalue
   *
   * @return lastValue
   */
  public IntWritable getLastVertexValue() {
    return new IntWritable(this.lastValue);
  }

  /**
   * Get the currentValue
   *
   * @return currentValue
   */
  public IntWritable getCurrentVertexValue() {
    return new IntWritable(this.currentValue);
  }

  /**
   * Serializes the content of the vertex object.
   *
   * @param dataOutput data to be serialized
   * @throws IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.lastValue);
    dataOutput.writeInt(this.currentValue);
  }

  /**
   * Deserializes the content of the vertex object.
   *
   * @param dataInput data to be deserialized
   * @throws IOException
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.lastValue = dataInput.readInt();
    this.currentValue = dataInput.readInt();
  }
}
