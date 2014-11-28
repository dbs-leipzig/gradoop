package org.gradoop.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by gomezk on 28.11.14.
 */
public class KwayPartitioningVertex implements Writable {

  private int lastValue;
  private int currentValue;


  public KwayPartitioningVertex(){}

  public void setCurrentVertexValue(IntWritable currentVertexValue) {
    this.currentValue = currentVertexValue.get();
  }

  public void setLastVertexValue(IntWritable lastVertexValue) {
    this.lastValue = lastVertexValue.get();
  }

  public IntWritable getLastVertexValue() {
    return new IntWritable(this.lastValue);
  }

  public IntWritable getCurrentVertexValue() {
    return new IntWritable(this.currentValue);
  }

  @Override
  public String toString(){
    return Integer.toString(currentValue);
  }


  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    // vertex lastvalue
    dataOutput.writeInt(this.lastValue);
    // vertex currentvalue
    dataOutput.writeInt(this.currentValue);
  }


  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    // vertex lastvalue
    this.lastValue = dataInput.readInt();
    // vertex currentvalue
    this.currentValue = dataInput.readInt();
  }
}
