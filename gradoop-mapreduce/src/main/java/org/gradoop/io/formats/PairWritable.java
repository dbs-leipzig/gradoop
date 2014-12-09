package org.gradoop.io.formats;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is just for testing purpose of Select/Aggregate.
 */
public class PairWritable implements Writable {

  /**
   * Result of predicate.
   */
  private BooleanWritable predicateResult;

  /**
   * Attribute value for aggregation.
   */
  private DoubleWritable value;

  public PairWritable() {
  }

  public PairWritable(BooleanWritable predicateResult, DoubleWritable value) {
    this.predicateResult = predicateResult;
    this.value = value;
  }

  public BooleanWritable getPredicateResult() {
    return predicateResult;
  }

  public DoubleWritable getValue() {
    return value;
  }

  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    predicateResult.write(dataOutput);
    value.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    if (this.predicateResult == null) {
      this.predicateResult = new BooleanWritable();
    }
    if (this.value == null) {
      this.value = new DoubleWritable();
    }
    this.predicateResult.readFields(dataInput);
    this.value.readFields(dataInput);
  }
}
