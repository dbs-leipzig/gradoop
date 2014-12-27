package org.gradoop.io.formats;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Used to transfer a predicate result and a value from the mapper to the
 * reducer.
 */
public class GenericPairWritable implements Writable {

  /**
   * Result of predicate.
   */
  private BooleanWritable predicateResult;

  /**
   * Attribute value for aggregation.
   */
  private ValueWritable value;

  /**
   * Default constructor needed for deserialization.
   */
  public GenericPairWritable() {
  }

  /**
   * Creates a GenericPairWritable based on the given parameters.
   *
   * @param predicateResult predicate result
   * @param value           value
   */
  public GenericPairWritable(BooleanWritable predicateResult,
    ValueWritable value) {
    this.predicateResult = predicateResult;
    this.value = value;
  }

  public BooleanWritable getPredicateResult() {
    return predicateResult;
  }

  public ValueWritable getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    predicateResult.write(dataOutput);
    value.write(dataOutput);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    if (this.predicateResult == null) {
      this.predicateResult = new BooleanWritable();
    }
    if (this.value == null) {
      this.value = new ValueWritable();
    }
    this.predicateResult.readFields(dataInput);
    this.value.readFields(dataInput);
  }
}
