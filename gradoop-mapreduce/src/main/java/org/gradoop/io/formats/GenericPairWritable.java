package org.gradoop.io.formats;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Used to transfer a predicate result, a vertex id and a value from the
 * mapper to the reducer.
 */
public class GenericPairWritable implements Writable {

  /**
   * Result of predicate.
   */
  private BooleanWritable predicateResult;

  /**
   * Vertex ID
   */
  private LongWritable vertexID;

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
   * @param vertexID        vertex identifier
   * @param value           value
   */
  public GenericPairWritable(BooleanWritable predicateResult,
    LongWritable vertexID, ValueWritable value) {
    this.predicateResult = predicateResult;
    this.vertexID = vertexID;
    this.value = value;
  }

  public BooleanWritable getPredicateResult() {
    return predicateResult;
  }

  public LongWritable getVertexID() {
    return vertexID;
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
    vertexID.write(dataOutput);
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
    if (this.vertexID == null) {
      this.vertexID = new LongWritable();
    }
    if (this.value == null) {
      this.value = new ValueWritable();
    }
    this.predicateResult.readFields(dataInput);
    this.vertexID.readFields(dataInput);
    this.value.readFields(dataInput);
  }
}
