
package org.gradoop.flink.io.impl.tlf.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Represents a vertex used in a graph generation from TLF-files.
 */
public class TLFVertex extends Tuple2<Integer, String> {

  /**
   * Symbol identifying a line to represent a vertex.
   */
  public static final String SYMBOL = "v";

  /**
   * default constructor
   */
  public TLFVertex() {
  }

  /**
   * valued constructor
   * @param id vertex id
   * @param label vertex label
   */
  public TLFVertex(Integer id, String label) {
    super(id, label);
  }

  public Integer getId() {
    return this.f0;
  }

  public void setId(Integer id) {
    this.f0 = id;
  }

  public String getLabel() {
    return this.f1;
  }

  public void setLabel(String label) {
    this.f1 = label;
  }
}
