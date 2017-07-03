
package org.gradoop.flink.io.impl.tlf.tuples;

import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Represents a graph head used in a graph generation from TLF-files.
 */
public class TLFGraphHead extends Tuple1<Long> {

  /**
   * default constructor
   */
  public TLFGraphHead() {
  }

  /**
   * valued constructor
   * @param id graph head id
   */
  public TLFGraphHead(Long id) {
    super(id);
  }

  public Long getId() {
    return this.f0;
  }

  public void setId(long id) {
    this.f0 = id;
  }

}
