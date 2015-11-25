package org.gradoop.model.impl.id.generators;

import org.gradoop.model.impl.id.Context;
import org.gradoop.model.impl.id.ReuseIdGenerator;

/**
 * Convenient reuse ID generator for unit testing.
 */
public class TestReuseIdGenerator extends ReuseIdGenerator {

  public TestReuseIdGenerator() {
    this(0);
  }

  public TestReuseIdGenerator(int creator) {
    super(0, Context.TEST);
  }
}
