package org.gradoop.model.impl.id.generators;

import org.gradoop.model.impl.id.Context;

public class TestReuseIdGenerator extends ReuseIdGenerator {

  public TestReuseIdGenerator() {
    this(0);
  }

  public TestReuseIdGenerator(int creator) {
    super(0, Context.TEST);
  }
}
