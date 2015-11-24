package org.gradoop.model.impl.id.generators;

import org.gradoop.model.impl.id.Context;
import org.gradoop.model.impl.id.SequenceIdGenerator;

/**
 * Convenient class to create GradoopId instances during unit testing.
 */
public class TestSequenceIdGenerator extends SequenceIdGenerator {

  public TestSequenceIdGenerator() {
    super(0, Context.TEST);
  }
}
