package org.gradoop.model.impl.id;

import org.gradoop.model.impl.id.Context;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.SequenceIdGenerator;
import org.junit.Test;

import static org.junit.Assert.*;

public class SequenceIdGeneratorTest {

  @Test
  public void testCreateSequence() {
    SequenceIdGenerator generator =
      new SequenceIdGenerator(0, Context.TEST);

    for (long id = 0L; id < 10L; id++) {
      assertEquals(generator.createId(), new GradoopId(id, 0, Context.TEST));
    }
  }

  @Test
  public void testCreateSequenceWithOffset() {
    SequenceIdGenerator generator =
      new SequenceIdGenerator(10L, 0, Context.TEST);

    for (long id = 10L; id < 20L; id++) {
      assertEquals(generator.createId(), new GradoopId(id, 0, Context.TEST));
    }
  }
}