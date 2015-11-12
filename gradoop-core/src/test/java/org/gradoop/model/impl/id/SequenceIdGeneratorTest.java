package org.gradoop.model.impl.id;

import org.junit.Assert;
import org.junit.Test;

public class SequenceIdGeneratorTest {

  @Test(expected = IllegalArgumentException.class)
  public void illegalArgumentTest() {
    new SequenceIdGenerator(null);
  }

  @Test
  public void createSequenceTest() {
    SequenceIdGenerator generator = new SequenceIdGenerator();

    for (long id = 0L; id < 10L; id++) {
      Assert.assertEquals(generator.createId(), new GradoopId(id));
    }
  }

  @Test
  public void createSequenceWithOffsetTest() {
    SequenceIdGenerator generator = new SequenceIdGenerator(10L);

    for (long id = 10L; id < 20L; id++) {
      Assert.assertEquals(generator.createId(), new GradoopId(id));
    }
  }
}
