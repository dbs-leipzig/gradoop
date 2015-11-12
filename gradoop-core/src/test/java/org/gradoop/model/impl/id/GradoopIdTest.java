package org.gradoop.model.impl.id;

import org.junit.Assert;
import org.junit.Test;

public class GradoopIdTest {

  @Test(expected = IllegalArgumentException.class)
  public void illegalArgumentTest() {
    new GradoopId(null);
  }

  @Test
  public void toStringTest() {
    GradoopId id = new GradoopId(0L);
    Assert.assertNotNull(id);
    Assert.assertEquals("0", id.toString());
  }

  @Test
  public void compareToTest() {
    GradoopId one = new GradoopId(1L);
    GradoopId two = new GradoopId(2L);

    Assert.assertEquals(-1, one.compareTo(two));
    Assert.assertEquals(0, one.compareTo(one));
    Assert.assertEquals(1, two.compareTo(one));
  }

  @Test
  public void equalsTest() {
    GradoopId one = new GradoopId(1L);
    GradoopId anotherOne = new GradoopId(1L);
    GradoopId two = new GradoopId(2L);

    Assert.assertTrue(one.equals(anotherOne));
    Assert.assertFalse(one.equals(two));
  }
}
