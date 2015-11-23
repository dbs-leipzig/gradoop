package org.gradoop.model.impl.id;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.*;

public class GradoopIdTest {

  @Test(expected = IllegalArgumentException.class)
  public void illegalArgumentTest() {
    new GradoopId(null);
  }

  @Test
  public void testHashCode() throws Exception {
    GradoopId id1 = new GradoopId(1L);
    GradoopId id2 = new GradoopId(1L);
    GradoopId id3 = new GradoopId(2L);

    assertTrue(id1.hashCode() == id1.hashCode());
    assertTrue(id1.hashCode() == id2.hashCode());
    assertFalse(id1.hashCode() == id3.hashCode());
  }

  @Test
  public void testEquals() throws Exception {
    GradoopId id1 = new GradoopId(1L);
    GradoopId id2 = new GradoopId(1L);
    GradoopId id3 = new GradoopId(2L);

    assertTrue(id1.equals(id1));
    assertTrue(id1.equals(id2));
    assertFalse(id1.equals(id3));
  }

  @Test
  public void testToString() throws Exception {
    GradoopId id = new GradoopId(0L);
    assertNotNull(id);
    assertEquals("0", id.toString());
  }

  @Test
  public void testCompareTo() throws Exception {
    GradoopId one = new GradoopId(1L);
    GradoopId two = new GradoopId(2L);

    assertEquals(-1, one.compareTo(two));
    assertEquals(0, one.compareTo(one));
    assertEquals(1, two.compareTo(one));
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    GradoopId id1 = new GradoopId(23L);

    // write to byte[]
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    id1.write(dataOut);

    // read from byte[]
    GradoopId id2 = new GradoopId();
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DataInput dataIn = new DataInputStream(in);
    id2.readFields(dataIn);

    assertEquals(id2, id2);
  }

  @Test
  public void testFromLong() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = GradoopId.fromLong(23L);

    assertEquals(id1, id2);
  }

  @Test
  public void testFromLongString() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = GradoopId.fromLongString("23");

    assertEquals(id1, id2);
  }

  @Test
  public void testMin() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);

    GradoopId min = GradoopId.min(id1, id2);
    assertEquals(min, id1);

    min = GradoopId.min(id1, id1);
    assertEquals(min, id1);
  }
}
