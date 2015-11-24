package org.gradoop.model.impl.id;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GradoopIdTest {

  @Test
  public void testEquals() throws Exception {
    GradoopId id1 = new GradoopId(23L, 0, Context.TEST);
    GradoopId id2 = new GradoopId(23L, 0, Context.TEST);
    GradoopId id3 = new GradoopId(23L, 1, Context.TEST);
    GradoopId id4 = new GradoopId(42L, 0, Context.TEST);
    GradoopId id5 = new GradoopId(42L, 0, Context.RUNTIME);

    assertTrue(id1.equals(id1));
    assertTrue(id1.equals(id2));
    assertFalse(id1.equals(id3));
    assertFalse(id1.equals(id4));
    assertFalse(id4.equals(id5));
  }

  @Test
  public void testHashCode() throws Exception {
    GradoopId id1 = new GradoopId(23L, 0, Context.TEST);
    GradoopId id2 = new GradoopId(23L, 0, Context.TEST);
    GradoopId id3 = new GradoopId(23L, 1, Context.TEST);
    GradoopId id4 = new GradoopId(42L, 0, Context.TEST);
    GradoopId id5 = new GradoopId(42L, 0, Context.RUNTIME);

    assertTrue(id1.hashCode() == id1.hashCode());
    assertTrue(id1.hashCode() == id2.hashCode());
    assertFalse(id1.hashCode() == id3.hashCode());
    assertFalse(id1.hashCode() == id4.hashCode());
    assertFalse(id4.hashCode() == id5.hashCode());
  }

  @Test
  public void testCompareTo() throws Exception {
    GradoopId id1 = new GradoopId(23L, 0, Context.TEST);
    GradoopId id2 = new GradoopId(23L, 0, Context.TEST);

    assertThat(id1.compareTo(id1), is(0));
    assertThat(id1.compareTo(id2), is(0));

    GradoopId id3 = new GradoopId(23L, 1, Context.TEST);
    assertThat(id1.compareTo(id3), is(-1));
    assertThat(id3.compareTo(id1), is(1));

    GradoopId id4 = new GradoopId(23L, 0, Context.RUNTIME);
    assertThat(id1.compareTo(id4), is(1));
    assertThat(id4.compareTo(id1), is(-1));
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    GradoopId id1 = new GradoopId(23L, 0, Context.TEST);

    // write to byte[]
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    id1.write(dataOut);

    // read from byte[]
    GradoopId id2 = new GradoopId();
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DataInputStream dataIn = new DataInputStream(in);
    id2.readFields(dataIn);

    assertEquals(id1, id2);
  }

  @Test
  public void testToFromString() {
    GradoopId originalId = new GradoopId(47L, 11, Context.RUNTIME);
    GradoopId toFromStringId = GradoopId.fromString(originalId.toString());
    assertTrue(
      "reconstruction from string failed",
      originalId.equals(toFromStringId)
    );
  }
}