package org.gradoop.common.model.impl.id;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GradoopIdTest {

  @Test
  public void testEquals() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = id1;

    assertTrue(id1.equals(id1));
    assertFalse(id1.equals(id2));
    assertTrue(id1.equals(id3));
  }

  @Test
  public void testHashCode() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = id1;

    assertTrue(id1.hashCode() == id1.hashCode());
    assertFalse(id1.hashCode() == id2.hashCode());
    assertTrue(id1.hashCode() == id3.hashCode());
  }

  @Test
  public void testCompareTo() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    assertThat(id1.compareTo(id1), is(0));
    assertTrue(id1.compareTo(id2) != 0);
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    GradoopId id1 = GradoopId.get();

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
    GradoopId originalId = GradoopId.get();
    GradoopId toFromStringId = GradoopId.fromString(originalId.toString());
    assertTrue(
      "reconstruction from string failed",
      originalId.equals(toFromStringId)
    );
  }
}