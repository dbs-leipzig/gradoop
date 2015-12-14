package org.gradoop.model.impl.properties;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.gradoop.GradoopTestUtils.writeAndReadFields;
import static org.junit.Assert.*;

public class PropertyValueListTest {

  @Test
  public void testEqualsAndHashCode() throws IOException {
    PropertyValueList p1 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );
    PropertyValueList p2 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );
    PropertyValueList p3 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(3L))
    );

    assertTrue(p1.equals(p2));
    assertFalse(p1.equals(p3));

    assertTrue(p1.hashCode() == p2.hashCode());
    assertFalse(p1.hashCode() == p3.hashCode());
  }

  @Test
  public void testCompareTo() throws Exception {
    PropertyValueList p1 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );
    PropertyValueList p2 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );
    PropertyValueList p3 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(3L))
    );

    assertEquals(0, p1.compareTo(p2));
    assertEquals(-1, p1.compareTo(p3));
    assertEquals(1, p3.compareTo(p1));
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    PropertyValueList p1 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );

    PropertyValueList p2 = writeAndReadFields(PropertyValueList.class, p1);

    assertEquals(p1, p2);
  }

  @Test
  public void testIterator() throws Exception {
    PropertyValueList p = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );

    List<PropertyValue> expected = Lists.newArrayList(p);

    assertEquals(2, expected.size());
    assertTrue(expected.contains(PropertyValue.create(1L)));
    assertTrue(expected.contains(PropertyValue.create(2L)));
  }

  @Test
  public void testEmptyIterator() throws Exception {
    PropertyValueList p = new PropertyValueList();

    List<PropertyValue> expected = Lists.newArrayList(p);
    assertEquals(0, expected.size());
  }
}