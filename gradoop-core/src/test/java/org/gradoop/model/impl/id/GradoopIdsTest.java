package org.gradoop.model.impl.id;

import junit.framework.TestCase;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by peet on 17.11.15.
 */
public class GradoopIdsTest{

  public void testAdd() throws Exception {

  }

  public void testContains() throws Exception {

  }

  public void testAddAll() throws Exception {

  }

  public void testAddAll1() throws Exception {

  }

  public void testContainsAll() throws Exception {

  }

  public void testContainsAll1() throws Exception {

  }

  public void testIsEmpty() throws Exception {

  }

  public void testToCollection() throws Exception {

  }

  public void testWrite() throws Exception {

  }

  public void testReadFields() throws Exception {

  }

  public void testIterator() throws Exception {

  }

  public void testClear() throws Exception {

  }

  public void testSize() throws Exception {

  }

  @Test
  public void testFromLongs() throws Exception {
    GradoopIds ids = GradoopIds.fromLongs(0L, 1L, 3L);
    assertThat(ids.size(), is(3));
  }

  @Test
  public void createFromExisting() {
    GradoopIds ids = GradoopIds.fromExisting(
      GradoopId.fromLong(0L),
      GradoopId.fromLong(1L),
      GradoopId.fromLong(2L)
    );
    assertThat(ids.size(), is(3));
  }
}