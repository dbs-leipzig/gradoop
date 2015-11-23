package org.gradoop.model.impl.id;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GradoopIdSetTest {

  @Test
  public void testAdd() throws Exception {
    GradoopId id1 = GradoopId.fromLong(23L);
    GradoopId id2 = GradoopId.fromLong(42L);
    GradoopIdSet ids = new GradoopIdSet();

    assertThat(ids.size(), is(0));

    ids.add(id1);
    assertThat(ids.size(), is(1));
    assertTrue(ids.contains(id1));
    // must not change
    ids.add(id1);
    assertThat(ids.size(), is(1));
    assertTrue(ids.contains(id1));
    // must change
    ids.add(id2);
    assertThat(ids.size(), is(2));
    assertTrue(ids.contains(id1));
    assertTrue(ids.contains(id2));
  }

  @Test
  public void testContains() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);

    GradoopIdSet ids = new GradoopIdSet();
    ids.add(id1);

    assertThat(ids.size(), is(1));
    assertTrue(ids.contains(id1));
    assertFalse(ids.contains(id2));
  }

  @Test
  public void testAddAll() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);
    GradoopId id3 = new GradoopId(84L);

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Lists.newArrayList(id1, id2));

    assertThat(ids.size(), is(2));
    assertTrue(ids.contains(id1));
    assertTrue(ids.contains(id2));
    assertFalse(ids.contains(id3));
  }

  @Test
  public void testAddAll1() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);
    GradoopIdSet ids1 = new GradoopIdSet();
    ids1.add(id1);
    ids1.add(id2);

    GradoopIdSet ids2 = new GradoopIdSet();
    ids2.addAll(ids1);

    assertThat(ids2.size(), is(2));
    assertTrue(ids2.contains(id1));
    assertTrue(ids2.contains(id2));
  }

  @Test
  public void testContainsAll() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);
    GradoopId id3 = new GradoopId(84L);

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Lists.newArrayList(id1, id2));

    assertTrue(ids.containsAll(Lists.newArrayList(id1)));
    assertTrue(ids.containsAll(Lists.newArrayList(id2)));
    assertTrue(ids.containsAll(Lists.newArrayList(id1, id2)));
    assertFalse(ids.containsAll(Lists.newArrayList(id3)));
    assertFalse(ids.containsAll(Lists.newArrayList(id1, id3)));
  }

  @Test
  public void testContainsAll1() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);
    GradoopId id3 = new GradoopId(84L);

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Lists.newArrayList(id1, id2));

    assertTrue(ids.containsAll(GradoopIdSet.fromExisting(id1)));
    assertTrue(ids.containsAll(GradoopIdSet.fromExisting(id2)));
    assertTrue(ids.containsAll(GradoopIdSet.fromExisting(id1, id2)));
    assertFalse(ids.containsAll(GradoopIdSet.fromExisting(id3)));
    assertFalse(ids.containsAll(GradoopIdSet.fromExisting(id1, id3)));
  }

  @Test
  public void testContainsAny() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);
    GradoopId id3 = new GradoopId(84L);

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Lists.newArrayList(id1, id2));

    assertTrue(ids.containsAny(Lists.newArrayList(id1)));
    assertTrue(ids.containsAny(Lists.newArrayList(id2)));
    assertTrue(ids.containsAny(Lists.newArrayList(id1, id2)));
    assertFalse(ids.containsAny(Lists.newArrayList(id3)));
    assertTrue(ids.containsAny(Lists.newArrayList(id1, id3)));
  }

  @Test
  public void testContainsAny1() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);
    GradoopId id3 = new GradoopId(84L);

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Lists.newArrayList(id1, id2));

    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id1)));
    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id2)));
    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id1, id2)));
    assertFalse(ids.containsAny(GradoopIdSet.fromExisting(id3)));
    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id1, id3)));
  }

  @Test
  public void testIsEmpty() throws Exception {
    GradoopIdSet ids1 = GradoopIdSet.fromExisting(new GradoopId(23L));
    GradoopIdSet ids2 = new GradoopIdSet();

    assertFalse(ids1.isEmpty());
    assertTrue(ids2.isEmpty());
  }

  @Test
  public void testToCollection() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);

    GradoopIdSet ids = GradoopIdSet.fromExisting(id1, id2);

    Collection<GradoopId> coll = ids.toCollection();

    assertThat(coll.size(), is(2));
    assertTrue(coll.contains(id1));
    assertTrue(coll.contains(id2));
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);

    GradoopIdSet idsWrite = GradoopIdSet.fromExisting(id1, id2);

    // write to byte[]
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    idsWrite.write(dataOut);

    // read from byte[]
    GradoopIdSet idsRead = new GradoopIdSet();
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DataInputStream dataIn = new DataInputStream(in);
    idsRead.readFields(dataIn);

    assertThat(idsRead.size(), is(2));
    assertTrue(idsRead.contains(id1));
    assertTrue(idsRead.contains(id2));
  }

  @Test
  public void testIterator() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);

    GradoopIdSet ids = GradoopIdSet.fromExisting(id1, id2);

    Iterator<GradoopId> idsIterator = ids.iterator();

    assertTrue(idsIterator.hasNext());
    assertNotNull(idsIterator.next());
    assertTrue(idsIterator.hasNext());
    assertNotNull(idsIterator.next());
    assertFalse(idsIterator.hasNext());
  }

  @Test(expected = NoSuchElementException.class)
  public void testIterator1() throws Exception {
    GradoopIdSet ids = new GradoopIdSet();

    Iterator<GradoopId> idsIterator = ids.iterator();

    assertFalse(idsIterator.hasNext());
    idsIterator.next();
  }

  @Test
  public void testClear() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);

    GradoopIdSet ids = new GradoopIdSet();
    ids.add(id1);
    ids.add(id2);

    assertThat(ids.size(), is(2));

    ids.clear();

    assertThat(ids.size(), is(0));
  }

  @Test
  public void testSize() throws Exception {
    GradoopId id1 = new GradoopId(23L);
    GradoopId id2 = new GradoopId(42L);

    GradoopIdSet ids = new GradoopIdSet();
    assertThat(ids.size(), is(0));
    ids.add(id1);
    assertThat(ids.size(), is(1));
    ids.add(id1);
    assertThat(ids.size(), is(1));
    ids.add(id2);
    assertThat(ids.size(), is(2));
  }

  @Test
  public void testFromLongs() throws Exception {
    Long id1 = 23L;
    Long id2 = 42L;
    Long id3 = 84L;
    GradoopIdSet ids = GradoopIdSet.fromLongs(id1, id2, id3);
    assertThat(ids.size(), is(3));
    assertTrue(ids.contains(new GradoopId(id1)));
    assertTrue(ids.contains(new GradoopId(id2)));
    assertTrue(ids.contains(new GradoopId(id3)));
  }

  @Test
  public void testFromLongs1() throws Exception {
    Long id1 = 23L;
    Long id2 = 42L;
    Long id3 = 84L;
    GradoopIdSet ids = GradoopIdSet.fromLongs(
      Lists.newArrayList(id1, id2, id3));
    assertThat(ids.size(), is(3));
    assertTrue(ids.contains(new GradoopId(id1)));
    assertTrue(ids.contains(new GradoopId(id2)));
    assertTrue(ids.contains(new GradoopId(id3)));
  }

  @Test
  public void testFromExisting() {
    GradoopIdSet ids = GradoopIdSet.fromExisting(
      GradoopId.fromLong(0L),
      GradoopId.fromLong(1L),
      GradoopId.fromLong(2L)
    );
    assertThat(ids.size(), is(3));
  }
}