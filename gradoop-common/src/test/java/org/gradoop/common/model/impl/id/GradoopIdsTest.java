/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.model.impl.id;

import com.google.common.collect.Sets;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.testng.AssertJUnit.*;
import static org.testng.Assert.assertNotEquals;

public class GradoopIdsTest {

  @Test
  public void testAdd() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();

    assertEquals(0, ids.size());

    ids.add(id1);
    assertEquals(1, ids.size());
    assertTrue(ids.contains(id1));
    // must not change
    ids.add(id1);
    assertEquals(1, ids.size());
    assertTrue(ids.contains(id1));
    // must change
    ids.add(id2);
    assertEquals(2, ids.size());
    assertTrue(ids.contains(id1));
    assertTrue(ids.contains(id2));
  }

  @Test
  public void testContains() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.add(id1);

    assertEquals(1, ids.size());
    assertTrue(ids.contains(id1));
    assertFalse(ids.contains(id2));
  }

  @Test
  public void testAddAllCollection() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Arrays.asList(id1, id2));

    assertEquals(2, ids.size());
    assertTrue(ids.contains(id1));
    assertTrue(ids.contains(id2));
    assertFalse(ids.contains(id3));

    ids.addAll(Arrays.asList(id1, id2));
    assertEquals(2, ids.size());
  }

  @Test
  public void testAddAllGradoopIds() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopIdSet list1 = new GradoopIdSet();
    list1.add(id1);
    list1.add(id2);

    GradoopIdSet list2 = new GradoopIdSet();
    list2.addAll(list1);

    assertEquals(2, list2.size());
    assertTrue(list2.contains(id1));
    assertTrue(list2.contains(id2));

    list2.addAll(list1);
    assertEquals(2, list2.size());
  }

  @Test
  public void testContainsAllCollection() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Arrays.asList(id1, id2));

    assertTrue(ids.containsAll(Sets.newHashSet(id1)));
    assertTrue(ids.containsAll(Sets.newHashSet(id2)));
    assertTrue(ids.containsAll(Sets.newHashSet(id1, id2)));
    assertFalse(ids.containsAll(Sets.newHashSet(id3)));
    assertFalse(ids.containsAll(Sets.newHashSet(id1, id3)));
  }

  @Test
  public void testContainsAllGradoopIds() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();


    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Arrays.asList(id1, id2));

    assertTrue(ids.containsAll(GradoopIdSet.fromExisting(id1)));
    assertTrue(ids.containsAll(GradoopIdSet.fromExisting(id2)));
    assertTrue(ids.containsAll(GradoopIdSet.fromExisting(id1, id2)));
    assertFalse(ids.containsAll(GradoopIdSet.fromExisting(id3)));
    assertFalse(ids.containsAll(GradoopIdSet.fromExisting(id1, id3)));
  }

  @Test
  public void testContainsAny() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();


    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Arrays.asList(id1, id2));

    assertTrue(ids.containsAny(Sets.newHashSet(id1)));
    assertTrue(ids.containsAny(Sets.newHashSet(id2)));
    assertTrue(ids.containsAny(Sets.newHashSet(id1, id2)));
    assertFalse(ids.containsAny(Sets.newHashSet(id3)));
    assertTrue(ids.containsAny(Sets.newHashSet(id1, id3)));
  }

  @Test
  public void testContainsAny1() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Arrays.asList(id1, id2));

    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id1)));
    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id2)));
    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id1, id2)));
    assertFalse(ids.containsAny(GradoopIdSet.fromExisting(id3)));
    assertTrue(ids.containsAny(GradoopIdSet.fromExisting(id1, id3)));
  }

  @Test
  public void testIsEmpty() {
    GradoopIdSet set1 = GradoopIdSet.fromExisting(GradoopId.get());

    assertFalse(set1.isEmpty());
    assertTrue(new GradoopIdSet().isEmpty());
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet idsWrite = GradoopIdSet.fromExisting(id1, id2);

    // write to byte[]
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputView dataOutputView = new DataOutputViewStreamWrapper(out);
    idsWrite.write(dataOutputView);

    // read from byte[]
    GradoopIdSet idsRead = new GradoopIdSet();
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DataInputView dataInputView = new DataInputViewStreamWrapper(in);
    idsRead.read(dataInputView);

    assertEquals(2, idsRead.size());
    assertTrue(idsRead.contains(id1));
    assertTrue(idsRead.contains(id2));
  }

  @Test
  public void testIterator() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet ids = GradoopIdSet.fromExisting(id1, id2);

    Iterator<GradoopId> idsIterator = ids.iterator();

    assertTrue(idsIterator.hasNext());
    assertNotNull(idsIterator.next());
    assertTrue(idsIterator.hasNext());
    assertNotNull(idsIterator.next());
    assertFalse(idsIterator.hasNext());
  }

  @Test(expectedExceptions = NoSuchElementException.class)
  public void testIteratorException() {
    GradoopIdSet ids = new GradoopIdSet();

    Iterator<GradoopId> idsIterator = ids.iterator();

    assertFalse(idsIterator.hasNext());
    idsIterator.next();
  }

  @Test
  public void testClear() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.add(id1);
    ids.add(id2);

    assertEquals(2, ids.size());

    ids.clear();

    assertEquals(0, ids.size());
  }

  @Test
  public void testSize() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    assertEquals(0, ids.size());
    ids.add(id1);
    assertEquals(1, ids.size());
    ids.add(id1);
    assertEquals(1, ids.size());
    ids.add(id2);
    assertEquals(2, ids.size());
  }

  @Test
  public void testFromExisting() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();
    GradoopIdSet ids = GradoopIdSet.fromExisting(id1, id2, id3);
    assertEquals(3, ids.size());
  }

  @Test
  public void testEquals() {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    GradoopIdSet abc = GradoopIdSet.fromExisting(a, b, c);
    assertEquals("equals failed for same object", abc, abc);
    assertEquals("hashCode failed for same object", abc.hashCode(), abc.hashCode());

    GradoopIdSet abc2 = GradoopIdSet.fromExisting(a, b, c);
    assertEquals("equals failed for same ids in same order", abc, abc2);
    assertEquals("hashCode failed for same ids in same order", abc.hashCode(), abc2.hashCode());

    GradoopIdSet cba = GradoopIdSet.fromExisting(c, b, a);
    assertEquals("equals succeeds for same ids in different order", abc, cba);
    assertEquals("hashCode succeeds for same ids in different order", abc.hashCode(), cba.hashCode());

    GradoopIdSet aab = GradoopIdSet.fromExisting(a, a, b);
    GradoopIdSet abb = GradoopIdSet.fromExisting(a, b, b);
    assertEquals("equals succeeds for same ids in different cardinality", aab, abb);
    assertEquals("hashCode succeeds for same ids in different cardinality", aab.hashCode(), abb.hashCode());

    GradoopIdSet ab = GradoopIdSet.fromExisting(a, b);
    assertEquals("equals succeeds for same ids but different sizes", aab, ab);
    assertEquals("hashCode succeeds for same ids but different sizes", aab.hashCode(), ab.hashCode());

    GradoopIdSet empty = new GradoopIdSet();
    assertNotEquals(empty, abc, "equals failed for one empty list");
    assertTrue("hashCode failed for one empty list", abc.hashCode() != empty.hashCode());

    GradoopIdSet empty2 = new GradoopIdSet();
    assertEquals("equals failed for two empty lists", empty2, empty);
    assertEquals("hashCode failed two one empty lists", empty2.hashCode(), empty.hashCode());
  }
}
