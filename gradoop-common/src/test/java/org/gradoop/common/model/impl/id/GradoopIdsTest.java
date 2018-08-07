/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Test;

import com.google.common.collect.Sets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GradoopIdsTest {

  @Test
  public void testAdd() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

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
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.add(id1);

    assertThat(ids.size(), is(1));
    assertTrue(ids.contains(id1));
    assertFalse(ids.contains(id2));
  }

  @Test
  public void testAddAllCollection() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.addAll(Arrays.asList(id1, id2));

    assertThat(ids.size(), is(2));
    assertTrue(ids.contains(id1));
    assertTrue(ids.contains(id2));
    assertFalse(ids.contains(id3));

    ids.addAll(Arrays.asList(id1, id2));
    assertThat(ids.size(), is(2));
  }

  @Test
  public void testAddAllGradoopIds() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopIdSet list1 = new GradoopIdSet();
    list1.add(id1);
    list1.add(id2);

    GradoopIdSet list2 = new GradoopIdSet();
    list2.addAll(list1);

    assertThat(list2.size(), is(2));
    assertTrue(list2.contains(id1));
    assertTrue(list2.contains(id2));

    list2.addAll(list1);
    assertThat(list2.size(), is(2));  }

  @Test
  public void testContainsAllCollection() throws Exception {
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
  public void testContainsAllGradoopIds() throws Exception {
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
  public void testContainsAny() throws Exception {
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
  public void testContainsAny1() throws Exception {
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
  public void testIsEmpty() throws Exception {
    GradoopIdSet set1 = GradoopIdSet.fromExisting(GradoopId.get());
    GradoopIdSet set2 = new GradoopIdSet();

    assertFalse(set1.isEmpty());
    assertTrue(set2.isEmpty());
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

    assertThat(idsRead.size(), is(2));
    assertTrue(idsRead.contains(id1));
    assertTrue(idsRead.contains(id2));
  }

  @Test
  public void testIterator() throws Exception {
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

  @Test(expected = NoSuchElementException.class)
  public void testIteratorException() throws Exception {
    GradoopIdSet ids = new GradoopIdSet();

    Iterator<GradoopId> idsIterator = ids.iterator();

    assertFalse(idsIterator.hasNext());
    idsIterator.next();
  }

  @Test
  public void testClear() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

    GradoopIdSet ids = new GradoopIdSet();
    ids.add(id1);
    ids.add(id2);

    assertThat(ids.size(), is(2));

    ids.clear();

    assertThat(ids.size(), is(0));
  }

  @Test
  public void testSize() throws Exception {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();

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
  public void testFromExisting() {
    GradoopId id1 = GradoopId.get();
    GradoopId id2 = GradoopId.get();
    GradoopId id3 = GradoopId.get();
    GradoopIdSet ids = GradoopIdSet.fromExisting(id1, id2, id3);
    assertThat(ids.size(), is(3));
  }

  @Test
  public void testEquals(){
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    GradoopIdSet abc = GradoopIdSet.fromExisting(a, b, c);
    assertTrue("equals failed for same object", abc.equals(abc));
    assertTrue("hashCode failed for same object", abc.hashCode() == abc.hashCode());

    GradoopIdSet abc2 = GradoopIdSet.fromExisting(a, b, c);
    assertTrue("equals failed for same ids in same order", abc.equals(abc2));
    assertTrue("hashCode failed for same ids in same order", abc.hashCode() == abc2.hashCode());

    GradoopIdSet cba = GradoopIdSet.fromExisting(c, b, a);
    assertTrue("equals succeeds for same ids in different order", abc.equals(cba));
    assertTrue("hashCode succeeds for same ids in different order", abc.hashCode() == cba.hashCode());

    GradoopIdSet aab = GradoopIdSet.fromExisting(a, a, b);
    GradoopIdSet abb = GradoopIdSet.fromExisting(a, b, b);
    assertTrue("equals succeeds for same ids in different cardinality", aab.equals(abb));
    assertTrue("hashCode succeeds for same ids in different cardinality", aab.hashCode() == abb.hashCode());

    GradoopIdSet ab = GradoopIdSet.fromExisting(a, b);
    assertTrue("equals succeeds for same ids but different sizes", aab.equals(ab));
    assertTrue("hashCode succeeds for same ids but different sizes", aab.hashCode() == ab.hashCode());

    GradoopIdSet empty = new GradoopIdSet();
    assertTrue("equals failed for one empty list", !abc.equals(empty));
    assertTrue("hashCode failed for one empty list", abc.hashCode() != empty.hashCode());

    GradoopIdSet empty2 = new GradoopIdSet();
    assertTrue("equals failed for two empty lists", empty2.equals(empty));
    assertTrue("hashCode failed two one empty lists", empty2.hashCode() == empty.hashCode());
  }
}