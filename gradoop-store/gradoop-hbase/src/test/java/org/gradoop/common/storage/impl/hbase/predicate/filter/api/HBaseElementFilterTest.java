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
package org.gradoop.common.storage.impl.hbase.predicate.filter.api;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.storage.impl.hbase.filter.api.HBaseElementFilter;
import org.junit.Test;

import javax.annotation.Nonnull;

/**
 * Test class for the {@link HBaseElementFilter} interface
 */
public class HBaseElementFilterTest {

  /**
   * Test that calling the 'or' function of an HBaseElementFilter instance throws the
   * expected NotImplementedException.
   */
  @Test(expected = NotImplementedException.class)
  public void testElementFilterOr() {
    DummyElementFilter<Edge> filter = new DummyElementFilter<>();
    DummyElementFilter<Edge> anotherFilter = new DummyElementFilter<>();

    filter.or(anotherFilter);
  }

  /**
   * Test that calling the 'and' function of an HBaseElementFilter instance throws the
   * expected NotImplementedException.
   */
  @Test(expected = NotImplementedException.class)
  public void testElementFilterAnd() {
    DummyElementFilter<Edge> filter = new DummyElementFilter<>();
    DummyElementFilter<Edge> anotherFilter = new DummyElementFilter<>();

    filter.and(anotherFilter);
  }

  /**
   * Test that calling the 'negate' function of an HBaseElementFilter instance throws the
   * expected NotImplementedException.
   */
  @Test(expected = NotImplementedException.class)
  public void testElementFilterNegate() {
    DummyElementFilter<Edge> filter = new DummyElementFilter<>();

    filter.negate();
  }

  /**
   * A dummy class implements the interface to test
   *
   * @param <E> a EPGM graph element class
   */
  public class DummyElementFilter<E extends EPGMElement> implements HBaseElementFilter<E> {
    @Nonnull
    @Override
    public Filter toHBaseFilter() {
      return new FilterList();
    }
  }
}
