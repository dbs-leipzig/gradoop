/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.util;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.comparators.IdentifiableComparator;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Common test utilities for temporal graphs and graph elements.
 */
public class TemporalGradoopTestUtils {
  /**
   * Checks if two TPGM collections contain the same TPGM elements in terms of data
   * (i.e. label and properties).
   *
   * @param collection1 first collection
   * @param collection2 second collection
   */
  public static void validateTPGMElementCollections(
    Collection<? extends TemporalElement> collection1,
    Collection<? extends TemporalElement> collection2) {

    // First, validate EPGM information
    GradoopTestUtils.validateElementCollections(collection1, collection2);

    List<? extends TemporalElement> list1 = new ArrayList<>(collection1);
    List<? extends TemporalElement> list2 = new ArrayList<>(collection2);
    assertEquals("Collection sizes differ.", list1.size(), list2.size());

    list1.sort(new IdentifiableComparator());
    list2.sort(new IdentifiableComparator());

    Iterator<? extends TemporalElement> it1 = list1.iterator();
    Iterator<? extends TemporalElement> it2 = list2.iterator();

    while (it1.hasNext()) {
      TemporalElement firstElement = it1.next();
      TemporalElement secondElement = it2.next();

      assertEquals(firstElement.getTransactionTime(), secondElement.getTransactionTime());
      assertEquals(firstElement.getValidTime(), secondElement.getValidTime());
    }
    assertFalse("Too many elements in second collection", it2.hasNext());
  }
}
