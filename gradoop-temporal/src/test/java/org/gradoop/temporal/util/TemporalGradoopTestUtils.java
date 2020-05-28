/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.comparators.IdentifiableComparator;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Common test utilities for temporal graphs and graph elements.
 */
public class TemporalGradoopTestUtils {

  /**
   * The resource path to the temporal social network GDL file.
   */
  public static final String SOCIAL_NETWORK_TEMPORAL_GDL_FILE = "/data/gdl/social_network_temporal.gdl";
  /**
   * A property key used to store the valid-from time as a property.
   */
  public static final String PROPERTY_VALID_FROM = "__valFrom";
  /**
   * A property key used to store the valid-to time as a property.
   */
  public static final String PROPERTY_VALID_TO = "__valTo";

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
  }

  /**
   * Extract the valid time from properties of an {@link Element}.
   * Those times are expected to be stored as {@value #PROPERTY_VALID_FROM} and {@value #PROPERTY_VALID_TO}.
   *
   * @param element The element storing valid times as properties.
   * @return The valid time extracted from that element.
   */
  public static Tuple2<Long, Long> extractTime(Element element) {
    Tuple2<Long, Long> validTime = new Tuple2<>(TemporalElement.DEFAULT_TIME_FROM,
      TemporalElement.DEFAULT_TIME_TO);

    if (element.hasProperty(PROPERTY_VALID_FROM)) {
      validTime.f0 = element.getPropertyValue(PROPERTY_VALID_FROM).getLong();
    }
    if (element.hasProperty(PROPERTY_VALID_TO)) {
      validTime.f1 = element.getPropertyValue(PROPERTY_VALID_TO).getLong();
    }
    return validTime;
  }
}
