/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.Writable;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.common.config.GradoopConfig;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GradoopTestUtils {

  public static final String SOCIAL_NETWORK_GDL_FILE =
    "/data/gdl/social_network.gdl";

  /**
   * Contains values of all supported property types
   */
  public static Map<String, Object> SUPPORTED_PROPERTIES;

  public static final String KEY_0 = "key0";
  public static final String KEY_1 = "key1";
  public static final String KEY_2 = "key2";
  public static final String KEY_3 = "key3";
  public static final String KEY_4 = "key4";
  public static final String KEY_5 = "key5";
  public static final String KEY_6 = "key6";
  public static final String KEY_7 = "key7";

  public static final Object      NULL_VAL_0        = null;
  public static final boolean     BOOL_VAL_1        = true;
  public static final int         INT_VAL_2         = 23;
  public static final long        LONG_VAL_3        = 23L;
  public static final float       FLOAT_VAL_4       = 2.3f;
  public static final double      DOUBLE_VAL_5      = 2.3;
  public static final String      STRING_VAL_6      = "23";
  public static final BigDecimal  BIG_DECIMAL_VAL_7 = new BigDecimal(23);

  static {
    SUPPORTED_PROPERTIES = Maps.newTreeMap();
    SUPPORTED_PROPERTIES.put(KEY_0, NULL_VAL_0);
    SUPPORTED_PROPERTIES.put(KEY_1, BOOL_VAL_1);
    SUPPORTED_PROPERTIES.put(KEY_2, INT_VAL_2);
    SUPPORTED_PROPERTIES.put(KEY_3, LONG_VAL_3);
    SUPPORTED_PROPERTIES.put(KEY_4, FLOAT_VAL_4);
    SUPPORTED_PROPERTIES.put(KEY_5, DOUBLE_VAL_5);
    SUPPORTED_PROPERTIES.put(KEY_6, STRING_VAL_6);
    SUPPORTED_PROPERTIES.put(KEY_7, BIG_DECIMAL_VAL_7);
  }

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  public static AsciiGraphLoader<GraphHead, Vertex, Edge>
  getSocialNetworkLoader() throws IOException {
    GradoopConfig<GraphHead, Vertex, Edge> config =
      GradoopConfig.getDefaultConfig();

    InputStream inputStream = GradoopTestUtils.class
      .getResourceAsStream(SOCIAL_NETWORK_GDL_FILE);
    return AsciiGraphLoader.fromStream(inputStream, config);
  }

  /**
   * Checks if the two collections contain the same identifiers.
   *
   * @param collection1 first collection
   * @param collection2 second collection
   */
  public static void validateIdEquality(
    Collection<GradoopId> collection1,
    Collection<GradoopId> collection2) {

    List<GradoopId> list1 = Lists.newArrayList(collection1);
    List<GradoopId> list2 = Lists.newArrayList(collection2);

    Collections.sort(list1);
    Collections.sort(list2);
    Iterator<GradoopId> it1 = list1.iterator();
    Iterator<GradoopId> it2 = list2.iterator();

    while(it1.hasNext()) {
      assertTrue("id mismatch", it1.next().equals(it2.next()));
    }
    assertFalse("too many elements in first collection", it1.hasNext());
    assertFalse("too many elements in second collection", it2.hasNext());
  }

  /**
   * Checks if no identifier is contained in both lists.
   *
   * @param collection1 first collection
   * @param collection2 second collection
   */
  public static void validateIdInequality(
    Collection<GradoopId> collection1,
    Collection<GradoopId> collection2) {

    for (GradoopId id1 : collection1) {
      for (GradoopId id2 : collection2) {
        assertFalse("id in both collections", id1.equals(id2));
      }
    }
  }

  /**
   * Checks if two collections contain the same EPGM elements in terms of data
   * (i.e. label and properties).
   *
   * @param collection1 first collection
   * @param collection2 second collection
   */
  public static void validateEPGMElementCollections(
    Collection<? extends EPGMElement> collection1,
    Collection<? extends EPGMElement> collection2) {
    assertNotNull("first collection was null", collection1);
    assertNotNull("second collection was null", collection1);

    List<? extends EPGMElement> list1 = Lists.newArrayList(collection1);
    List<? extends EPGMElement> list2 = Lists.newArrayList(collection2);

    Collections.sort(list1, ID_COMPARATOR);
    Collections.sort(list2, ID_COMPARATOR);

    Iterator<? extends EPGMElement> it1 = list1.iterator();
    Iterator<? extends EPGMElement> it2 = list2.iterator();

    while(it1.hasNext()) {
      validateEPGMElements(
        it1.next(),
        it2.next());
    }
    assertFalse("too many elements in first collection", it1.hasNext());
    assertFalse("too many elements in second collection", it2.hasNext());
  }

  /**
   * Sorts the given collections by element id and checks pairwise if elements
   * are contained in the same graphs.
   *
   * @param collection1 first collection
   * @param collection2 second collection
   */
  public static void validateEPGMGraphElementCollections(
    Collection<? extends EPGMGraphElement> collection1,
    Collection<? extends EPGMGraphElement> collection2) {
    assertNotNull("first collection was null", collection1);
    assertNotNull("second collection was null", collection1);

    List<? extends EPGMGraphElement> list1 = Lists.newArrayList(collection1);
    List<? extends EPGMGraphElement> list2 = Lists.newArrayList(collection2);

    Collections.sort(list1, ID_COMPARATOR);
    Collections.sort(list2, ID_COMPARATOR);

    Iterator<? extends EPGMGraphElement> it1 = list1.iterator();
    Iterator<? extends EPGMGraphElement> it2 = list2.iterator();

    while(it1.hasNext()) {
      validateEPGMGraphElements(
        it1.next(),
        it2.next());
    }
    assertFalse("too many elements in first collection", it1.hasNext());
    assertFalse("too many elements in second collection", it2.hasNext());
  }

  /**
   * Checks if two given EPGM elements are equal by considering their label and
   * properties.
   *
   * @param element1 first element
   * @param element2 second element
   */
  public static void validateEPGMElements(EPGMElement element1,
    EPGMElement element2) {
    assertNotNull("first element was null", element1);
    assertNotNull("second element was null", element2);

    assertEquals("id mismatch", element1.getId(), element2.getId());
    assertEquals("label mismatch", element1.getLabel(), element2.getLabel());

    if (element1.getPropertyCount() == 0) {
      assertEquals("property count mismatch",
        element1.getPropertyCount(), element2.getPropertyCount());
    } else {
      List<String> keys1 = Lists.newArrayList(element1.getPropertyKeys());
      Collections.sort(keys1);

      List<String> keys2 = Lists.newArrayList(element2.getPropertyKeys());
      Collections.sort(keys2);

      Iterator<String> it1 = keys1.iterator();
      Iterator<String> it2 = keys2.iterator();

      while (it1.hasNext() && it2.hasNext()) {
        String key1 = it1.next();
        String key2 = it2.next();
        assertEquals("property key mismatch", key1, key2);
        assertEquals("property value mismatch",
          element1.getPropertyValue(key1),
          element2.getPropertyValue(key2));
      }
      assertFalse("too many properties in first element", it1.hasNext());
      assertFalse("too many properties in second element", it2.hasNext());
    }
  }

  /**
   * Checks if two given EPGM graph elements are equal by considering the
   * graphs they are contained in.
   *
   * @param element1 first element
   * @param element2 second element
   */
  public static void validateEPGMGraphElements(
    EPGMGraphElement element1,
    EPGMGraphElement element2) {
    assertNotNull("first element was null", element1);
    assertNotNull("second element was null", element2);
    assertTrue(
      String.format("graph containment mismatch. expected: %s actual: %s",
        element1.getGraphIds(), element2.getGraphIds()),
      element1.getGraphIds().equals(element2.getGraphIds())
    );
  }

  public static <T extends Writable> T writeAndReadFields(Class<T> clazz, T in)
    throws IOException {
    // write to byte[]
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(outputStream);
    in.write(dataOut);
    dataOut.flush();

    T out;
    try {
      out = clazz.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Cannot initialize the class: " + clazz);
    }

    // read from byte[]
    ByteArrayInputStream inputStream = new ByteArrayInputStream(
      outputStream.toByteArray());
    DataInputStream dataIn = new DataInputStream(inputStream);
    out.readFields(dataIn);

    return out;
  }

  /**
   * Compares to EPGM elements based on their ID.
   */
  private static Comparator<EPGMIdentifiable> ID_COMPARATOR =
    new Comparator<EPGMIdentifiable>() {
      @Override
      public int compare(EPGMIdentifiable o1, EPGMIdentifiable o2) {
        return o1.getId().compareTo(o2.getId());
      }
    };
}
