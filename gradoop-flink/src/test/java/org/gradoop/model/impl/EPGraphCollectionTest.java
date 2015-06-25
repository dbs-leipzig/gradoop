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

package org.gradoop.model.impl;

import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.*;

public class EPGraphCollectionTest extends EPFlinkTest {

  @Test
  public void testGetGraph() throws Exception {
    EPGraphStore graphStore = createSocialGraph();
    EPGraphCollection graphColl = graphStore.getCollection();

    EPGraph graphCommunity = graphColl.getGraph(2L);
    assertNotNull("graph was null", graphCommunity);
    assertEquals("wrong label", LABEL_COMMUNITY, graphCommunity.getLabel());
  }

  public void testSelect() throws Exception {

  }

  public void testUnion() throws Exception {

  }

  public void testIntersect() throws Exception {

  }

  public void testDifference() throws Exception {

  }

  public void testDistinct() throws Exception {

  }

  public void testSortBy() throws Exception {

  }

  public void testTop() throws Exception {

  }

  public void testApply() throws Exception {

  }

  public void testReduce() throws Exception {

  }

  public void testCallForGraph() throws Exception {

  }

  public void testCallForCollection() throws Exception {

  }
}