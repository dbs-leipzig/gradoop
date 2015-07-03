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
import org.gradoop.model.EPVertexData;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGraphCollectionTest extends EPFlinkTest {

  @Test
  public void testGetGraph() throws Exception {
    EPGraphStore graphStore = createSocialGraph();
    EPGraphCollection graphColl = graphStore.getCollection();

    EPGraph graphCommunity = graphColl.getGraph(2L);
    assertNotNull("graph was null", graphCommunity);
    assertEquals("wrong label", LABEL_COMMUNITY, graphCommunity.getLabel());
  }

  @Test
  public void testGetGraphs() throws Exception {
    EPGraphStore graphStore = createSocialGraph();
    EPGraphCollection graphColl = graphStore.getCollection();

    EPGraphCollection graphs = graphColl.getGraphs(0l, 1l);

    assertNotNull("graph collection is null", graphs);
    assertEquals("wrong number of graphs", 2, graphs.size());
    assertEquals("wrong number of vertices", 6,
      graphs.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 10, graphs.getGraph().getEdgeCount());
  }

  @Test
  public void testSelect() throws Exception {
    EPGraphStore graphStore = createSocialGraph();
    EPGraphCollection graphColl = graphStore.getCollection();

    EPGraphCollection selectedGraphs =
      graphColl.select(new Predicate<EPGraph>() {

        @Override
        public boolean filter(EPGraph entity) throws Exception {
          if (entity.getVertices().filter(new Predicate<EPVertexData>() {
            @Override
            public boolean filter(EPVertexData entity) throws Exception {
              if (entity.getLabel().equals("Person")) {
                Map<String, Object> properties = entity.getProperties();
                if (properties != null) {
                  if (properties.get("city").equals("Leipzig")) {
                    return true;
                  }
                }
              }
              return false;
            }
          }).size() > 0) {
            return true;
          }
          return false;
        }
      });
    assertNotNull("graph was null", selectedGraphs);
  }

  @Test
  public void testUnion() throws Exception {
    EPGraphStore graphStore = createSocialGraph();
    EPGraphCollection graphColl = graphStore.getCollection();
    EPGraphCollection collection1 = graphColl.getGraphs(0l, 2l);
    EPGraphCollection collection2 = graphColl.getGraphs(2l);

    EPGraphCollection unionCollection = collection1.union(collection2);

    assertNotNull("graph collection is null", unionCollection);
    assertEquals("wrong number of graphs", 2, unionCollection.size());
    assertEquals("wrong number of vertices", 5,
      unionCollection.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 8,
      unionCollection.getGraph().getEdgeCount());
  }

  @Test
  public void testIntersect() throws Exception {
    EPGraphStore graphStore = createSocialGraph();
    EPGraphCollection graphColl = graphStore.getCollection();
    EPGraphCollection collection1 = graphColl.getGraphs(0l, 2l);
    EPGraphCollection collection2 = graphColl.getGraphs(1l, 2l);

    EPGraphCollection unionCollection = collection1.intersect(collection2);

    assertNotNull("graph collection is null", unionCollection);
    assertEquals("wrong number of graphs", 1, unionCollection.size());
    assertEquals("wrong number of vertices", 4,
      unionCollection.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 6,
      unionCollection.getGraph().getEdgeCount());
  }

  @Test
  public void testDifference() throws Exception {
    EPGraphStore graphStore = createSocialGraph();
    EPGraphCollection graphColl = graphStore.getCollection();
    EPGraphCollection collection1 = graphColl.getGraphs(0l, 1l);
    EPGraphCollection collection2 = graphColl.getGraphs(0l, 2l);

    EPGraphCollection unionCollection = collection1.difference(collection2);

    assertNotNull("graph collection is null", unionCollection);
    assertEquals("wrong number of graphs", 1, unionCollection.size());
    assertEquals("wrong number of vertices", 3,
      unionCollection.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 4,
      unionCollection.getGraph().getEdgeCount());
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