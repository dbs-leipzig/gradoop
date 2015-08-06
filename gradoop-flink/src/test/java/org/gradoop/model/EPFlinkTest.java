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

package org.gradoop.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.EdgeDataFactory;
import org.gradoop.model.impl.FlinkGraphStore;
import org.gradoop.model.impl.GraphDataFactory;
import org.gradoop.model.impl.VertexDataFactory;
import org.gradoop.model.store.EPGraphStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class EPFlinkTest {

  protected static final String LABEL_COMMUNITY = "Community";
  protected static final String LABEL_PERSON = "Person";
  protected static final String LABEL_FORUM = "Forum";
  protected static final String LABEL_TAG = "Tag";
  protected static final String LABEL_KNOWS = "knows";
  protected static final String LABEL_HAS_MODERATOR = "hasModerator";
  protected static final String LABEL_HAS_MEMBER = "hasMember";
  protected static final String LABEL_HAS_INTEREST = "hasInterest";
  protected static final String LABEL_HAS_TAG = "hasTag";
  protected static final String PROPERTY_KEY_NAME = "name";
  protected static final String PROPERTY_KEY_GENDER = "gender";
  protected static final String PROPERTY_KEY_CITY = "city";
  protected static final String PROPERTY_KEY_SPEAKS = "speaks";
  protected static final String PROPERTY_KEY_LOC_IP = "locIP";
  protected static final String PROPERTY_KEY_TITLE = "title";
  protected static final String PROPERTY_KEY_SINCE = "since";
  protected static final String PROPERTY_KEY_INTEREST = "interest";
  protected static final String PROPERTY_KEY_VERTEX_COUNT = "vertexCount";

  protected ExecutionEnvironment env =
    ExecutionEnvironment.getExecutionEnvironment();

  protected VertexData alice;
  protected VertexData bob;
  protected VertexData carol;
  protected VertexData dave;
  protected VertexData eve;
  protected VertexData frank;
  protected VertexData tagDatabases;
  protected VertexData tagGraphs;
  protected VertexData tagHadoop;
  protected VertexData forumGDBS;
  protected VertexData forumGPS;

  protected EdgeData edge0;
  protected EdgeData edge1;
  protected EdgeData edge2;
  protected EdgeData edge3;
  protected EdgeData edge4;
  protected EdgeData edge5;
  protected EdgeData edge6;
  protected EdgeData edge7;
  protected EdgeData edge8;
  protected EdgeData edge9;
  protected EdgeData edge10;
  protected EdgeData edge11;
  protected EdgeData edge12;
  protected EdgeData edge13;
  protected EdgeData edge14;
  protected EdgeData edge15;
  protected EdgeData edge16;
  protected EdgeData edge17;
  protected EdgeData edge18;
  protected EdgeData edge19;
  protected EdgeData edge20;
  protected EdgeData edge21;
  protected EdgeData edge22;
  protected EdgeData edge23;

  /**
   * Creates a social network as a basis for tests.
   * <p>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  protected EPGraphStore createSocialGraph() {

    // vertices

    // Person:Alice (0L)
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Alice");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Leipzig");
    alice = VertexDataFactory.createDefaultVertex(0L, LABEL_PERSON, properties,
      Sets.newHashSet(0L, 2L));
    // Person:Bob (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Bob");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Leipzig");
    bob = VertexDataFactory.createDefaultVertex(1L, LABEL_PERSON, properties,
      Sets.newHashSet(0L, 2L));
    // Person:Carol (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Carol");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    carol = VertexDataFactory.createDefaultVertex(2L, LABEL_PERSON, properties,
      Sets.newHashSet(1L, 2L, 3L));
    // Person:Dave (3L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Dave");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    dave = VertexDataFactory.createDefaultVertex(3L, LABEL_PERSON, properties,
      Sets.newHashSet(1L, 2L, 3L));
    // Person:Eve (4L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Eve");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    properties.put(PROPERTY_KEY_SPEAKS, "English");
    eve = VertexDataFactory
      .createDefaultVertex(4L, LABEL_PERSON, properties, Sets.newHashSet(0L));
    // Person:Frank (5L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Frank");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Berlin");
    properties.put(PROPERTY_KEY_LOC_IP, "127.0.0.1");
    frank = VertexDataFactory
      .createDefaultVertex(5L, LABEL_PERSON, properties, Sets.newHashSet(1L));

    // Tag:Databases (6L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Databases");
    tagDatabases = VertexDataFactory
      .createDefaultVertexWithProperties(6L, LABEL_TAG, properties);
    // Tag:Databases (7L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Graphs");
    tagGraphs = VertexDataFactory
      .createDefaultVertexWithProperties(7L, LABEL_TAG, properties);
    // Tag:Databases (8L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Hadoop");
    tagHadoop = VertexDataFactory
      .createDefaultVertexWithProperties(8L, LABEL_TAG, properties);

    // Forum:Graph Databases (9L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_TITLE, "Graph Databases");
    forumGDBS = VertexDataFactory
      .createDefaultVertexWithProperties(9L, LABEL_FORUM, properties);
    // Forum:Graph Processing (10L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_TITLE, "Graph Processing");
    forumGPS = VertexDataFactory
      .createDefaultVertex(10L, LABEL_FORUM, properties, Sets.newHashSet(3L));

    List<VertexData> vertices = Lists
      .newArrayList(alice, bob, carol, dave, eve, frank, tagDatabases,
        tagGraphs, tagHadoop, forumGDBS, forumGPS);

    // sna_edges
    List<EdgeData> edges = Lists.newArrayList();
    // Person:Alice-[knows]->Person:Bob (0L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge0 = EdgeDataFactory
      .createDefaultEdge(0L, LABEL_KNOWS, alice.getId(), bob.getId(),
        properties, Sets.newHashSet(0L, 2L));
    edges.add(edge0);
    // Person:Bob-[knows]->Person:Alice (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge1 = EdgeDataFactory
      .createDefaultEdge(1L, LABEL_KNOWS, bob.getId(), alice.getId(),
        properties, Sets.newHashSet(0L, 2L));
    edges.add(edge1);
    // Person:Bob-[knows]->Person:Carol (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge2 = EdgeDataFactory
      .createDefaultEdge(2L, LABEL_KNOWS, bob.getId(), carol.getId(),
        properties, Sets.newHashSet(2L));
    edges.add(edge2);
    // Person:Carol-[knows]->Person:Bob (3L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge3 = EdgeDataFactory
      .createDefaultEdge(3L, LABEL_KNOWS, carol.getId(), bob.getId(),
        properties, Sets.newHashSet(2L));
    edges.add(edge3);
    // Person:Carol-[knows]->Person:Dave (4L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge4 = EdgeDataFactory
      .createDefaultEdge(4L, LABEL_KNOWS, carol.getId(), dave.getId(),
        properties, Sets.newHashSet(1L, 2L, 3L));
    edges.add(edge4);
    // Person:Dave-[knows]->Person:Carol (5L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge5 = EdgeDataFactory
      .createDefaultEdge(5L, LABEL_KNOWS, dave.getId(), carol.getId(),
        properties, Sets.newHashSet(1L, 2L));
    edges.add(edge5);
    // Person:Eve-[knows]->Person:Alice (6L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge6 = EdgeDataFactory
      .createDefaultEdge(6L, LABEL_KNOWS, eve.getId(), alice.getId(),
        properties, Sets.newHashSet(0L));
    edges.add(edge6);
    // Person:Eve-[knows]->Person:Bob (21L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    edge21 = EdgeDataFactory
      .createDefaultEdge(21L, LABEL_KNOWS, eve.getId(), bob.getId(), properties,
        Sets.newHashSet(0L));
    edges.add(edge21);
    // Person:Frank-[knows]->Person:Carol (22L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    edge22 = EdgeDataFactory
      .createDefaultEdge(22L, LABEL_KNOWS, frank.getId(), carol.getId(),
        properties, Sets.newHashSet(1L));
    edges.add(edge22);
    // Person:Frank-[knows]->Person:Dave (23L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    edge23 = EdgeDataFactory
      .createDefaultEdge(23L, LABEL_KNOWS, frank.getId(), dave.getId(),
        properties, Sets.newHashSet(1L));
    edges.add(edge23);
    // Person:Eve-[hasInterest]->Tag:Databases (7L)
    edge7 = EdgeDataFactory
      .createDefaultEdgeWithLabel(7L, LABEL_HAS_INTEREST, eve.getId(),
        tagDatabases.getId());
    edges.add(edge7);
    // Person:Alice-[hasInterest]->Tag:Databases (8L)
    edge8 = EdgeDataFactory
      .createDefaultEdgeWithLabel(8L, LABEL_HAS_INTEREST, alice.getId(),
        tagDatabases.getId());
    edges.add(edge8);
    // Person:Dave-[hasInterest]->Tag:Hadoop (9L)
    edge9 = EdgeDataFactory
      .createDefaultEdgeWithLabel(9L, LABEL_HAS_INTEREST, dave.getId(),
        tagHadoop.getId());
    edges.add(edge9);
    // Person:Frank-[hasInterest]->Tag:Hadoop (10L)
    edge10 = EdgeDataFactory
      .createDefaultEdgeWithLabel(10L, LABEL_HAS_INTEREST, frank.getId(),
        tagHadoop.getId());
    edges.add(edge10);
    // Forum:Graph Databases-[hasTag]->Tag:Databases (11L)
    edge11 = EdgeDataFactory
      .createDefaultEdgeWithLabel(11L, LABEL_HAS_TAG, forumGDBS.getId(),
        tagDatabases.getId());
    edges.add(edge11);
    // Forum:Graph Databases-[hasTag]->Tag:Graphs (12L)
    edge12 = EdgeDataFactory
      .createDefaultEdgeWithLabel(12L, LABEL_HAS_TAG, forumGDBS.getId(),
        tagGraphs.getId());
    edges.add(edge12);
    // Forum:Graph Processing-[hasTag]->Tag:Graphs (13L)
    edge13 = EdgeDataFactory
      .createDefaultEdgeWithLabel(13L, LABEL_HAS_TAG, forumGPS.getId(),
        tagGraphs.getId());
    edges.add(edge13);
    // Forum:Graph Processing-[hasTag]->Tag:Hadoop (14L)
    edge14 = EdgeDataFactory
      .createDefaultEdgeWithLabel(14L, LABEL_HAS_TAG, forumGPS.getId(),
        tagHadoop.getId());
    edges.add(edge14);
    // Forum:Graph Databases-[hasModerator]->Person:Alice (15L)
    edge15 = EdgeDataFactory
      .createDefaultEdgeWithLabel(15L, LABEL_HAS_MODERATOR, forumGDBS.getId(),
        alice.getId());
    edges.add(edge15);
    // Forum:Graph Processing-[hasModerator]->Person:Dave (16L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge16 = EdgeDataFactory
      .createDefaultEdge(16L, LABEL_HAS_MODERATOR, forumGPS.getId(),
        dave.getId(), properties, Sets.newHashSet(3L));
    edges.add(edge16);
    // Forum:Graph Databases-[hasMember]->Person:Alice (17L)
    edge17 = EdgeDataFactory
      .createDefaultEdgeWithLabel(17L, LABEL_HAS_MEMBER, forumGDBS.getId(),
        alice.getId());
    edges.add(edge17);
    // Forum:Graph Databases-[hasMember]->Person:Bob (18L)
    edge18 = EdgeDataFactory
      .createDefaultEdgeWithLabel(18L, LABEL_HAS_MEMBER, forumGDBS.getId(),
        bob.getId());
    edges.add(edge18);
    // Forum:Graph Processing-[hasMember]->Person:Carol (19L)
    edge19 = EdgeDataFactory
      .createDefaultEdgeWithGraphs(19L, LABEL_HAS_MEMBER, forumGPS.getId(),
        carol.getId(), Sets.newHashSet(3L));
    edges.add(edge19);
    // Forum:Graph Processing-[hasMember]->Person:Dave (20L)
    edge20 = EdgeDataFactory
      .createDefaultEdgeWithGraphs(20L, LABEL_HAS_MEMBER, forumGPS.getId(),
        dave.getId(), Sets.newHashSet(3L));
    edges.add(edge20);

    // graphs
    List<GraphData> graphs = Lists.newArrayList();
    // Community {interest: Databases, vertexCount: 3} (0L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Databases");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 3);
    graphs.add(GraphDataFactory
      .createDefaultGraphWithIDAndLabelAndProperties(0L, LABEL_COMMUNITY,
        properties));
    // Community {interest: Hadoop, vertexCount: 3} (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Hadoop");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 3);
    graphs.add(GraphDataFactory
      .createDefaultGraphWithIDAndLabelAndProperties(1L, LABEL_COMMUNITY,
        properties));
    // Community {interest: Graphs, vertexCount: 4} (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Graphs");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 4);
    graphs.add(GraphDataFactory
      .createDefaultGraphWithIDAndLabelAndProperties(2L, LABEL_COMMUNITY,
        properties));

    // Forum {} (3L)
    graphs
      .add(GraphDataFactory.createDefaultGraphWithIDAndLabel(3L, LABEL_FORUM));

    return FlinkGraphStore.fromCollection(vertices, edges, graphs, env);
  }

  /**
   * Creates a list of long ids from a given string (e.g. "0 1 2 3")
   *
   * @param graphIDString e.g. "0 1 2 3"
   * @return List with long values
   */
  protected List<Long> extractGraphIDs(String graphIDString) {
    String[] tokens = graphIDString.split(" ");
    List<Long> graphIDs = Lists.newArrayListWithCapacity(tokens.length);
    for (String token : tokens) {
      graphIDs.add(Long.parseLong(token));
    }
    return graphIDs;
  }
}
