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

package org.gradoop;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultEdgeDataFactory;
import org.gradoop.model.impl.DefaultGraphData;
import org.gradoop.model.impl.DefaultGraphDataFactory;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.DefaultVertexDataFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class GradoopTest {

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

  protected DefaultVertexData alice;
  protected DefaultVertexData bob;
  protected DefaultVertexData carol;
  protected DefaultVertexData dave;
  protected DefaultVertexData eve;
  protected DefaultVertexData frank;
  protected DefaultVertexData tagDatabases;
  protected DefaultVertexData tagGraphs;
  protected DefaultVertexData tagHadoop;
  protected DefaultVertexData forumGDBS;
  protected DefaultVertexData forumGPS;

  protected DefaultEdgeData edge0;
  protected DefaultEdgeData edge1;
  protected DefaultEdgeData edge2;
  protected DefaultEdgeData edge3;
  protected DefaultEdgeData edge4;
  protected DefaultEdgeData edge5;
  protected DefaultEdgeData edge6;
  protected DefaultEdgeData edge7;
  protected DefaultEdgeData edge8;
  protected DefaultEdgeData edge9;
  protected DefaultEdgeData edge10;
  protected DefaultEdgeData edge11;
  protected DefaultEdgeData edge12;
  protected DefaultEdgeData edge13;
  protected DefaultEdgeData edge14;
  protected DefaultEdgeData edge15;
  protected DefaultEdgeData edge16;
  protected DefaultEdgeData edge17;
  protected DefaultEdgeData edge18;
  protected DefaultEdgeData edge19;
  protected DefaultEdgeData edge20;
  protected DefaultEdgeData edge21;
  protected DefaultEdgeData edge22;
  protected DefaultEdgeData edge23;

  protected DefaultGraphData communityDatabases;
  protected DefaultGraphData communityHadoop;
  protected DefaultGraphData communityGraphs;
  protected DefaultGraphData forumGraph;

  public GradoopTest() {
    createVertexDataCollection();
    createEdgeDataCollection();
    createGraphDataCollection();
  }

  protected Collection<DefaultVertexData> createVertexDataCollection() {
    // vertices
    DefaultVertexDataFactory vertexDataFactory = new DefaultVertexDataFactory();
    // Person:Alice (0L)
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Alice");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Leipzig");
    alice = vertexDataFactory
      .createVertexData(0L, LABEL_PERSON, properties, Sets.newHashSet(0L, 2L));
    // Person:Bob (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Bob");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Leipzig");
    bob = vertexDataFactory
      .createVertexData(1L, LABEL_PERSON, properties, Sets.newHashSet(0L, 2L));
    // Person:Carol (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Carol");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    carol = vertexDataFactory.createVertexData(2L, LABEL_PERSON, properties,
      Sets.newHashSet(1L, 2L, 3L));
    // Person:Dave (3L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Dave");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    dave = vertexDataFactory.createVertexData(3L, LABEL_PERSON, properties,
      Sets.newHashSet(1L, 2L, 3L));
    // Person:Eve (4L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Eve");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    properties.put(PROPERTY_KEY_SPEAKS, "English");
    eve = vertexDataFactory
      .createVertexData(4L, LABEL_PERSON, properties, Sets.newHashSet(0L));
    // Person:Frank (5L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Frank");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Berlin");
    properties.put(PROPERTY_KEY_LOC_IP, "127.0.0.1");
    frank = vertexDataFactory
      .createVertexData(5L, LABEL_PERSON, properties, Sets.newHashSet(1L));

    // Tag:Databases (6L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Databases");
    tagDatabases =
      vertexDataFactory.createVertexData(6L, LABEL_TAG, properties);
    // Tag:Databases (7L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Graphs");
    tagGraphs = vertexDataFactory.createVertexData(7L, LABEL_TAG, properties);
    // Tag:Databases (8L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Hadoop");
    tagHadoop = vertexDataFactory.createVertexData(8L, LABEL_TAG, properties);

    // Forum:Graph Databases (9L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_TITLE, "Graph Databases");
    forumGDBS = vertexDataFactory.createVertexData(9L, LABEL_FORUM, properties);
    // Forum:Graph Processing (10L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_TITLE, "Graph Processing");
    forumGPS = vertexDataFactory
      .createVertexData(10L, LABEL_FORUM, properties, Sets.newHashSet(3L));

    return Lists.newArrayList(alice, bob, carol, dave, eve, frank, tagDatabases,
      tagGraphs, tagHadoop, forumGDBS, forumGPS);
  }


  protected Collection<DefaultEdgeData> createEdgeDataCollection() {
    // sna_edges
    DefaultEdgeDataFactory edgeDataFactory = new DefaultEdgeDataFactory();

    List<DefaultEdgeData> edges = Lists.newArrayList();
    // Person:Alice-[knows]->Person:Bob (0L)
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge0 = edgeDataFactory
      .createEdgeData(0L, LABEL_KNOWS, alice.getId(), bob.getId(), properties,
        Sets.newHashSet(0L, 2L));
    edges.add(edge0);
    // Person:Bob-[knows]->Person:Alice (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge1 = edgeDataFactory
      .createEdgeData(1L, LABEL_KNOWS, bob.getId(), alice.getId(), properties,
        Sets.newHashSet(0L, 2L));
    edges.add(edge1);
    // Person:Bob-[knows]->Person:Carol (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge2 = edgeDataFactory
      .createEdgeData(2L, LABEL_KNOWS, bob.getId(), carol.getId(), properties,
        Sets.newHashSet(2L));
    edges.add(edge2);
    // Person:Carol-[knows]->Person:Bob (3L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge3 = edgeDataFactory
      .createEdgeData(3L, LABEL_KNOWS, carol.getId(), bob.getId(), properties,
        Sets.newHashSet(2L));
    edges.add(edge3);
    // Person:Carol-[knows]->Person:Dave (4L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge4 = edgeDataFactory
      .createEdgeData(4L, LABEL_KNOWS, carol.getId(), dave.getId(), properties,
        Sets.newHashSet(1L, 2L, 3L));
    edges.add(edge4);
    // Person:Dave-[knows]->Person:Carol (5L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    edge5 = edgeDataFactory
      .createEdgeData(5L, LABEL_KNOWS, dave.getId(), carol.getId(), properties,
        Sets.newHashSet(1L, 2L));
    edges.add(edge5);
    // Person:Eve-[knows]->Person:Alice (6L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge6 = edgeDataFactory
      .createEdgeData(6L, LABEL_KNOWS, eve.getId(), alice.getId(), properties,
        Sets.newHashSet(0L));
    edges.add(edge6);
    // Person:Eve-[knows]->Person:Bob (21L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    edge21 = edgeDataFactory
      .createEdgeData(21L, LABEL_KNOWS, eve.getId(), bob.getId(), properties,
        Sets.newHashSet(0L));
    edges.add(edge21);
    // Person:Frank-[knows]->Person:Carol (22L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    edge22 = edgeDataFactory
      .createEdgeData(22L, LABEL_KNOWS, frank.getId(), carol.getId(),
        properties, Sets.newHashSet(1L));
    edges.add(edge22);
    // Person:Frank-[knows]->Person:Dave (23L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    edge23 = edgeDataFactory
      .createEdgeData(23L, LABEL_KNOWS, frank.getId(), dave.getId(), properties,
        Sets.newHashSet(1L));
    edges.add(edge23);
    // Person:Eve-[hasInterest]->Tag:Databases (7L)
    edge7 = edgeDataFactory.createEdgeData(7L, LABEL_HAS_INTEREST, eve.getId(),
      tagDatabases.getId());
    edges.add(edge7);
    // Person:Alice-[hasInterest]->Tag:Databases (8L)
    edge8 = edgeDataFactory
      .createEdgeData(8L, LABEL_HAS_INTEREST, alice.getId(),
        tagDatabases.getId());
    edges.add(edge8);
    // Person:Dave-[hasInterest]->Tag:Hadoop (9L)
    edge9 = edgeDataFactory
      .createEdgeData(9L, LABEL_HAS_INTEREST, dave.getId(), tagHadoop.getId());
    edges.add(edge9);
    // Person:Frank-[hasInterest]->Tag:Hadoop (10L)
    edge10 = edgeDataFactory
      .createEdgeData(10L, LABEL_HAS_INTEREST, frank.getId(),
        tagHadoop.getId());
    edges.add(edge10);
    // Forum:Graph Databases-[hasTag]->Tag:Databases (11L)
    edge11 = edgeDataFactory
      .createEdgeData(11L, LABEL_HAS_TAG, forumGDBS.getId(),
        tagDatabases.getId());
    edges.add(edge11);
    // Forum:Graph Databases-[hasTag]->Tag:Graphs (12L)
    edge12 = edgeDataFactory
      .createEdgeData(12L, LABEL_HAS_TAG, forumGDBS.getId(), tagGraphs.getId());
    edges.add(edge12);
    // Forum:Graph Processing-[hasTag]->Tag:Graphs (13L)
    edge13 = edgeDataFactory
      .createEdgeData(13L, LABEL_HAS_TAG, forumGPS.getId(), tagGraphs.getId());
    edges.add(edge13);
    // Forum:Graph Processing-[hasTag]->Tag:Hadoop (14L)
    edge14 = edgeDataFactory
      .createEdgeData(14L, LABEL_HAS_TAG, forumGPS.getId(), tagHadoop.getId());
    edges.add(edge14);
    // Forum:Graph Databases-[hasModerator]->Person:Alice (15L)
    edge15 = edgeDataFactory
      .createEdgeData(15L, LABEL_HAS_MODERATOR, forumGDBS.getId(),
        alice.getId());
    edges.add(edge15);
    // Forum:Graph Processing-[hasModerator]->Person:Dave (16L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    edge16 = edgeDataFactory
      .createEdgeData(16L, LABEL_HAS_MODERATOR, forumGPS.getId(), dave.getId(),
        properties, Sets.newHashSet(3L));
    edges.add(edge16);
    // Forum:Graph Databases-[hasMember]->Person:Alice (17L)
    edge17 = edgeDataFactory
      .createEdgeData(17L, LABEL_HAS_MEMBER, forumGDBS.getId(), alice.getId());
    edges.add(edge17);
    // Forum:Graph Databases-[hasMember]->Person:Bob (18L)
    edge18 = edgeDataFactory
      .createEdgeData(18L, LABEL_HAS_MEMBER, forumGDBS.getId(), bob.getId());
    edges.add(edge18);
    // Forum:Graph Processing-[hasMember]->Person:Carol (19L)
    edge19 = edgeDataFactory
      .createEdgeData(19L, LABEL_HAS_MEMBER, forumGPS.getId(), carol.getId(),
        Sets.newHashSet(3L));
    edges.add(edge19);
    // Forum:Graph Processing-[hasMember]->Person:Dave (20L)
    edge20 = edgeDataFactory
      .createEdgeData(20L, LABEL_HAS_MEMBER, forumGPS.getId(), dave.getId(),
        Sets.newHashSet(3L));
    edges.add(edge20);

    return edges;
  }

  protected Collection<DefaultGraphData> createGraphDataCollection() {
    // graphs
    DefaultGraphDataFactory graphDataFactory = new DefaultGraphDataFactory();
    List<DefaultGraphData> graphs = Lists.newArrayList();
    // Community {interest: Databases, vertexCount: 3} (0L)
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Databases");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 3);
    communityDatabases =
      graphDataFactory.createGraphData(0L, LABEL_COMMUNITY, properties);
    graphs.add(communityDatabases);
    // Community {interest: Hadoop, vertexCount: 3} (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Hadoop");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 3);
    communityHadoop =
      graphDataFactory.createGraphData(1L, LABEL_COMMUNITY, properties);
    graphs.add(communityHadoop);
    // Community {interest: Graphs, vertexCount: 4} (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Graphs");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 4);
    communityGraphs =
      graphDataFactory.createGraphData(2L, LABEL_COMMUNITY, properties);
    graphs.add(communityGraphs);
    // Forum {} (3L)
    forumGraph = graphDataFactory.createGraphData(3L, LABEL_FORUM);
    graphs.add(forumGraph);

    return graphs;
  }
}
