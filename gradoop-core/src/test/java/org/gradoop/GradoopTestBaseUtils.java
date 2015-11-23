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
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains method to create a test EPGM database. A visual representation of
 * the graph can be found in dev-support/social-network.pdf.
 */
public abstract class GradoopTestBaseUtils {

  public static final String LABEL_COMMUNITY = "Community";
  public static final String LABEL_PERSON = "Person";
  public static final String LABEL_FORUM = "Forum";
  public static final String LABEL_TAG = "Tag";
  public static final String LABEL_KNOWS = "knows";
  public static final String LABEL_HAS_MODERATOR = "hasModerator";
  public static final String LABEL_HAS_MEMBER = "hasMember";
  public static final String LABEL_HAS_INTEREST = "hasInterest";
  public static final String LABEL_HAS_TAG = "hasTag";
  public static final String PROPERTY_KEY_NAME = "name";
  public static final String PROPERTY_KEY_GENDER = "gender";
  public static final String PROPERTY_KEY_CITY = "city";
  public static final String PROPERTY_KEY_SPEAKS = "speaks";
  public static final String PROPERTY_KEY_LOC_IP = "locIP";
  public static final String PROPERTY_KEY_TITLE = "title";
  public static final String PROPERTY_KEY_SINCE = "since";
  public static final String PROPERTY_KEY_INTEREST = "interest";
  public static final String PROPERTY_KEY_VERTEX_COUNT = "vertexCount";

  public static VertexPojo VERTEX_PERSON_ALICE;
  public static VertexPojo VERTEX_PERSON_BOB;
  public static VertexPojo VERTEX_PERSON_CAROL;
  public static VertexPojo VERTEX_PERSON_DAVE;
  public static VertexPojo VERTEX_PERSON_EVE;
  public static VertexPojo VERTEX_PERSON_FRANK;
  public static VertexPojo VERTEX_TAG_DATABASES;
  public static VertexPojo VERTEX_TAG_GRAPHS;
  public static VertexPojo VERTEX_TAG_HADOOP;
  public static VertexPojo VERTEX_FORUM_GDBS;
  public static VertexPojo VERTEX_FORUM_GPS;

  public static EdgePojo EDGE_0_KNOWS;
  public static EdgePojo EDGE_1_KNOWS;
  public static EdgePojo EDGE_2_KNOWS;
  public static EdgePojo EDGE_3_KNOWS;
  public static EdgePojo EDGE_4_KNOWS;
  public static EdgePojo EDGE_5_KNOWS;
  public static EdgePojo EDGE_6_KNOWS;
  public static EdgePojo EDGE_7_HAS_INTEREST;
  public static EdgePojo EDGE_8_HAS_INTEREST;
  public static EdgePojo EDGE_9_HAS_INTEREST;
  public static EdgePojo EDGE_10_HAS_INTEREST;
  public static EdgePojo EDGE_11_HAS_TAG;
  public static EdgePojo EDGE_12_HAS_TAG;
  public static EdgePojo EDGE_13_HAS_TAG;
  public static EdgePojo EDGE_14_HAS_TAG;
  public static EdgePojo EDGE_15_HAS_MODERATOR;
  public static EdgePojo EDGE_16_HAS_MODERATOR;
  public static EdgePojo EDGE_17_HAS_MEMBER;
  public static EdgePojo EDGE_18_HAS_MEMBER;
  public static EdgePojo EDGE_19_HAS_MEMBER;
  public static EdgePojo EDGE_20_HAS_MEMBER;
  public static EdgePojo EDGE_21_KNOWS;
  public static EdgePojo EDGE_22_KNOWS;
  public static EdgePojo EDGE_23_KNOWS;

  public static GraphHeadPojo communityDatabases;
  public static GraphHeadPojo communityHadoop;
  public static GraphHeadPojo communityGraphs;
  public static GraphHeadPojo forumGraph;

  static {
    createVertexPojoCollection();
    createEdgePojoCollection();
    createGraphHeadCollection();
  }

  public static Collection<VertexPojo> createVertexPojoCollection() {

    // vertices
    VertexPojoFactory vertexDataFactory = new VertexPojoFactory();
    // Person:Alice (0L)
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Alice");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Leipzig");
    VERTEX_PERSON_ALICE = vertexDataFactory
      .createVertex(GradoopId.fromLong(0L), LABEL_PERSON, properties,
        GradoopIdSet.fromLongs(0L, 2L));
    // Person:Bob (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Bob");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Leipzig");
    VERTEX_PERSON_BOB = vertexDataFactory
      .createVertex(GradoopId.fromLong(1L), LABEL_PERSON, properties,
        GradoopIdSet.fromLongs(0L, 2L));
    // Person:Carol (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Carol");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    VERTEX_PERSON_CAROL = vertexDataFactory
      .createVertex(GradoopId.fromLong(2L), LABEL_PERSON, properties,
        GradoopIdSet.fromLongs(1L, 2L, 3L));
    // Person:Dave (3L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Dave");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    VERTEX_PERSON_DAVE = vertexDataFactory
      .createVertex(GradoopId.fromLong(3L), LABEL_PERSON, properties,
        GradoopIdSet.fromLongs(1L, 2L, 3L));
    // Person:Eve (4L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Eve");
    properties.put(PROPERTY_KEY_GENDER, "f");
    properties.put(PROPERTY_KEY_CITY, "Dresden");
    properties.put(PROPERTY_KEY_SPEAKS, "English");
    VERTEX_PERSON_EVE = vertexDataFactory
      .createVertex(GradoopId.fromLong(4L), LABEL_PERSON, properties,
        GradoopIdSet.fromLongs(0L));
    // Person:Frank (5L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Frank");
    properties.put(PROPERTY_KEY_GENDER, "m");
    properties.put(PROPERTY_KEY_CITY, "Berlin");
    properties.put(PROPERTY_KEY_LOC_IP, "127.0.0.1");
    VERTEX_PERSON_FRANK = vertexDataFactory
      .createVertex(GradoopId.fromLong(5L), LABEL_PERSON, properties,
        GradoopIdSet.fromLongs(1L));

    // Tag:Databases (6L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Databases");
    VERTEX_TAG_DATABASES = vertexDataFactory
      .createVertex(GradoopId.fromLong(6L), LABEL_TAG, properties);
    // Tag:Databases (7L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Graphs");
    VERTEX_TAG_GRAPHS = vertexDataFactory
      .createVertex(GradoopId.fromLong(7L), LABEL_TAG, properties);
    // Tag:Databases (8L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_NAME, "Hadoop");
    VERTEX_TAG_HADOOP = vertexDataFactory
      .createVertex(GradoopId.fromLong(8L), LABEL_TAG, properties);

    // Forum:Graph Databases (9L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_TITLE, "Graph Databases");
    VERTEX_FORUM_GDBS = vertexDataFactory
      .createVertex(GradoopId.fromLong(9L), LABEL_FORUM, properties);
    // Forum:Graph Processing (10L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_TITLE, "Graph Processing");
    VERTEX_FORUM_GPS = vertexDataFactory
      .createVertex(GradoopId.fromLong(10L), LABEL_FORUM, properties,
        GradoopIdSet.fromLongs(3L));

    return Lists
      .newArrayList(VERTEX_PERSON_ALICE, VERTEX_PERSON_BOB, VERTEX_PERSON_CAROL,
        VERTEX_PERSON_DAVE, VERTEX_PERSON_EVE, VERTEX_PERSON_FRANK,
        VERTEX_TAG_DATABASES, VERTEX_TAG_GRAPHS, VERTEX_TAG_HADOOP,
        VERTEX_FORUM_GDBS, VERTEX_FORUM_GPS);
  }


  public static Collection<EdgePojo> createEdgePojoCollection() {
    // sna_edges
    EdgePojoFactory edgeDataFactory = new EdgePojoFactory();

    List<EdgePojo> edges = Lists.newArrayList();
    // Person:Alice-[knows]->Person:Bob (0L)
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    EDGE_0_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(0L), LABEL_KNOWS,
        VERTEX_PERSON_ALICE.getId(), VERTEX_PERSON_BOB.getId(), properties,
        GradoopIdSet.fromLongs(0L, 2L));
    edges.add(EDGE_0_KNOWS);
    // Person:Bob-[knows]->Person:Alice (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    EDGE_1_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(1L), LABEL_KNOWS,
        VERTEX_PERSON_BOB.getId(), VERTEX_PERSON_ALICE.getId(), properties,
        GradoopIdSet.fromLongs(0L, 2L));
    edges.add(EDGE_1_KNOWS);
    // Person:Bob-[knows]->Person:Carol (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    EDGE_2_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(2L), LABEL_KNOWS,
        VERTEX_PERSON_BOB.getId(), VERTEX_PERSON_CAROL.getId(), properties,
        GradoopIdSet.fromLongs(2L));
    edges.add(EDGE_2_KNOWS);
    // Person:Carol-[knows]->Person:Bob (3L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    EDGE_3_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(3L), LABEL_KNOWS,
        VERTEX_PERSON_CAROL.getId(), VERTEX_PERSON_BOB.getId(), properties,
        GradoopIdSet.fromLongs(2L));
    edges.add(EDGE_3_KNOWS);
    // Person:Carol-[knows]->Person:Dave (4L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    EDGE_4_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(4L), LABEL_KNOWS,
        VERTEX_PERSON_CAROL.getId(), VERTEX_PERSON_DAVE.getId(), properties,
        GradoopIdSet.fromLongs(1L, 2L, 3L));
    edges.add(EDGE_4_KNOWS);
    // Person:Dave-[knows]->Person:Carol (5L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2014);
    EDGE_5_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(5L), LABEL_KNOWS,
        VERTEX_PERSON_DAVE.getId(), VERTEX_PERSON_CAROL.getId(), properties,
        GradoopIdSet.fromLongs(1L, 2L));
    edges.add(EDGE_5_KNOWS);
    // Person:Eve-[knows]->Person:Alice (6L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    EDGE_6_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(6L), LABEL_KNOWS,
        VERTEX_PERSON_EVE.getId(), VERTEX_PERSON_ALICE.getId(), properties,
        GradoopIdSet.fromLongs(0L));
    edges.add(EDGE_6_KNOWS);
    // Person:Eve-[knows]->Person:Bob (21L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    EDGE_21_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(21L), LABEL_KNOWS,
        VERTEX_PERSON_EVE.getId(), VERTEX_PERSON_BOB.getId(), properties,
        GradoopIdSet.fromLongs(0L));
    edges.add(EDGE_21_KNOWS);
    // Person:Frank-[knows]->Person:Carol (22L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    EDGE_22_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(22L), LABEL_KNOWS,
        VERTEX_PERSON_FRANK.getId(), VERTEX_PERSON_CAROL.getId(), properties,
        GradoopIdSet.fromLongs(1L));
    edges.add(EDGE_22_KNOWS);
    // Person:Frank-[knows]->Person:Dave (23L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2015);
    EDGE_23_KNOWS = edgeDataFactory
      .createEdge(GradoopId.fromLong(23L), LABEL_KNOWS,
        VERTEX_PERSON_FRANK.getId(), VERTEX_PERSON_DAVE.getId(), properties,
        GradoopIdSet.fromLongs(1L));
    edges.add(EDGE_23_KNOWS);
    // Person:Eve-[hasInterest]->Tag:Databases (7L)
    EDGE_7_HAS_INTEREST = edgeDataFactory
      .createEdge(GradoopId.fromLong(7L), LABEL_HAS_INTEREST,
        VERTEX_PERSON_EVE.getId(), VERTEX_TAG_DATABASES.getId());
    edges.add(EDGE_7_HAS_INTEREST);
    // Person:Alice-[hasInterest]->Tag:Databases (8L)
    EDGE_8_HAS_INTEREST = edgeDataFactory
      .createEdge(GradoopId.fromLong(8L), LABEL_HAS_INTEREST,
        VERTEX_PERSON_ALICE.getId(), VERTEX_TAG_DATABASES.getId());
    edges.add(EDGE_8_HAS_INTEREST);
    // Person:Dave-[hasInterest]->Tag:Hadoop (9L)
    EDGE_9_HAS_INTEREST = edgeDataFactory
      .createEdge(GradoopId.fromLong(9L), LABEL_HAS_INTEREST,
        VERTEX_PERSON_DAVE.getId(), VERTEX_TAG_HADOOP.getId());
    edges.add(EDGE_9_HAS_INTEREST);
    // Person:Frank-[hasInterest]->Tag:Hadoop (10L)
    EDGE_10_HAS_INTEREST = edgeDataFactory
      .createEdge(GradoopId.fromLong(10L), LABEL_HAS_INTEREST,
        VERTEX_PERSON_FRANK.getId(), VERTEX_TAG_HADOOP.getId());
    edges.add(EDGE_10_HAS_INTEREST);
    // Forum:Graph Databases-[hasTag]->Tag:Databases (11L)
    EDGE_11_HAS_TAG = edgeDataFactory
      .createEdge(GradoopId.fromLong(11L), LABEL_HAS_TAG,
        VERTEX_FORUM_GDBS.getId(), VERTEX_TAG_DATABASES.getId());
    edges.add(EDGE_11_HAS_TAG);
    // Forum:Graph Databases-[hasTag]->Tag:Graphs (12L)
    EDGE_12_HAS_TAG = edgeDataFactory
      .createEdge(GradoopId.fromLong(12L), LABEL_HAS_TAG,
        VERTEX_FORUM_GDBS.getId(), VERTEX_TAG_GRAPHS.getId());
    edges.add(EDGE_12_HAS_TAG);
    // Forum:Graph Processing-[hasTag]->Tag:Graphs (13L)
    EDGE_13_HAS_TAG = edgeDataFactory
      .createEdge(GradoopId.fromLong(13L), LABEL_HAS_TAG,
        VERTEX_FORUM_GPS.getId(), VERTEX_TAG_GRAPHS.getId());
    edges.add(EDGE_13_HAS_TAG);
    // Forum:Graph Processing-[hasTag]->Tag:Hadoop (14L)
    EDGE_14_HAS_TAG = edgeDataFactory
      .createEdge(GradoopId.fromLong(14L), LABEL_HAS_TAG,
        VERTEX_FORUM_GPS.getId(), VERTEX_TAG_HADOOP.getId());
    edges.add(EDGE_14_HAS_TAG);
    // Forum:Graph Databases-[hasModerator]->Person:Alice (15L)
    EDGE_15_HAS_MODERATOR = edgeDataFactory
      .createEdge(GradoopId.fromLong(15L), LABEL_HAS_MODERATOR,
        VERTEX_FORUM_GDBS.getId(), VERTEX_PERSON_ALICE.getId());
    edges.add(EDGE_15_HAS_MODERATOR);
    // Forum:Graph Processing-[hasModerator]->Person:Dave (16L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_SINCE, 2013);
    EDGE_16_HAS_MODERATOR = edgeDataFactory
      .createEdge(GradoopId.fromLong(16L), LABEL_HAS_MODERATOR,
        VERTEX_FORUM_GPS.getId(), VERTEX_PERSON_DAVE.getId(), properties,
        GradoopIdSet.fromLongs(3L));
    edges.add(EDGE_16_HAS_MODERATOR);
    // Forum:Graph Databases-[hasMember]->Person:Alice (17L)
    EDGE_17_HAS_MEMBER = edgeDataFactory
      .createEdge(GradoopId.fromLong(17L), LABEL_HAS_MEMBER,
        VERTEX_FORUM_GDBS.getId(), VERTEX_PERSON_ALICE.getId());
    edges.add(EDGE_17_HAS_MEMBER);
    // Forum:Graph Databases-[hasMember]->Person:Bob (18L)
    EDGE_18_HAS_MEMBER = edgeDataFactory
      .createEdge(GradoopId.fromLong(18L), LABEL_HAS_MEMBER,
        VERTEX_FORUM_GDBS.getId(), VERTEX_PERSON_BOB.getId());
    edges.add(EDGE_18_HAS_MEMBER);
    // Forum:Graph Processing-[hasMember]->Person:Carol (19L)
    EDGE_19_HAS_MEMBER = edgeDataFactory
      .createEdge(GradoopId.fromLong(19L), LABEL_HAS_MEMBER,
        VERTEX_FORUM_GPS.getId(), VERTEX_PERSON_CAROL.getId(),
        GradoopIdSet.fromLongs(3L));
    edges.add(EDGE_19_HAS_MEMBER);
    // Forum:Graph Processing-[hasMember]->Person:Dave (20L)
    EDGE_20_HAS_MEMBER = edgeDataFactory
      .createEdge(GradoopId.fromLong(20L), LABEL_HAS_MEMBER,
        VERTEX_FORUM_GPS.getId(), VERTEX_PERSON_DAVE.getId(),
        GradoopIdSet.fromLongs(3L));
    edges.add(EDGE_20_HAS_MEMBER);

    return edges;
  }

  public static Collection<GraphHeadPojo> createGraphHeadCollection() {
    // graphs
    GraphHeadPojoFactory graphDataFactory = new GraphHeadPojoFactory();
    List<GraphHeadPojo> graphs = Lists.newArrayList();
    // Community {interest: Databases, vertexCount: 3} (0L)
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Databases");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 3);
    communityDatabases = graphDataFactory
      .createGraphHead(GradoopId.fromLong(0L), LABEL_COMMUNITY, properties);
    graphs.add(communityDatabases);
    // Community {interest: Hadoop, vertexCount: 3} (1L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Hadoop");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 3);
    communityHadoop = graphDataFactory
      .createGraphHead(GradoopId.fromLong(1L), LABEL_COMMUNITY, properties);
    graphs.add(communityHadoop);
    // Community {interest: Graphs, vertexCount: 4} (2L)
    properties = new HashMap<>();
    properties.put(PROPERTY_KEY_INTEREST, "Graphs");
    properties.put(PROPERTY_KEY_VERTEX_COUNT, 4);
    communityGraphs = graphDataFactory
      .createGraphHead(GradoopId.fromLong(2L), LABEL_COMMUNITY, properties);
    graphs.add(communityGraphs);
    // Forum {} (3L)
    forumGraph =
      graphDataFactory.createGraphHead(GradoopId.fromLong(3L), LABEL_FORUM);
    graphs.add(forumGraph);

    return graphs;
  }

  /**
   * Creates a list of long ids from a given string (e.g. "0 1 2 3")
   *
   * @param graphIDString e.g. "0 1 2 3"
   * @return List with long values
   */
  public static List<Long> extractGraphIDs(String graphIDString) {
    String[] tokens = graphIDString.split(" ");
    List<Long> graphIDs = Lists.newArrayListWithCapacity(tokens.length);
    for (String token : tokens) {
      graphIDs.add(Long.parseLong(token));
    }
    return graphIDs;
  }
}
