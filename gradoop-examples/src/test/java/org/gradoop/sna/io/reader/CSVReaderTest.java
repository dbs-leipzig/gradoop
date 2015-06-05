package org.gradoop.sna.io.reader;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.io.reader.ConfigurableVertexLineReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CSVReader}.
 */
public class CSVReaderTest {
  Logger LOG = Logger.getLogger(CSVReaderTest.class);
  // Meta Strings
  private final String NODE_META = "long|string|string|string|string|";
  private final String EDGE_META = "long|long|long|string|string|";
  // node test values, properties and labels
  private static final String LABEL_NODE = "Person";
  private static final String FIRSTNAMEPROPERTY = "firstName";
  private static final String LASTNAMEPROPERTY = "lastName";
  private static final String GENDERPROPERTY = "gender";
  private static final String BIRTHDAYPROPERTY = "birthday";
  private static final long person0 = 0;
  private static final String FIRSTNAMEVALUE0 = "Arun";
  private static final String LASTNAMEVALUE0 = "Reddy";
  private static final String GENDERVALUE0 = "female";
  private static final String BIRTHDAYVALUE0 = "1987-05-27";
  private static final long person1 = 1;
  private static final String FIRSTNAMEVALUE1 = "Yang";
  private static final String LASTNAMEVALUE1 = "Li";
  private static final String GENDERVALUE1 = "male";
  private static final String BIRTHDAYVALUE1 = "1984-07-09";
  // edge test label and id's
  private static final String EDGELABEL = "workedAt";
  private static final long person2 = 2;
  private static final long person3 = 3;
  private static final long person4 = 4;
  private static final long person5 = 5;
  // Test files
  private static final String[] PERSON_CSV =
    new String[]{"id|firstName|lastName|gender|birthday|",
                 "0|Arun|Reddy|female|1987-05-27|",
                 "1|Yang|Li|male|1984-07-09|"};
  private static final String[] KNOWS_CSV =
    new String[]{"Person.id|Organisation.id|since|office|department|",
                 "2|3|2015|P414|visual-studios|", "4|5|2012|P416|databases|"};

  @Test
  public void checkNodeCSVInputTest() {
    List<Vertex> vlist = createVerticesFromCSV();
    assertTrue(vlist.size() == 2);
    for (Vertex v : vlist) {
      long id = v.getID();
      if (id == person0) {
        checkLabel(v, LABEL_NODE);
        for (String propertyKey : v.getPropertyKeys()) {
          switch (propertyKey) {
          case FIRSTNAMEPROPERTY: {
            String value = (String) v.getProperty(propertyKey);
            assertEquals(value, FIRSTNAMEVALUE0);
            break;
          }
          case LASTNAMEPROPERTY: {
            String value = (String) v.getProperty(LASTNAMEPROPERTY);
            assertEquals(value, LASTNAMEVALUE0);
            break;
          }
          case GENDERPROPERTY: {
            String value = (String) v.getProperty(GENDERPROPERTY);
            assertEquals(value, GENDERVALUE0);
            break;
          }
          case BIRTHDAYPROPERTY: {
            String value = (String) v.getProperty(BIRTHDAYPROPERTY);
            assertEquals(value, BIRTHDAYVALUE0);
            break;
          }
          }
        }
      } else if (id == person1) {
        checkLabel(v, LABEL_NODE);
        for (String propertyKey : v.getPropertyKeys()) {
          switch (propertyKey) {
          case FIRSTNAMEPROPERTY: {
            String value = (String) v.getProperty(propertyKey);
            assertEquals(value, FIRSTNAMEVALUE1);
            break;
          }
          case LASTNAMEPROPERTY: {
            String value = (String) v.getProperty(LASTNAMEPROPERTY);
            assertEquals(value, LASTNAMEVALUE1);
            break;
          }
          case GENDERPROPERTY: {
            String value = (String) v.getProperty(GENDERPROPERTY);
            assertEquals(value, GENDERVALUE1);
            break;
          }
          case BIRTHDAYPROPERTY: {
            String value = (String) v.getProperty(BIRTHDAYPROPERTY);
            assertEquals(value, BIRTHDAYVALUE1);
            break;
          }
          }
        }
      }
    }
  }

  @Test
  public void checkEdgeCSVInputTest() {
    List<Vertex> vlist = createEdgesFromCSV();
    for (Vertex v : vlist) {
      long id = v.getID();
      assertEquals(vlist.size(), 4);
      if (id == person2) {
        assertEquals(v.getOutgoingDegree(), 1);
        assertEquals(v.getIncomingDegree(), 0);
        checkOutgoingEdge(Lists.newArrayList(v.getOutgoingEdges()));
      } else if (id == person3) {
        assertEquals(v.getIncomingDegree(), 1);
        assertEquals(v.getOutgoingDegree(), 0);
        checkIncomingEdge(Lists.newArrayList(v.getIncomingEdges()));
      } else if (id == person4) {
        assertEquals(v.getIncomingDegree(), 0);
        assertEquals(v.getOutgoingDegree(), 1);
        checkOutgoingEdge(Lists.newArrayList(v.getOutgoingEdges()));
      } else if (id == person5) {
        assertEquals(v.getIncomingDegree(), 1);
        assertEquals(v.getOutgoingDegree(), 0);
        checkIncomingEdge(Lists.newArrayList(v.getIncomingEdges()));
      }
    }
  }

  protected void checkLabel(Vertex vertex, String label) {
    assertEquals(label, vertex.getLabel());
  }

  protected void checkOutgoingEdge(List<Edge> edges) {
    for (Edge edge : edges) {
      assertEquals(edge.getLabel(), EDGELABEL);
      LOG.info(edge.getPropertyKeys());
    }
  }

  protected void checkIncomingEdge(List<Edge> edges) {
    for (Edge edge : edges) {
      assertEquals(edge.getLabel(), EDGELABEL);
      LOG.info(edge.getPropertyKeys());
    }
  }

  private List<Vertex> createVerticesFromCSV() {
    ConfigurableVertexLineReader reader = new CSVReader();
    Configuration conf = new Configuration();
    conf.set(CSVReader.META_DATA, NODE_META);
    conf.set(CSVReader.LABEL, "Person");
    conf.set(CSVReader.TYPE, "node");
    reader.setConf(conf);
    List<Vertex> vertices = Lists.newArrayList();
    for (String line : PERSON_CSV) {
      List<Vertex> verticesPerLine = reader.readVertexList(line);
      for (Vertex v : verticesPerLine) {
        vertices.add(v);
      }
    }
    return vertices;
  }

  private List<Vertex> createEdgesFromCSV() {
    ConfigurableVertexLineReader reader = new CSVReader();
    Configuration conf = new Configuration();
    conf.set(CSVReader.META_DATA, EDGE_META);
    conf.set(CSVReader.LABEL, "workedAt");
    conf.set(CSVReader.TYPE, "edge");
    reader.setConf(conf);
    List<Vertex> vertices = Lists.newArrayList();
    for (String line : KNOWS_CSV) {
      for (Vertex v : reader.readVertexList(line)) {
        vertices.add(v);
      }
    }
    return vertices;
  }
}
