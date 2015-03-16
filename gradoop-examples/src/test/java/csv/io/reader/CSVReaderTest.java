package csv.io.reader;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.GradoopClusterTest;
import org.gradoop.csv.io.reader.CSVReader;
import org.gradoop.io.reader.ConfigurableVertexLineReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link org.gradoop.csv.io.reader.CSVReader}.
 */
public class CSVReaderTest{
  private final String NODE_META =
    getClass().getResource("/node_meta.csv").getPath();
  private final String EDGE_META =
    getClass().getResource("/edge_meta.csv").getPath();
  private static final String label_node = "Person";
  private static final String firstNameProperty = "firstName";
  private static final String lastNameProperty = "lastName";
  private static final String genderProperty = "gender";
  private static final String birthdayProperty = "birthday";
  private static final long person0 = 0;
  private static final String firstNameValue0 = "Arun";
  private static final String lastNameValue0 = "Reddy";
  private static final String genderValue0 = "female";
  private static final String birthdayValue0 = "1987-05-27";
  private static final long person1 = 1;
  private static final String firstNameValue1 = "Yang";
  private static final String lastNameValue1 = "Li";
  private static final String genderValue1 = "male";
  private static final String birthdayValue1 = "1984-07-09";
  private static final String edgeLabel = "workedAt";
  private static final long person2 = 2;
  private static final long person3 = 3;
  private static final long person4 = 4;
  private static final long person5 = 5;
  private static final String[] PERSON_CSV =
    new String[]{"id|firstName|lastName|gender|birthday|",
                 "0|Arun|Reddy|female|1987-05-27|",
                 "1|Yang|Li|male|1984-07-09|"};
  private static final String[] KNOWS_CSV =
    new String[]{"Person.id|Organisation.id|since|", "2|3|2015|", "4|5|2012|"};

  @Test
  public void checkNodeCSVInputTest() {
    List<Vertex> vlist = createVerticesFromCSV();
    assertTrue(vlist.size() == 2);
    for (Vertex v : vlist) {
      long id = v.getID();
      if (id == person0) {
        checkLabel(v, label_node);
        for (String propertyKey : v.getPropertyKeys()) {
          if (propertyKey.equals(firstNameProperty)) {
            String value = (String) v.getProperty(propertyKey);
            assertEquals(value, firstNameValue0);
          } else if (propertyKey.equals(lastNameProperty)) {
            String value = (String) v.getProperty(lastNameProperty);
            assertEquals(value, lastNameValue0);
          } else if (propertyKey.equals(genderProperty)) {
            String value = (String) v.getProperty(genderProperty);
            assertEquals(value, genderValue0);
          } else if (propertyKey.equals(birthdayProperty)) {
            String value = (String) v.getProperty(birthdayProperty);
            assertEquals(value, birthdayValue0);
          }
        }
      } else if (id == person1) {
        checkLabel(v, label_node);
        for (String propertyKey : v.getPropertyKeys()) {
          if (propertyKey.equals(firstNameProperty)) {
            String value = (String) v.getProperty(propertyKey);
            assertEquals(value, firstNameValue1);
          } else if (propertyKey.equals(lastNameProperty)) {
            String value = (String) v.getProperty(lastNameProperty);
            assertEquals(value, lastNameValue1);
          } else if (propertyKey.equals(genderProperty)) {
            String value = (String) v.getProperty(genderProperty);
            assertEquals(value, genderValue1);
          } else if (propertyKey.equals(birthdayProperty)) {
            String value = (String) v.getProperty(birthdayProperty);
            assertEquals(value, birthdayValue1);
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
    for (String s : vertex.getLabels()) {
      assertEquals(s, label);
    }
  }

  protected void checkOutgoingEdge(List<Edge> edges) {
    for (Edge edge : edges) {
      assertEquals(edge.getLabel(), edgeLabel);
    }
  }

  protected void checkIncomingEdge(List<Edge> edges) {
    for (Edge edge : edges) {
      assertEquals(edge.getLabel(), edgeLabel);
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
