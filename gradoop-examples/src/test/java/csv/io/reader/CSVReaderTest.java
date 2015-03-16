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

/**
 * Tests for {@link org.gradoop.csv.io.reader.CSVReader}.
 */
public class CSVReaderTest extends GradoopClusterTest {
  Logger Log = Logger.getLogger(CSVReaderTest.class);
  private final String NODE_META =
    getClass().getResource("/node_meta.csv").getPath();

  private final String EDGE_META =
    getClass().getResource("/edge_meta.csv").getPath();

  private static final long lgid = 0;
  private static final String label_node = "Person";
  private static final String label_edge = "knows";
  private static final String firstName = "firstName";
  private static final String firstName0 = "Arun";
  private static final String lastName = "lastName";
  private static final String lastName0 = "Reddy";

  private static final String[] PERSON_CSV = new String[] {
    "id|firstName|lastName|gender|birthday|creationDate|locationIP" +
      "|browserUsed|",
    "0|Arun|Reddy|female|1987-05-27|2010-03-22T12:42:14.255+0000|59.185.111" +
      ".183|Firefox|",
    "1|Yang|Li|male|1984-07-09|2010-03-07T13:58:27.400+0000|14.192.76" +
      ".41|InternetExplorer|"
  };

  private static final String[] KNOWS_CSV = new String[] {
    "Person.id|Person.id|creationDate|",
    "0|1|2011-11-30T05:58:45.255+0000|",
    "1|0|2012-04-18T01:53:12.825+0000|"
  };


  @Test
  public void checkNodeCSVInputTest() {
    for (Vertex v : createVerticesFromCSV()) {
      long id = v.getID();
      if (id == lgid) {
        checkLabel(v, label_node);
        for (String propertyKey : v.getPropertyKeys()) {
          if (propertyKey.equals(firstName)) {
            String value = (String) v.getProperty(propertyKey);
            assertEquals(value, firstName0);
          } else if (propertyKey.equals(lastName)) {
            String value = (String) v.getProperty(lastName);
            assertEquals(value, lastName0);
          }
        }
      }
    }
  }

  @Test
  public void checkEdgeCSVInputTest() {
    for (Vertex v : createEdgesFromCSV()) {
      Log.info("####ID: " + v.getID());
      Log.info("#####imcomingDegree: " + v.getIncomingDegree());
      Log.info("#####outgoingdegree: " + v.getOutgoingDegree());
    }
  }

  protected void checkLabel(Vertex vertex, String label) {
    for (String s : vertex.getLabels()) {
      assertEquals(s, label);
    }
  }

  protected void checkOutgoingEdge(List<Edge> edges) {
    for (Edge edge : edges) {
      assertEquals(edge.getOtherID(), "1");
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
      for (Vertex v : reader.readVertexList(line)) {
        vertices.add(v);
      }
    }
    return vertices;
  }

  private List<Vertex> createEdgesFromCSV() {
    ConfigurableVertexLineReader reader = new CSVReader();
    Configuration conf = new Configuration();
    conf.set(CSVReader.META_DATA, EDGE_META);
    conf.set(CSVReader.LABEL, "knows");
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
