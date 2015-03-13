package csv.io.reader;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.GradoopClusterTest;
import org.gradoop.csv.io.reader.CSVReader;
import org.gradoop.io.reader.ConfigurableVertexLineReader;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Tests for {@link org.gradoop.csv.io.reader.CSVReader}.
 */
public class CSVReaderTest extends GradoopClusterTest {
  Logger Log = Logger.getLogger(CSVReaderTest.class);
  private final String NODE_META = getClass().getResource("/node_meta" +
    ".csv").getPath();

  private static final String[] PERSON_CSV = new String[]{
    "id|firstName|lastName|gender|birthday|creationDate|locationIP" +
      "|browserUsed|",
    "0|Arun|Reddy|female|1987-05-27|2010-03-22T12:42:14.255+0000|59.185.111" +
      ".183|Firefox|",
    "1|Yang|Li|male|1984-07-09|2010-03-07T13:58:27.400+0000|14.192.76" +
      ".41|InternetExplorer|"};

  @Test
  public void checkSimpleCSVInputTest() throws IOException, URISyntaxException {


    Log.info(getClass().getResource("/node_meta.csv").getPath());





    for (Vertex v : createVerticesFromCSV()) {
      System.out.println(v.getID());
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
}
