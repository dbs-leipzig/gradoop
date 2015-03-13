package csv.io.reader;

import com.google.common.collect.Lists;
import org.gradoop.GradoopClusterTest;
import org.gradoop.csv.io.reader.CSVReader;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * Tests for {@link org.gradoop.csv.io.reader.CSVReader}.
 */
public class CSVReaderTest extends GradoopClusterTest {

  private static final String NODE_META = "node_meta.csv";
  private static final String NODE_TEXT = "node.csv";



  @Test
  public void checkSimpleCSVInputTest() {
    for (Vertex v : createVerticesFromCSV()) {
      if(v.getID() == 0){
      }
    }

  }

  private List<Vertex> createVerticesFromCSV(){
    VertexLineReader reader = new CSVReader(NODE_META);
    List<Vertex> vertices = Lists.newArrayList();

    try {
      BufferedReader in = new BufferedReader(new FileReader(NODE_TEXT));
      String line;
      while ((line = in.readLine()) != null) {
        for (Vertex v : reader.readVertexList(line)) {
          vertices.add(v);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return vertices;
  }

}
