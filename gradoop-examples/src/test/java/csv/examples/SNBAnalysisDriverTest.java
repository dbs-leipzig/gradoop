package csv.examples;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.GradoopClusterTest;
import org.gradoop.utils.ConfigurationUtils;
import org.junit.Test;

/**
 * Tests the pipeline described in
 * {@link org.gradoop.csv.examples.SNBAnalysisDriver}.
 */
public class SNBAnalysisDriverTest extends GradoopClusterTest {

  @Test
  public void driverTest() throws Exception{
    Configuration conf = utility.getConfiguration();


    String nodeFile = "csv.node";

    String[] args1 = new String[] {
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, nodeFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH, "/output/csv",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    String edgeFile ="csv.edge";

    String[] args2 = new String[] {
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, edgeFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH, "/output/csv",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };




  }

}
