package csv.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.GradoopClusterTest;
import org.gradoop.csv.examples.SNAAnalysisDriver;
import org.junit.Test;

import java.net.URL;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests the pipeline described in
 * {@link SNAAnalysisDriver}.
 */
public class SNAAnalysisDriverTest extends GradoopClusterTest {

  Logger LOG = Logger.getLogger(SNAAnalysisDriverTest.class);

  @Test
  public void driverTest() throws Exception {
    Configuration conf = utility.getConfiguration();


    String nodeFile = "person.csv";
    String edgeFile = "person_knows_person.csv";

    URL resourceUrl = getClass().getResource("/person_meta.csv");

    LOG.info("###resource URL: " + resourceUrl);

    String[] args = new String[] {
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_VERTEX_LINE_READER,
      "CSVReader", "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_DROP_TABLES,
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_BULKLOAD,
      "-" + SNAAnalysisDriver.OPTION_GRAPH_INPUT_PATH, "input",
      "-" + SNAAnalysisDriver.OPTION_GRAPH_OUTPUT_PATH, "/output/sna",
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_METADATA_PATH, resourceUrl
      .getPath().replace("person_meta.csv","")
//      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_WORKERS, "1",
//      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_LABLEPROPAGATION
    };

    copyFromLocal(nodeFile);
    copyFromLocal(edgeFile);

    SNAAnalysisDriver snaAnalysisDriver = new SNAAnalysisDriver();
    snaAnalysisDriver.setConf(conf);

    //run the pipeline
    int exitCode1 = snaAnalysisDriver.run(args);


    //test
    assertThat(exitCode1, is(0));

  }

}
