package csv.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.GradoopClusterTest;
import org.gradoop.csv.examples.SNAAnalysisDriver;
import org.gradoop.csv.io.reader.CSVReader;
import org.gradoop.drivers.BulkDriver;
import org.gradoop.utils.ConfigurationUtils;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests the pipeline described in
 * {@link SNAAnalysisDriver}.
 */
public class SNAAnalysisDriverTest extends GradoopClusterTest {

  private static final Logger LOG = Logger.getLogger(SNAAnalysisDriverTest.class);
  @Test
  public void driverTest() throws Exception{
    Configuration conf = utility.getConfiguration();


    String nodeFile = "csv.node";

    String[] args1 = new String[] {
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_DROP_TABLES,
      "-" + SNAAnalysisDriver.LoadConfUtils.OPTION_BULKLOAD,
      "-" + BulkDriver.OPTION_GRAPH_INPUT_PATH, nodeFile,
      "-" + BulkDriver.OPTION_GRAPH_OUTPUT_PATH, "/output/csv",
      "-" + BulkDriver.OPTION_CUSTOM_ARGUMENT, CSVReader.TYPE+"=node",
      "-" + BulkDriver.OPTION_CUSTOM_ARGUMENT, CSVReader.LABEL+"=Person",
      "-" + BulkDriver.OPTION_CUSTOM_ARGUMENT, CSVReader.META_DATA+"=long|string|long|"
    };

    for(int i=0; i<args1.length;i++ ){
      LOG.info("###"+args1[i]);
    }


    String edgeFile ="csv.edge";

    String[] args2 = new String[] {
      "-" + ConfigurationUtils.OPTION_WORKERS, "1",
      "-" + ConfigurationUtils.OPTION_GRAPH_INPUT_PATH, edgeFile,
      "-" + ConfigurationUtils.OPTION_GRAPH_OUTPUT_PATH, "/output/csv",
      "-" + ConfigurationUtils.OPTION_DROP_TABLES
    };

    copyFromLocal(nodeFile);
    copyFromLocal(edgeFile);


    LOG.info("###Initialize SNADriver Object");

    SNAAnalysisDriver snaAnalysisDriver = new SNAAnalysisDriver();
    snaAnalysisDriver.setConf(conf);

    //run the pipeline
    int exitCode = snaAnalysisDriver.run(args1);
    //exitCode += snaAnalysisDriver.run(args2);

    //test
    assertThat(exitCode, is(0));
  }

}
