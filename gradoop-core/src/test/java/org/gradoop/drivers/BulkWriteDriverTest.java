//package org.gradoop.drivers;
//
//import org.apache.hadoop.fs.Path;
//import org.gradoop.GradoopClusterTest;
//import org.gradoop.io.writer.SimpleVertexWriter;
//import org.gradoop.model.VertexData;
//import org.gradoop.storage.GraphStore;
//import org.junit.Test;
//
//import static org.hamcrest.CoreMatchers.is;
//import static org.junit.Assert.assertThat;
//
///**
// * Tests for {@link org.gradoop.drivers.BulkWriteDriver}.
// */
//public class BulkWriteDriverTest extends GradoopClusterTest {
//
//  @Test
//  public void testBulkWriteDriver() throws Exception {
//    GraphStore store = createEmptyGraphStore();
//    for (VertexData v : createBasicGraphVertices()) {
//      store.writeVertexData(v);
//    }
//    store.close();
//
//    String outputDirName = "/output/export/extended-graph";
//
//    String[] args = new String[] {
//      "-" + BulkWriteDriver.LoadConfUtils.OPTION_VERTEX_LINE_WRITER,
//      SimpleVertexWriter.class.getName(),
//      "-" + BulkWriteDriver.LoadConfUtils.OPTION_HBASE_SCAN_CACHE, "10",
//      "-" + BulkWriteDriver.OPTION_GRAPH_OUTPUT_PATH, outputDirName,
//      "-" + BulkWriteDriver.OPTION_CUSTOM_ARGUMENT,
//      "sna-reader.label=knows, sna-reader.meta_data=person_knows_person.meta"
//    };
//
//    BulkWriteDriver bulkWriteDriver = new BulkWriteDriver();
//    bulkWriteDriver.setConf(utility.getConfiguration());
//
//    // run the bulk write
//    int exitCode = bulkWriteDriver.run(args);
//
//    // testing
//    assertThat(exitCode, is(0));
//
//    // read map output
//    Path outputFile = new Path(outputDirName, "part-m-00000");
//    String[] fileContent = readGraphFromFile(outputFile, BASIC_GRAPH.length);
//
//    // validate text output with input graph
//    validateBasicGraphVertices(fileContent);
//  }
//}
