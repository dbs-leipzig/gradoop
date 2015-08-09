//package org.gradoop.io.writer;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.gradoop.GConstants;
//import org.gradoop.GradoopClusterTest;
//import org.gradoop.model.VertexData;
//import org.gradoop.storage.GraphStore;
//import org.gradoop.storage.hbase.EPGVertexHandler;
//import org.gradoop.storage.hbase.VertexHandler;
//import org.junit.Test;
//
//import java.io.IOException;
//
///**
// * Testing for Bulk Export.
// */
//public class BulkWriteTest extends GradoopClusterTest {
//
//  @Test
//  public void bulkWriteSimpleGraphTest() throws IOException,
//    ClassNotFoundException, InterruptedException {
//    Configuration conf = utility.getConfiguration();
//    // create store and store some test data
//    GraphStore store = createEmptyGraphStore();
//    for (VertexData v : createBasicGraphVertices()) {
//      store.writeVertexData(v);
//    }
//    store.close();
//
//    /*
//    Setup MapReduce Job for BulkExport
//     */
//    conf.setClass(BulkWriteEPG.VERTEX_LINE_WRITER, SimpleVertexWriter.class,
//      VertexLineWriter.class);
//    conf.setClass(BulkWriteEPG.VERTEX_HANDLER, EPGVertexHandler.class,
//      VertexHandler.class);
//
//    Job job = Job.getInstance(conf, BulkWriteTest.class.getName());
//    Scan scan = new Scan();
//    scan.setCaching(500);
//    scan.setCacheBlocks(false);
//
//    // map
//    TableMapReduceUtil
//      .initTableMapperJob(GConstants.DEFAULT_TABLE_VERTICES, scan,
//        BulkWriteEPG.class, Text.class, NullWritable.class, job);
//    // no reduce needed for that job
//    job.setNumReduceTasks(0);
//
//    // set output path
//    Path outputDir = new Path("/output/export/simple-graph");
//    FileOutputFormat.setOutputPath(job, outputDir);
//
//    // run
//    job.waitForCompletion(true);
//
//    // read map output
//    Path outputFile = new Path(outputDir, "part-m-00000");
//    String[] fileContent = readGraphFromFile(outputFile, BASIC_GRAPH.length);
//
//    // validate text output with input graph
//    validateBasicGraphVertices(fileContent);
//  }
//}
