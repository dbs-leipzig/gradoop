package org.gradoop.io.reader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.gradoop.GConstants;
import org.gradoop.MapReduceClusterTest;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.VertexHandler;
import org.junit.Test;

import java.io.BufferedReader;

/**
 * Testing the Bulk Import.
 */
public class BulkLoadTest extends MapReduceClusterTest {

  @Test
  public void bulkLoadTest()
    throws Exception {
    Configuration conf = utility.getConfiguration();
    // need to create tables
    GraphStore graphStore = createEmptyGraphStore();
    graphStore.close();
    // need to create test file and output dir
    FileSystem fs = utility.getTestFileSystem();
    Path graphFile = new Path("graph.txt");
    Path outputDir = new Path("/output/hfiles");
    FSDataOutputStream bw = fs.create(graphFile);
    BufferedReader br = createTestReader(EXTENDED_GRAPH);
    IOUtils.copy(br, bw);
    bw.close();
    br.close();

    /*
    Setup MapReduce Job for BulkImport
     */
    conf.setClass(BulkLoadEPG.VERTEX_LINE_READER,
      EPGVertexReader.class,
      VertexLineReader.class);
    conf.setClass(BulkLoadEPG.VERTEX_HANDLER,
      EPGVertexHandler.class,
      VertexHandler.class);

    Job job = new Job(conf, BulkLoadTest.class.getName());
    job.setMapperClass(BulkLoadEPG.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, graphFile);
    FileOutputFormat.setOutputPath(job, outputDir);

    HTable hTable = new HTable(conf, GConstants.DEFAULT_TABLE_VERTICES);

    // auto configure partitioner and reducer
    HFileOutputFormat2.configureIncrementalLoad(job, hTable);

    // run job
    job.waitForCompletion(true);

    /*
     Load created HFiles to the region servers
      */
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(outputDir, hTable);

    // validate data
    graphStore = openGraphStore();
    validateExtendedGraphVertices(graphStore);
  }
}
