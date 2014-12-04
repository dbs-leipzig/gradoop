package org.gradoop.biiig.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.biiig.io.reader.FoodBrokerReader;
import org.gradoop.io.reader.BulkLoadEPG;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Runs the BIIIG Foodbroker/BTG Example
 */
public class BTGAnalysisDriver extends Configured implements Tool {
  private static Logger LOG = Logger.getLogger(BTGAnalysisDriver.class);

  /**
   * args[0] - bulk load input graph file
   * args[1] - bulk load output folder
   *
   * @param args driver arguments
   * @return Exit code (0 - ok)
   * @throws Exception
   */
  @Override
  public int run(String[] args)
    throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <graph-input-file> " +
        "<bulk-load-output-dir>%n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
    }

    Configuration conf = getConf();

    /*
    Step 0: Create HBase tables
     */
    HBaseGraphStoreFactory.createGraphStore(conf,
      new EPGVertexHandler(),
      new EPGGraphHandler()
    );

    /*
    Step 1: Bulk Load of the graph into HBase
     */
    if (!runBulkLoad(conf, args[0], args[1])) {
      return -1;
    }

    /*
    Step 2: BTG Computation
     */

    return 0;
  }

  /**
   * Runs the HFile conversion from the given file to the output dir. Also
   * loads the HFiles to region servers.
   *
   * @param conf      Cluster config
   * @param graphFile input file in HDFS
   * @param outDir    HFile output dir in HDFS
   * @throws IOException
   */
  private boolean runBulkLoad(Configuration conf, String graphFile, String
    outDir)
    throws Exception {
    Path inputFile = new Path(graphFile);
    Path outputDir = new Path(outDir);

    // set line reader to read lines in input splits
    conf.setClass(BulkLoadEPG.VERTEX_LINE_READER,
      FoodBrokerReader.class,
      VertexLineReader.class);
    // set vertex handler that creates the Puts
    conf.setClass(BulkLoadEPG.VERTEX_HANDLER,
      EPGVertexHandler.class,
      VertexHandler.class);

    Job job = new Job(conf, BulkLoadEPG.class.getName());
    // mapper that runs the HFile conversion
    job.setMapperClass(BulkLoadEPG.class);
    // input format for Mapper (File)
    job.setInputFormatClass(TextInputFormat.class);
    // output Key class of Mapper
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    // output Value class of Mapper
    job.setMapOutputValueClass(Put.class);

    // set input file
    FileInputFormat.addInputPath(job, inputFile);
    // set output directory
    FileOutputFormat.setOutputPath(job, outputDir);

    HTable hTable = new HTable(conf, GConstants.DEFAULT_TABLE_VERTICES);

    // auto configure partitioner and reducer corresponding to the number of
    // regions
    HFileOutputFormat2.configureIncrementalLoad(job, hTable);
    boolean successful;

    // run job
    successful = job.waitForCompletion(true);
    if (!successful) {
      LOG.error("Error during bulk import ... stopping pipeline");
      return false;
    }

    // load created HFiles to the region servers
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(outputDir, hTable);

    return true;
  }
}
