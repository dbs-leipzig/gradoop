package org.gradoop.rdf.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gradoop.GConstants;
import org.gradoop.rdf.algorithms.RDFInstanceEnrichment;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * RDF Instance Enrichment Driver
 */
public class RDFInstanceEnrichmentDriver extends Configured implements Tool {
  /**
   * Job names start with that prefix.
   */
  private static final String JOB_PREFIX = "RDF Instance Enrichment: ";

  static {
    Configuration.addDefaultResource("hbase-site.xml");
  }

  /**
   * Starting point for RDF analysis pipeline.
   *
   * @param args driver arguments
   * @return Exit code (0 - ok)
   * @throws Exception
   */
  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    CommandLine cmd = ConfigurationUtils.parseArgs(args);

    boolean verbose = cmd.hasOption(ConfigurationUtils.OPTION_VERBOSE);

    /*
    Step 0: Open HBase tables
     */
    HBaseGraphStoreFactory.createOrOpenGraphStore(conf, new EPGVertexHandler(),
      new EPGGraphHandler());

    /*
    Step 1: Enrichment of properties for instances (MapReduce)
     */
    int sc = Integer.parseInt(
      cmd.getOptionValue(ConfigurationUtils.OPTION_HBASE_SCAN_CACHE, "500"));
    if (!runEnrich(conf, sc, verbose)) {
      return -1;
    }

    return 0;
  }

  /**
   * Runs Enrichment of instances using a MapReduce Job.
   *
   * @param conf      Cluster configuration
   * @param scanCache HBase client scan cache
   * @param verbose   print output during job  @return true, if the job
   *                  completed successfully, false otherwise
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @return success state
   */
  private boolean runEnrich(Configuration conf, int scanCache,
    boolean verbose) throws IOException, ClassNotFoundException,
    InterruptedException {
    conf.setClass(GConstants.VERTEX_HANDLER_CLASS, EPGVertexHandler.class,
      VertexHandler.class);

    Job job =
      Job.getInstance(conf, JOB_PREFIX + RDFInstanceEnrichment.class.getName());
    Scan scan = new Scan();
    scan.setCaching(scanCache);
    scan.setCacheBlocks(false);

    // map
    TableMapReduceUtil.initTableMapperJob(GConstants.DEFAULT_TABLE_VERTICES,
      scan, RDFInstanceEnrichment.EnrichMapper.class, ImmutableBytesWritable
        .class, Put.class, job);
    // no reduce
    TableMapReduceUtil.initTableReducerJob(GConstants.DEFAULT_TABLE_VERTICES,
      null, job);
    job.setNumReduceTasks(0);

    // run
    return job.waitForCompletion(verbose);
  }

  /**
   * Runs the job.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new RDFInstanceEnrichmentDriver(), args));
  }
}
