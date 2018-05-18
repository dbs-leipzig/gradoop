package org.gradoop;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloGraphStoreTest;
import org.gradoop.flink.io.impl.accumulo.AccumuloDataSinkSourceTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.gradoop.common.config.GradoopAccumuloConfig.*;

/**
 * gradoop accumulo test suit
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  AccumuloGraphStoreTest.class, AccumuloDataSinkSourceTest.class,
})
public class AccumuloTestSuite {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloTestSuite.class);

  /**
   * accumulo password
   */
  private static final String ACCUMULO_PASSWORD = "123456";

  /**
   * accumulo minicluster for test
   */
  private static MiniAccumuloCluster accumulo;

  /**
   * temporary folder creator
   */
  @ClassRule
  public static TemporaryFolder tmp = new TemporaryFolder();

  public static MiniAccumuloCluster getAccumulo() {
    return accumulo;
  }

  /**
   * get gradoop accumulo configure
   *
   * @param prefix store prefix
   * @return gradoop accumulo configure
   */
  public static GradoopAccumuloConfig<GraphHead, Vertex, Edge> getAcConfig(
    ExecutionEnvironment env,
    String prefix
  ) {
    return GradoopAccumuloConfig.getDefaultConfig(env)
      .set(ACCUMULO_USER, "root")
      .set(ACCUMULO_INSTANCE, accumulo.getInstanceName())
      .set(ZOOKEEPER_HOSTS, accumulo.getZooKeepers())
      .set(ACCUMULO_PASSWD, accumulo.getConfig().getRootPassword())
      .set(ACCUMULO_TABLE_PREFIX, prefix);
    //those are configure default ⤵
    //.set(ACCUMULO_AUTHORIZATIONS, Authorizations.EMPTY)
    //.set(GRADOOP_BATCH_SCANNER_THREADS, 10)
    //.set(GRADOOP_ITERATOR_PRIORITY, 0xf);

    //or you can change to your own test env, please copy gradoop-accumulo jar to your accumulo
    //runtime lib dir => $ACCUMULO_HOME/lib/ext

    //return GradoopAccumuloConfig.create()
    //  .set(ZOOKEEPER_HOSTS, "docker2:2181")
    //  .set(ACCUMULO_INSTANCE, "isinonet")
    //  .set(ACCUMULO_PASSWORD, "123456")
    //  .set(ACCUMULO_USER, "root");
  }

  /**
   * create mini cluster accumulo instance for test
   */
  @BeforeClass
  public static void setupAccumulo() throws Exception {
    LOG.warn("If using your own accumulo cluster for test, compile gradoop-accumulo jar, " +
      "and copy it as accumulo external lib, which locate at $ACCUMULO_HOME/lib/ext");
    tmp.create();
    File tmpFolder = tmp.newFolder();
    MiniAccumuloConfig config = new MiniAccumuloConfig(tmpFolder, ACCUMULO_PASSWORD);
    config.setNativeLibPaths(AccumuloTestSuite.class.getResource("/").getFile());
    accumulo = new MiniAccumuloCluster(config);
    accumulo.start();
    LOG.info("create mini accumulo start success!");
  }

  /**
   * terminate and remove temporary file
   */
  @AfterClass
  public static void terminateAccumulo() throws IOException, InterruptedException {
    LOG.info("terminate mini accumulo cluster");
    try {
      accumulo.stop();
    } finally {
      tmp.delete();
    }
  }

}
