/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.storage.impl.accumulo;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.gradoop.storage.accumulo.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.basic.StoreTest;
import org.gradoop.storage.impl.accumulo.io.IOBasicTest;
import org.gradoop.storage.impl.accumulo.io.source.IOEdgePredicateTest;
import org.gradoop.storage.impl.accumulo.io.source.IOGraphPredicateTest;
import org.gradoop.storage.impl.accumulo.io.source.IOVertexPredicateTest;
import org.gradoop.storage.impl.accumulo.predicate.StoreBasicPredicateTest;
import org.gradoop.storage.impl.accumulo.predicate.StoreIdsPredicateTest;
import org.gradoop.storage.impl.accumulo.predicate.StoreLabelPredicateTest;
import org.gradoop.storage.impl.accumulo.predicate.StorePropPredicateTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;

import static org.gradoop.storage.accumulo.config.GradoopAccumuloConfig.*;

/**
 * gradoop accumulo test suit
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  //basic
  StoreTest.class,
  //predicate
  StoreBasicPredicateTest.class,
  StoreIdsPredicateTest.class,
  StoreLabelPredicateTest.class,
  StorePropPredicateTest.class,
  //sink and source
  IOBasicTest.class,
  IOEdgePredicateTest.class,
  IOVertexPredicateTest.class,
  IOGraphPredicateTest.class
})
public class AccumuloTestSuite {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloTestSuite.class);

  /**
   * Accumulo password
   */
  private static final String PASSWD = "123456";

  /**
   * Test namespace prefix
   */
  private static final String TEST_NAMESPACE_PREFIX = "gradoop_test";

  /**
   * Accumulo minicluster for test
   */
  private static MiniAccumuloCluster accumulo;

  /**
   * Temporary folder creator
   */
  @ClassRule
  public static TemporaryFolder tmp = new TemporaryFolder();

  public static MiniAccumuloCluster getAccumulo() {
    return accumulo;
  }

  /**
   * Get gradoop accumulo configure
   *
   * @param prefix store prefix
   * @return gradoop accumulo configure
   */
  public static GradoopAccumuloConfig getAcConfig(String prefix) {
    return GradoopAccumuloConfig.getDefaultConfig()
      .set(ACCUMULO_USER, "root")
      .set(ACCUMULO_INSTANCE, accumulo.getInstanceName())
      .set(ZOOKEEPER_HOSTS, accumulo.getZooKeepers())
      .set(ACCUMULO_PASSWD, accumulo.getConfig().getRootPassword())
      .set(ACCUMULO_TABLE_PREFIX, TEST_NAMESPACE_PREFIX + "." + prefix);
    //those are configure default ⤵
    //.set(ACCUMULO_AUTHORIZATIONS, Authorizations.EMPTY)
    //.set(GRADOOP_BATCH_SCANNER_THREADS, 10)
    //.set(GRADOOP_ITERATOR_PRIORITY, 0xf);

    //or you can change to your own test env, please copy gradoop-accumulo jar to your accumulo
    //runtime lib dir => $ACCUMULO_HOME/lib/ext
    //return GradoopAccumuloConfig.getDefaultConfig(env)
    //  .set(ZOOKEEPER_HOSTS, "docker2:2181")
    //  .set(ACCUMULO_INSTANCE, "instance")
    //  .set(ACCUMULO_PASSWD, "root")
    //  .set(ACCUMULO_USER, "root")
    //  .set(ACCUMULO_TABLE_PREFIX, TEST_NAMESPACE_PREFIX + "." + prefix);
  }

  /**
   * Create mini cluster accumulo instance for test
   *
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupAccumulo() throws Exception {
    LOG.warn("If using your own accumulo cluster for test, compile gradoop-accumulo jar, " +
      "and copy it as accumulo external lib, which locate at $ACCUMULO_HOME/lib/ext");
    tmp.create();
    File tmpFolder = tmp.newFolder();
    MiniAccumuloConfig config = new MiniAccumuloConfig(tmpFolder, PASSWD);
    config.setNativeLibPaths(
      URLDecoder.decode(AccumuloTestSuite.class.getResource("/").getFile(),
      StandardCharsets.UTF_8.name()));
    accumulo = new MiniAccumuloCluster(config);
    try {
      accumulo.start();
    } catch (RuntimeException e) {
      e.printStackTrace();
      Field fi = MiniAccumuloConfig.class.getDeclaredField("impl");
      fi.setAccessible(true);
      MiniAccumuloConfigImpl impl = (MiniAccumuloConfigImpl) fi.get(accumulo.getConfig());
      File logdir = impl.getLogDir();
      Collection<File> all = new ArrayList<>();
      addTree(logdir, all);
      all.stream().filter(f -> !f.isDirectory()).forEach(file -> {
        try {
          Scanner myReader = new Scanner(file);
          while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            System.out.println(data);
          }
          myReader.close();
        } catch (FileNotFoundException ed) {
          ed.printStackTrace();
        }
      });
      System.out.println();
    }
    LOG.info("create mini accumulo start success!");
  }

  static void addTree(File file, Collection<File> all) {
    File[] children = file.listFiles();
    if (children != null) {
      for (File child : children) {
        all.add(child);
        addTree(child, all);
      }
    }
  }

  /**
   * Terminate and remove temporary file
   *
   * @throws Exception on failure
   */
  @AfterClass
  public static void terminateAccumulo() throws Exception {
    LOG.info("terminate mini accumulo cluster");
    try {
      accumulo.stop();
    } finally {
      tmp.delete();
    }
  }

}
