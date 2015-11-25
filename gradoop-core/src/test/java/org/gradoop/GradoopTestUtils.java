package org.gradoop;

import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.AsciiGraphLoader;
import org.gradoop.util.GradoopConfig;

import java.io.IOException;

public class GradoopTestUtils {

  public static final String SOCIAL_NETWORK_GDL_FILE = "/data/social_network.gdl";

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  public static AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
  getSocialNetworkLoader() throws IOException {
    GradoopConfig<GraphHeadPojo, VertexPojo, EdgePojo> config =
      GradoopConfig.getDefaultConfig();

    return AsciiGraphLoader.fromFile(
      GradoopHBaseTestBase.class.getResource(SOCIAL_NETWORK_GDL_FILE).getFile(),
      config);
  }
}
