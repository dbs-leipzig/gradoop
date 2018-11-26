package org.gradoop.dataintegration.importer.csv;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

/**
 * Test class for {@link org.gradoop.dataintegration.importer.csv.MinimalCSVImporter}
 */
public class MinimalCSVImporterTest extends GradoopFlinkTestBase {

    /**
     * Test if the properties of the imported vertices works correct.
     * @throws Exception
     */
    @Test
    public void testImport() throws Exception {

        String csvPath = getFilePath("/csv/input.csv");
        String delimiter = ";";

        ExecutionEnvironment env = getExecutionEnvironment();

        MinimalCSVImporter importVertexImporter = new MinimalCSVImporter(csvPath, delimiter, getConfig());

        DataSet<ImportVertex<Long>> importVertex = importVertexImporter.importVertices();

        List<ImportVertex<Long>> lv = new ArrayList<>();
        importVertex.output(new LocalCollectionOutputFormat<>(lv));

        env.execute();

        assertThat(lv.size(), is(3));

        for(ImportVertex<Long> v : lv) {
            if (v.f2.get("name").toString().equals("foo")) {
                assertThat(v.f2.size(), is(4));
                assertThat(v.f2.get("name").toString(), is("foo"));
                assertThat(v.f2.get("value1").toString(), is("453"));
                assertThat(v.f2.get("value2").toString(), is("true"));
                assertThat(v.f2.get("value3").toString(), is("71.03"));
            } else if (v.f2.get("name").toString().equals("bar")) {
                assertThat(v.f2.size(), is(4));
                assertThat(v.f2.get("name").toString(), is("bar"));
                assertThat(v.f2.get("value1").toString(), is("76"));
                assertThat(v.f2.get("value2").toString(), is("false"));
                assertThat(v.f2.get("value3").toString(), is("33.4"));
            } else if (v.f2.get("name").toString().equals("bla")) {
                assertThat(v.f2.size(), is(3));
                assertThat(v.f2.get("name").toString(), is("bla"));
                assertThat(v.f2.get("value1").toString(), is("4568"));
                assertThat(v.f2.get("value3").toString(), is("9.42"));
            } else {
                fail();
            }
        }
    }
}
