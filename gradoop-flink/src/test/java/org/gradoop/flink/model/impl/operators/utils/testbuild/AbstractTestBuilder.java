package org.gradoop.flink.model.impl.operators.utils.testbuild;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.fusion.Fusion;
import org.gradoop.flink.model.impl.operators.utils.GDLBuilder;
import org.gradoop.flink.model.impl.operators.utils.IWithDependencies;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by Giacomo Bergami on 19/01/17.
 * Testing if GDL is correctly written and generating automatically the tests
 */
public abstract class AbstractTestBuilder extends GradoopFlinkTestBase {

    HashMap<String,IWithDependencies> toAssociate;

    protected void addTo(String varname, IWithDependencies o) {
        toAssociate.put(varname,o);
    }
    public static GDLBuilder.VertexBuilder<?> simpleVertex(String var, String type) {
        GDLBuilder.VertexBuilder<?> a = new GDLBuilder.VertexBuilder<>();
        GDLBuilder.VertexBuilder.generateWithValueAndType(null,a,var,type).propList().put(var+"value",var+"type").plEnd();
        return a;
    }

    public AbstractTestBuilder() {
        super();
        toAssociate = new HashMap<>();
    }

    public  String javify(String toJava) {
        try {

            return "\""+toAssociate.get(toJava).toString().replaceAll("\"","\\\\\"").replaceAll("\n", "\"+\n\t\t\t\"")+"\"";
        } catch (Exception e) {
            System.err.println(toJava);
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    public void generateToFile(File f, String binaryClassName, String testPatterns) {
        try(  PrintWriter out = new PrintWriter( f )  ){
            out.println("import org.gradoop.flink.model.GradoopFlinkTestBase;\n" +
                    "import org.gradoop.flink.model.impl.LogicalGraph;\n" +
                    "import org.gradoop.flink.util.FlinkAsciiGraphLoader;\n" +
                    "import org.junit.Test;\n\n" +
                    "public class "+binaryClassName+"Test extends GradoopFlinkTestBase {\n" + Pattern.compile("\n", Pattern.LITERAL)
                    .splitAsStream(testPatterns.trim())
                    .map(x -> x.trim().split(" "))
                    .map(x -> {
                        String testName = x[0] + "_" + x[1] + "_to_" + x[2];
                        StringBuilder sb = new StringBuilder();
                        Set<String> toJavify = new LinkedHashSet<>();
                        toAssociate.entrySet().stream()
                                .filter(y -> y.getKey().equals(x[0]) || y.getKey().equals(x[1]) || y.getKey().equals(x[2]))
                                .flatMap(y->y.getValue().getDependencies().stream())
                                .map(this::javify)
                                .forEach(toJavify::add);
                        toJavify.add(javify(x[0]));
                        toJavify.add(javify(x[1]));
                        toJavify.add(javify(x[2]));
                        StringBuilder append = sb.append("\t@Test\n\tpublic void ").append(x[0]).append("_").append(x[1]).append("_to_").append(x[2]).append("() throws Exception {\n\t\t").append("FlinkAsciiGraphLoader loader = getLoaderFromString(").append(toJavify.stream().collect(Collectors.joining("+\n\t\t\t"))).append(");\n\t\t").append("LogicalGraph left = loader.getLogicalGraphByVariable(\"").append(x[0]).append("\");\n\t\t").append("LogicalGraph right = loader.getLogicalGraphByVariable(\"").append(x[1]).append("\");\n\t\t")
                                .append(binaryClassName+" f = null;\n\t\t").append("LogicalGraph output = f.execute(left,right);\n\t\t").append("collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable(\"").append(x[2]).append("\")));\n\t}");
                        return sb.toString();
                    }).collect(Collectors.joining("\n\n"))+"\n}");
        } catch (Exception e) {

        }
    }


    protected FlinkAsciiGraphLoader loader;
    protected abstract FlinkAsciiGraphLoader getTestGraphLoader();


    public void check() {
        getTestGraphLoader();
        loader = getLoaderFromString(toAssociate.values().stream().map(Object::toString).collect(Collectors.joining("\n")));
    }

}
