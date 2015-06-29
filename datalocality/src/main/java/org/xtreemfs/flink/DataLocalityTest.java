package org.xtreemfs.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class DataLocalityTest {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        DataSet<String> text = env.fromElements("Flink", "XtreemFS",
                "Data Locality", "Test");
        text.print();
    }

}
