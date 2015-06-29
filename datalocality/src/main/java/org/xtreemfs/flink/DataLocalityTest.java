package org.xtreemfs.flink;

import java.io.File;
import java.io.PrintWriter;
import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class DataLocalityTest {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        final String workingDirectory = System.getenv("WORK");
        if (workingDirectory == null) {
            System.err
                    .println("$WORK must point to an XtreemFS volume mount point (as a file system path).");
            System.exit(1);
        }

        final String defaultVolume = System.getenv("DEFAULT_VOLUME");
        if (defaultVolume == null) {
            System.err
                    .println("$DEFAULT_VOLUME must point to an XtreemFS volume URL ('xtreemfs://hostname:port/volume').");
            System.exit(1);
        }

        // Generate 256kB of data to distribute among the two OSDs.
        Random random = new Random(0);
        PrintWriter writer = new PrintWriter(workingDirectory + "/words.txt",
                "UTF-8");
        for (int i = 0; i < 32768; ++i) {
            writer.println("Word" + (100 + random.nextInt(100)));
        }
        writer.close();

        // Use words as input to Flink wordcount Job.
        DataSet<String> words = env.readTextFile(defaultVolume + "/words.txt",
                "UTF-8");
        DataSet<String> filtered = words.filter(new FilterFunction<String>() {

            private static final long serialVersionUID = -7778608339455035028L;

            @Override
            public boolean filter(String arg0) throws Exception {
                return arg0.endsWith("5");
            }

        });

        DataSet<Tuple2<String, Integer>> counts = filtered
                .map(new MapFunction<String, Tuple2<String, Integer>>() {

                    private static final long serialVersionUID = 7917635531979595929L;

                    @Override
                    public Tuple2<String, Integer> map(String arg0)
                            throws Exception {
                        return new Tuple2<String, Integer>(arg0, 1);
                    }

                }).groupBy(0).sum(1);

        counts.print();

        File file = new File(workingDirectory + "/words.txt");
        System.out.println(file.length() + " bytes");
        file.delete();

    }
}
