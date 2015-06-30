package org.xtreemfs.flink;

import java.io.File;
import java.io.PrintWriter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;

public class DataLocalityTest {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        if (args.length != 1) {
            System.err
                    .println("Invoke with one positional parameter: the number of OSDs.");
            System.exit(1);
        }

        int osdCount = 0;
        try {
            osdCount = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Bad number of OSD argument: " + args[0]);
            System.exit(1);
        }

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

        // Generate enough data to distribute among the OSDs.
        PrintWriter out = new PrintWriter(workingDirectory + "/words.txt",
                "UTF-8");

        // Each entry is 8 bytes and we want 128 kilobytes per OSD.
        for (int i = 0; i < osdCount * 128 * 1024 / 8; ++i) {
            // Always write the same value to each OSD.
            out.println(1000000 + (i / (128 * 1024 / 8)) % osdCount);
        }
        out.close();

        // Use words as input to Flink wordcount Job.
        DataSet<String> input = env.readTextFile(workingDirectory
                + "/words.txt", "UTF-8");

        DataSet<Long> mapped = input.map(new MapFunction<String, Long>() {

            private static final long serialVersionUID = 4902651910843421516L;

            @Override
            public Long map(String arg0) throws Exception {
                return Long.parseLong(arg0);
            }

        });

        UnsortedGrouping<Tuple3<Long, Integer, String>> counts = mapped.map(
                new MapFunction<Long, Tuple3<Long, Integer, String>>() {

                    private static final long serialVersionUID = 7917635531979595929L;

                    @Override
                    public Tuple3<Long, Integer, String> map(Long arg0)
                            throws Exception {
                        return new Tuple3<Long, Integer, String>(arg0, 1,
                                System.getenv("HOSTNAME"));
                    }

                }).groupBy(2);

        DataSet<Tuple3<Long, Integer, String>> result = counts.sum(1).andMax(0);

        System.out.println(input.count() + " --> " + result.count());
        result.print();

        File file = new File(workingDirectory + "/words.txt");
        System.out.println(file.length() + " bytes");
        file.delete();

    }
}
