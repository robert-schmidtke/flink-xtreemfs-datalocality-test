package org.xtreemfs.flink;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
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
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(workingDirectory + "/data.bin")));

        // Each entry is 8 bytes and we want 128 kilobytes per OSD.
        for (int i = 0; i < osdCount * 128 * 1024 / 8; ++i) {
            // Always write the same value to each OSD.
            out.writeLong((i / (128 * 1024 / 8)) % osdCount);
        }
        out.close();

        // Use words as input to Flink wordcount Job.
        DataSet<Long> input = env.readFileOfPrimitives(workingDirectory
                + "/data.bin", Long.class);

        DataSet<Long> filtered = input;
        // .filter(new FilterFunction<Long>() {
        //
        // private static final long serialVersionUID = -7778608339455035028L;
        //
        // @Override
        // public boolean filter(Long arg0) throws Exception {
        // return arg0 % 2 == 0;
        // }
        //
        // });

        DataSet<Tuple3<Long, Integer, String>> counts = filtered
                .map(new MapFunction<Long, Tuple3<Long, Integer, String>>() {

                    private static final long serialVersionUID = 7917635531979595929L;

                    @Override
                    public Tuple3<Long, Integer, String> map(Long arg0)
                            throws Exception {
                        return new Tuple3<Long, Integer, String>(arg0, 1,
                                System.getenv("HOSTNAME"));
                    }

                }).groupBy(2).aggregate(Aggregations.MAX, 1)
                .aggregate(Aggregations.MAX, 0);

        counts.print();

        File file = new File(workingDirectory + "/data.bin");
        System.out.println(file.length() + " bytes");
        file.delete();

    }

}
