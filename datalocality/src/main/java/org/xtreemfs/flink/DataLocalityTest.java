package org.xtreemfs.flink;

import java.io.File;
import java.io.PrintWriter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;

public class DataLocalityTest {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        if (args.length != 2) {
            System.err
                    .println("Invoke with two positional parameters: the number of OSDs, the number of stripes per OSD.");
            System.exit(1);
        }

        int osdCount = 0;
        try {
            osdCount = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Bad number of OSD argument: " + args[0]);
            System.exit(1);
        }

        int stripesPerOsd = 0;
        try {
            stripesPerOsd = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Bad number of stripes per OSD argument: "
                    + args[1]);
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
        for (int i = 0; i < stripesPerOsd * osdCount * 128 * 1024 / 8; ++i) {
            // Always write the same value to each OSD.
            out.println(1000000 + (i / (128 * 1024 / 8)) % osdCount);
        }
        out.close();

        // Use words as input to Flink "wordcount" Job.
        DataSet<String> input = env.readTextFile(workingDirectory
                + "/words.txt", "UTF-8");

        DataSet<Tuple4<String, Long, Long, Long>> counts = input
                .map(new MapFunction<String, Tuple4<String, Long, Long, Long>>() {

                    private static final long serialVersionUID = 7917635531979595929L;

                    @Override
                    public Tuple4<String, Long, Long, Long> map(String arg0)
                            throws Exception {
                        Long arg = Long.parseLong(arg0);
                        return new Tuple4<String, Long, Long, Long>(System
                                .getenv("HOSTNAME"), 1L, arg, arg);
                    }

                }).groupBy(0).min(2).andMax(3).andSum(1);

        System.out.println(input.count() + " --> " + counts.count());
        counts.print();

        File file = new File(workingDirectory + "/words.txt");
        System.out.println(file.length() + " bytes ("
                + (stripesPerOsd * osdCount * 128 * 1024 / 8)
                + " 8-byte strings)");
        file.delete();

    }
}
