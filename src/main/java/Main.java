import org.apache.commons.cli.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {
        Path INPUT_FILE = null;
        Path OUTPUT_FILE = null;
        int THREAD_COUNT = Runtime.getRuntime().availableProcessors() + 1;

        CommandLine commandLine;
        Option input_path_option = Option.builder("i")
                .argName("file")
                .hasArg()
                .required(true)
                .desc("A path to the file with data")
                .longOpt("input")
                .build();

        Option output_path_option = Option.builder("o")
                .argName("file")
                .hasArg()
                .required(true)
                .desc("A path where result should be stored")
                .longOpt("output")
                .build();

        Options options = new Options();
        options.addOption(input_path_option);
        options.addOption(output_path_option);
        CommandLineParser parser = new DefaultParser();

        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("i")) {
                INPUT_FILE = Paths.get(commandLine.getOptionValue("i"));
            }
            if (commandLine.hasOption("o")) {
                OUTPUT_FILE = Paths.get(commandLine.getOptionValue("o"));
            }
        } catch (Exception exception) {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());
            return;
        }

        try {
            long start = System.currentTimeMillis();

            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(THREAD_COUNT);
            List<Future<Map<String,List<String >>>> futureList = new ArrayList<>();
            int linesCount = (int)Files.lines(INPUT_FILE).count();
            int batchSize = linesCount/THREAD_COUNT;

            for(int i = 0; i <= THREAD_COUNT; i++){
                futureList.add(executor.submit(new SubgroupAggregator(INPUT_FILE, 1 + i*batchSize, Math.min(linesCount, (i+1)*batchSize))));
            }

            List<Map<String,List<String>>> groupMapList = new ArrayList<>();
            for(Future<Map<String,List<String>>> f : futureList) {
                groupMapList.add(f.get());
            }
            futureList.clear();

            while(groupMapList.size() > 1) {
                while(groupMapList.size() > 1) {
                    Map<String, List<String>> group1 = groupMapList.get(0);
                    groupMapList.remove(0);
                    Map<String, List<String>> group2 = groupMapList.get(0);
                    groupMapList.remove(0);
                    futureList.add(executor.submit(new GroupMerger(group1, group2)));
                }
                for(Future<Map<String,List<String>>> f : futureList) {
                    groupMapList.add(f.get());
                }
                futureList.clear();
            }
            executor.shutdown();

            List<List<String>> groups = groupMapList.get(0).values()
                    .stream()
                    .distinct()
                    .sorted(Comparator.comparing((List<String> s) -> s.size()).reversed())
                    .toList();

            long multipleElementGroupCnt = groups.stream().filter(g -> g.size() > 1).count();

            if (Files.exists(OUTPUT_FILE)) {
                Files.delete(OUTPUT_FILE);
            }

            Files.writeString(OUTPUT_FILE, String.valueOf(multipleElementGroupCnt)+"\n");
            for(int i = 0; i < groups.size(); i++) {
                List<String> group = groups.get(i);
                StringBuilder groupString = new StringBuilder();
                for(String s : group){
                    groupString.append(s).append('\n');
                }
                Files.writeString(OUTPUT_FILE, "Группа " + i + "\n", StandardOpenOption.APPEND);
                Files.writeString(OUTPUT_FILE, groupString.toString(), StandardOpenOption.APPEND);
            }

            long end = System.currentTimeMillis();
            float sec = (end - start) / 1000F;
            System.out.println(sec + " seconds");
            System.out.println("Total groups count: " + groups.size());
            System.out.println("Count of groups with more than one element: " + multipleElementGroupCnt);

        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }
    }
}
