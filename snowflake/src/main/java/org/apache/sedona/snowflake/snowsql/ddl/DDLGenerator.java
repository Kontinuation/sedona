package org.apache.sedona.snowflake.snowsql.ddl;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.exit;

public class DDLGenerator {

    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-h")) {
                printUsage();
            }
            if (arg.startsWith("--")) {
                String argName = arg.substring(2).replace("-", "_");
                String argValue = args[++i];
                argMap.put(argName, argValue);
            }
        }
        try {
            assert argMap.containsKey(Constants.SEDONA_VERSION);
            assert argMap.containsKey(Constants.GEOTOOLS_VERSION);
        } catch (AssertionError e) {
            System.out.println("Missing required arguments");
            printUsage();
        }
        return argMap;
    }

    public static void printUsage() {
        System.out.println("Usage: java -jar snowsql-ddl-generator.jar [options]");
        System.out.println("Must have Arguments");
        System.out.println("  --sedona-version <version>");
        System.out.println("  --geotools-version <version>");
        System.out.println("Optional have Arguments");
        System.out.println("  --schema=<schema>  snowflake schema register functions");
        System.out.println("  --h  Print this help message");
        exit(0);
    }

    public static void main(String[] args) {
        // Config for generating DDL on WB3P
        String stageName = "@WHEROBOTS";
        boolean isNativeApp = false;
        String appRoleName = "app_public";

        // Config for generating DDL on Snowflake Native App
//        stageName = "";
//        isNativeApp = true;
//        appRoleName = "app_public";

        if (isNativeApp) {
            System.out.println("-- Generating DDL for Snowflake Native App");
            System.out.println("CREATE APPLICATION ROLE " + appRoleName + ";");
            System.out.println("CREATE OR ALTER VERSIONED SCHEMA sedona;");
            System.out.println("GRANT USAGE ON SCHEMA sedona TO APPLICATION ROLE " + appRoleName + ";");
        }
        try {
            Map<String, String> argMap = parseArgs(args);
            System.out.println("-- UDFs --");
            System.out.println(String.join("\n", UDFDDLGenerator.buildAll(argMap, stageName, isNativeApp, appRoleName)));
            System.out.println("-- UDTFs --");
            System.out.println(String.join("\n", UDTFDDLGenerator.buildAll(argMap, stageName, isNativeApp, appRoleName)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
