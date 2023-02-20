package cn.pockethub.permanentqueue.kafka.utils;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

/**
 * Helper functions for dealing with command line utilities
 */
public class CommandLineUtils {
    /**
     * Check that all the listed options are present
     */
    public static void checkRequiredArgs(final OptionParser parser, final OptionSet options, final OptionSpec... required) {
        for (final OptionSpec arg : required) {
            if (!options.has(arg)) {
                printUsageAndDie(parser, "Missing required argument \"" + arg + "\"");
            }
        }
    }

    /**
     * Print usage and exit
     */
    public static void printUsageAndDie(final OptionParser parser, final String message) {
        System.err.println(message);
        try {
            parser.printHelpOn(System.err);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(1);
    }
}
