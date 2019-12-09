package pmel.sdig.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

/**
 * Created by rhs on 11/9/17.
 */
public class FastCleanOptions extends Options {
    OptionGroup utilities = new OptionGroup();
    Option root = new Option("r", "root", true, "The root or parent URL.");
    Option dir = new Option("d", "dir", true, "Output directory for clean catalogs.");
    Option verbose = new Option("v", "verbose", false, "Verbose output during processing.");
    Option context = new Option("c", "context", true, "The thredds context name. Typically \"thredds\".");
    Option server = new Option("s", "server", true, "The base URL of the target THREDDS server.");
    Option number = new Option("n", "number", false, "Count the number of data access URLs in the named catalog root.");
    Option write = new Option("w", "write", false, "Write a file with data access URLs and a file with the data access catalogs in the named catalog root.");
    Option test = new Option("t", "test", false, "Test access to the das of every data access URL in the name catalog root.");
    public FastCleanOptions() {
        root.setRequired(true);
        context.setRequired(true);
        server.setRequired(true);
        utilities.addOption(number);
        utilities.addOption(test);
        utilities.addOption(write);
    }
    public void addUtilities() {
        addOption(root);
        addOptionGroup(utilities);
    }
    public void addClean() {
        addOption(root);
        addOption(dir);
        addOption(verbose);
        addOption(context);
        addOption(server);
    }
}
