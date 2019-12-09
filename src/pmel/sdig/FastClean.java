package pmel.sdig;

import dods.dap.DAS;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Parent;
import org.jdom2.filter.ElementFilter;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import pmel.sdig.cleaner.Clean;
import pmel.sdig.cli.FastCleanOptions;
import pmel.sdig.jdo.LeafNodeReference;
import pmel.sdig.jdom.JDOMUtils;
import pmel.sdig.util.DatasetCounter;
import pmel.sdig.util.OutputFiles;
import pmel.sdig.util.SkipManager;
import thredds.catalog.InvAccess;
import thredds.catalog.InvCatalog;
import thredds.catalog.InvCatalogFactory;
import thredds.catalog.InvDataset;
import thredds.catalog.ServiceType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FastClean {

    // Used for .das access testing
    static int success;
    static int failure;
    static List<String> failures = new ArrayList<String>();

    // Used for counting the number of access data sets in each catalog
    static HashMap<String, DatasetCounter> counters = new LinkedHashMap<>();

    // Keep track of data access points
    static HashMap<String, OutputFiles> output = new LinkedHashMap<>();


    static CommandLine commandLine;
    static CommandLine utilityLine;
    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        FastCleanOptions options = new FastCleanOptions();

        List<String> exclude = new ArrayList<>();
        List<String> excludeCatalog = new ArrayList<>();
        Map<String, List<String>> excludeDataset = new HashMap<>();


        String root = "";
        try {

            boolean test = false;
            boolean number = false;
            boolean write = false;
            try {
                options.addUtilities();
                commandLine = parser.parse(options, args);
                test = commandLine.hasOption("t");
                number = commandLine.hasOption("n");
                write = commandLine.hasOption("w");
                root = commandLine.getOptionValue("r");
                if (root != null && root.endsWith(".html")) {
                    root = root.replace(".html", ".xml");
                }
            } catch (ParseException e) {

                try {
                    options = new FastCleanOptions();
                    options.addClean();
                    commandLine = parser.parse(options, args);
                } catch (ParseException e2) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("fastClean", "", options, "", true);
                    System.exit(-1);
                }

            }

            if (!test && !number && !write) {
                root = commandLine.getOptionValue("r");
                String context = commandLine.getOptionValue("c");
                String server = commandLine.getOptionValue("s");
                boolean verbose = commandLine.hasOption("v");

                DateTime sdt = new DateTime();
                DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
                System.out.println("Clean has started... " + fmt.print(sdt));

                SkipManager skipper = new SkipManager();
                InputStream skipFile = skipper.getSkip();
                Document skipdoc = new Document();
                JDOMUtils.XML2JDOM(new InputStreamReader(skipFile), skipdoc);

                Element skip = skipdoc.getRootElement();
                List<Element> regexEls = skip.getChildren("regex");
                for (Iterator rIt = regexEls.iterator(); rIt.hasNext(); ) {
                    Element regex = (Element) rIt.next();
                    String value = regex.getAttributeValue("value");
                    if (value != null) {
                        exclude.add(value);
                    }
                }
                List<Element> catalogEls = skip.getChildren("catalog");
                for (Iterator catIt = catalogEls.iterator(); catIt.hasNext(); ) {
                    Element catE = (Element) catIt.next();
                    String url = catE.getAttributeValue("url");
                    if (url != null) {
                        if (catE.getChildren("dataset").size() == 0) {
                            excludeCatalog.add(url);
                        } else {
                            List<Element> dsE = catE.getChildren("dataset");
                            List<String> exds = new ArrayList<String>();
                            excludeDataset.put(url, exds);
                            for (Iterator dsIt = dsE.iterator(); dsIt.hasNext(); ) {
                                Element dataset = (Element) dsIt.next();
                                String name = dataset.getAttributeValue("name");
                                if (name != null) {
                                    exds.add(name);
                                }
                            }
                        }
                    }
                }

                Clean clean = new Clean(server, context, exclude, excludeCatalog, excludeDataset, verbose, root);
                clean.clean(root);

                removeEmpties("CleanCatalog.xml");
//                removeChildless("CleanCatalog.xml");
//                removeRedundant("CleanCatalog.xml");

                DateTime edt = new DateTime();
                System.out.println("Clean has finished... " + fmt.print(edt));

            } else if (test) {
                if (root != null) {
                    InvCatalogFactory factory = new InvCatalogFactory("default", false);
                    InvCatalog catalog = (InvCatalog) factory.readXML(root);
                    StringBuilder buff = new StringBuilder();

                    if (!catalog.check(buff, true)) {
                        System.err.println("Checking: " + root);
                        System.err.println("Invalid catalog: \n" + buff.toString());
                    } else {

                        List<InvDataset> datasets = catalog.getDatasets();
                        for (int i = 0; i < datasets.size(); i++) {
                            InvDataset dataset = datasets.get(i);
                            processDataset(dataset);
                        }

                    }


                    int total = success + failure;
                    File out = new File("report.txt");
                    FileUtils.write(out, "Found: " + total + "\n", StandardCharsets.UTF_8);
                    FileUtils.write(out, "Sucessfully read: " + success + "\n", StandardCharsets.UTF_8, true);
                    FileUtils.write(out, "Failed to read: " + failure + "\n", StandardCharsets.UTF_8, true);
                    FileUtils.writeLines(out, failures, true);
                } else {
                    System.out.println("Please specify the catalog you want to test with the -r option.");
                }

            } else if (number) {

                if (root != null) {
                    InvCatalogFactory factory = new InvCatalogFactory("default", false);
                    InvCatalog catalog = (InvCatalog) factory.readXML(root);
                    StringBuilder buff = new StringBuilder();

                    if (!catalog.check(buff, true)) {
                        System.err.println("Checking: " + root);
                        System.err.println("Invalid catalog: \n" + buff.toString());
                    } else {

                        List<InvDataset> datasets = catalog.getDatasets();
                        for (int i = 0; i < datasets.size(); i++) {
                            InvDataset dataset = datasets.get(i);
                            countDataset(dataset);
                        }
                    }


                    int total = 0;
                    File out = new File("report.txt");
                    for (Iterator kIt = counters.keySet().iterator(); kIt.hasNext(); ) {
                        String c = (String) kIt.next();
                        DatasetCounter dc = counters.get(c);
                        if (dc.getAcccessChildren() > 0) {
                            FileUtils.write(out, c + "  " + dc.getAcccessChildren() + "\n", StandardCharsets.UTF_8, true);
                        }
                        total = total + dc.getAcccessChildren();
                    }
                    FileUtils.write(out, "Total: " + total + "\n", StandardCharsets.UTF_8, true);
                } else {
                    System.out.println("Please specify the catalog you want to test with the -r option.");
                }
            } else if (write ) {
                if (root != null) {
                    InvCatalogFactory factory = new InvCatalogFactory("default", false);
                    InvCatalog catalog = (InvCatalog) factory.readXML(root);
                    StringBuilder buff = new StringBuilder();

                    if (!catalog.check(buff, true)) {
                        System.err.println("Checking: " + root);
                        System.err.println("Invalid catalog: \n" + buff.toString());
                    } else {

                        List<InvDataset> datasets = catalog.getDatasets();
                        for (int i = 0; i < datasets.size(); i++) {
                            InvDataset dataset = datasets.get(i);
                            writeAccessInformation(dataset);
                        }
                    }


                    int catalogIndex = 0;

                    File access = new File("access");
                    access.mkdirs();
                    File allCats = new File("access/AllCatalogsWithAccess.txt");
                    if ( allCats.exists() ) allCats.delete();
                    for (Iterator kIt = output.keySet().iterator(); kIt.hasNext(); ) {
                        String indexString = String.format("%04d", catalogIndex);
                        File out = new File("access/catalog"+indexString+".txt");
                        if ( out.exists() ) out.delete();
                        String c = (String) kIt.next();
                        OutputFiles dc = output.get(c);
                        if (dc.getAccessChildren().size() > 0) {
                            FileUtils.write(allCats, indexString + ".  " + c + "\n", StandardCharsets.UTF_8, true);
                            List<String> kids = dc.getAccessChildren();
                            for (int i = 0; i < kids.size(); i++) {
                                String url = kids.get(i);
                                FileUtils.write(out, url + "\n", StandardCharsets.UTF_8, true);
                            }
                            catalogIndex++;
                        }
                    }
                } else {
                    System.out.println("Please specify the catalog you want to test with the -r option.");
                }
            }else {
                System.out.println("Make up your mind. We're gonna do one thing: clean a catalog, count the access data sets or test the .das access. Only one of -n or -t allowed.");
            }
        } catch(JDOMException e){
            System.err.println("Error parsing XML:" + e.getLocalizedMessage());
        } catch(IOException e) {
            System.err.println("Error parsing XML:" + e.getLocalizedMessage());
        }
//        } catch(URISyntaxException e){
//            System.err.println("Error removing empty catalogs:" + e.getLocalizedMessage());
//        }
    }
    public static void removeEmpties(String base) throws IOException, JDOMException {
        List<Element> remove = new ArrayList<>();
        List<String> check = new ArrayList<>();
        List<String> files = new ArrayList<>();
        File baseFile = new File(base);
        if ( baseFile.exists() ) {
            Document doc = new Document();
            JDOMUtils.XML2JDOM(baseFile, doc);
            Iterator refIt = doc.getDescendants(new ElementFilter("catalogRef"));
            while (refIt.hasNext()) {
                Element ref = (Element) refIt.next();
                String href = ref.getAttributeValue("href", Clean.xlink);
                if (!href.startsWith("/")) {
                    href = base.substring(0, base.lastIndexOf("/") + 1) + href;
                }
                Document subDoc = new Document();
                File refFile = new File(href);
                if ( refFile.exists() ) {
                    JDOMUtils.XML2JDOM(refFile, subDoc);
                    Element dataset = subDoc.getRootElement().getChild("dataset", Clean.ns);
                    String name = dataset.getAttributeValue("name");
                    if (name.startsWith("This catalog was produced")) {
                        files.add(href);
                        remove.add(ref);
                        System.out.println("Removing for being empty.: " + href);
                    } else {
                        check.add(href);
                    }
                }
            }
            for (int i = 0; i < remove.size(); i++) {
                Element ref = remove.get(i);
                Parent p = ref.getParent();
                boolean b = p.removeContent(ref);
            }
            for (int i = 0; i < files.size(); i++) {
                File removeFile = new File(files.get(i));
                if (removeFile.exists()) {
                    boolean d = removeFile.delete();
                    if (!d) System.out.println("Failed to remove " + removeFile.getAbsolutePath());
                }
            }
            // Re-write the catalog...
            XMLOutputter xout = new XMLOutputter();
            Format format = Format.getPrettyFormat();
            format.setLineSeparator(System.getProperty("line.separator"));
            xout.setFormat(format);
            PrintStream fout = new PrintStream(new File(base));
            xout.output(doc, fout);
            fout.close();
            for (int i = 0; i < check.size(); i++) {
                removeEmpties(check.get(i));
            }
        }
    }
    public static void removeRedundant(String base) throws IOException, JDOMException, URISyntaxException {
        List<Element> remove = new ArrayList<>();
        List<String> check = new ArrayList<>();
        List<String> files = new ArrayList<>();
        Document doc = new Document();
        File baseFile = new File(base);
        JDOMUtils.XML2JDOM(baseFile, doc);
        Iterator refIt = doc.getDescendants(new ElementFilter("catalogRef"));
        String docname = doc.getRootElement().getAttributeValue("name");
        if ( docname == null ) {
            docname = doc.getRootElement().getAttributeValue("title", Clean.xlink);
        }
        while (refIt.hasNext()) {
            Element ref = (Element) refIt.next();
            String name = ref.getAttributeValue("title", Clean.xlink);
            String href = ref.getAttributeValue("href", Clean.xlink);
            if ( name.equals(docname)) {
                System.out.println("want to get rid of "+ name + " element " + ref.toString() + " which is a child of "+docname + " element " + doc.getRootElement().toString());
            }
            File hrefFile = new File(href);
            if ( hrefFile.exists() ) {
                Document subDoc = new Document();
                JDOMUtils.XML2JDOM(hrefFile, subDoc);
                Iterator subRefs = subDoc.getDescendants(new ElementFilter("catalogRef"));
                int subRefCount = 0;
                while (subRefs.hasNext()) {
                    subRefCount++;
                    subRefs.next();
                }
                // Check for access datasets

                List<LeafNodeReference> leaves = new ArrayList<>();
                leaves = Clean.findAccessDatasets(base, subDoc, leaves);
                int subDsCount = leaves.size();
                if ( subDsCount == 0 && name.equals(docname) ) {
                    files.add(href);
                    remove.add(ref);
                    System.out.println("Removing for being same name with nothing else in it.: " + href);
                } else {
                    check.add(href);
                }
            }
        }
//        for (int i = 0; i < remove.size(); i++) {
//            Element ref = remove.get(i);
//            Parent p = ref.getParent();
//            boolean b = p.removeContent(ref);
//        }
//        for (int i = 0; i < files.size(); i++) {
//            File file = new File(files.get(i));
//            if (file.exists()) {
//                boolean d = file.delete();
//                if (!d) System.out.println("Failed to remove " + file.getAbsolutePath());
//            }
//        }
//        // Re-write the catalog...
//        XMLOutputter xout = new XMLOutputter();
//        Format format = Format.getPrettyFormat();
//        format.setLineSeparator(System.getProperty("line.separator"));
//        xout.setFormat(format);
//        PrintStream fout = new PrintStream(new File(base));
//        xout.output(doc, fout);
//        fout.close();
        for (int i = 0; i < check.size(); i++) {
            removeRedundant(check.get(i));
        }
    }
    public static void removeChildless(String base) throws IOException, JDOMException, URISyntaxException {
        List<Element> remove = new ArrayList<>();
        List<String> check = new ArrayList<>();
        List<String> files = new ArrayList<>();
        Document doc = new Document();
        File baseFile = new File(base);
        if ( baseFile.exists() ) {
            JDOMUtils.XML2JDOM(baseFile, doc);
            Iterator refIt = doc.getDescendants(new ElementFilter("catalogRef"));
            while (refIt.hasNext()) {
                Element ref = (Element) refIt.next();
                String href = ref.getAttributeValue("href", Clean.xlink);
                if (!href.startsWith("/")) {
                    href = base.substring(0, base.lastIndexOf("/") + 1) + href;
                }
                File hrefFile = new File(href);
                if ( hrefFile.exists() ) {
                    Document subDoc = new Document();
                    JDOMUtils.XML2JDOM(hrefFile, subDoc);
                    Iterator subRefs = subDoc.getDescendants(new ElementFilter("catalogRef"));
                    int subRefCount = 0;
                    while (subRefs.hasNext()) {
                        subRefCount++;
                        subRefs.next();
                    }
                    // Check for access datasets

                    List<LeafNodeReference> leaves = new ArrayList<>();
                    leaves = Clean.findAccessDatasets(base, subDoc, leaves);
                    int subDsCount = leaves.size();
                    if (subRefCount == 0 && subDsCount == 0) {
                        files.add(href);
                        remove.add(ref);
                        System.out.println("Removing for being childless.: " + href);
                    } else {
                        check.add(href);
                    }
                }
            }
            for (int i = 0; i < remove.size(); i++) {
                Element ref = remove.get(i);
                Parent p = ref.getParent();
                boolean b = p.removeContent(ref);
            }
            for (int i = 0; i < files.size(); i++) {
                File file = new File(files.get(i));
                if (file.exists()) {
                    boolean d = file.delete();
                    if (!d) System.out.println("Failed to remove " + file.getAbsolutePath());
                }
            }
            // Re-write the catalog...
            XMLOutputter xout = new XMLOutputter();
            Format format = Format.getPrettyFormat();
            format.setLineSeparator(System.getProperty("line.separator"));
            xout.setFormat(format);
            PrintStream fout = new PrintStream(new File(base));
            xout.output(doc, fout);
            fout.close();
            for (int i = 0; i < check.size(); i++) {
                removeChildless(check.get(i));
            }
        }
    }
    private static void processDataset(InvDataset dataset) {
        if ( dataset.hasAccess() ) {
            InvAccess access = dataset.getAccess(ServiceType.OPENDAP);
            if ( access != null ) {
                DAS das = new DAS();
                String dsu = "";
                try {
                    dsu = access.getStandardUrlName() + ".das";
                    InputStream input = new URL(dsu).openStream();
                    das.parse(input);
                    success++;
                } catch ( Exception e ) {
                    failure++;
                    failures.add(dsu + " exception " + e.getLocalizedMessage());
                }
            }
        }
        if ( dataset.hasNestedDatasets() ) {
            List<InvDataset> children = dataset.getDatasets();
            for (int i = 0; i < children.size(); i++) {
                InvDataset child = children.get(i);
                processDataset(child);
            }
        }
    }
    private static void countDataset(InvDataset dataset) {
        DatasetCounter count = null;
        if ( !dataset.getName().toLowerCase().contains("rubric")) {
            InvDataset parent = dataset.getParent();
            InvCatalog parentCatalog = dataset.getParentCatalog();

            if (parent != null) {
                InvDataset grandparent = parent.getParent();
                String catalogURL = parent.getParentCatalog().getUriString();
                if ( catalogURL.contains("#") ) {
                    catalogURL = catalogURL.substring(0, catalogURL.indexOf("#"));
                }
                count = counters.get(catalogURL);
                if (count == null) {
                    count = new DatasetCounter();
                    String pCat = parent.getCatalogUrl();
                    if ( pCat.contains("#") ) {
                        pCat = pCat.substring(0, pCat.indexOf("#"));
                    }
                    counters.put(pCat, count);
                }
                if (grandparent != null) {
                    String gParent = grandparent.getParentCatalog().getUriString();
                    if ( gParent.contains("#") ) {
                        gParent = gParent.substring(0, gParent.indexOf("#"));
                    }
                    DatasetCounter parentCount = counters.get(gParent);
                    if (parentCount == null) {
                        parentCount = new DatasetCounter();
                        parentCount.setDepth(0);
                    }
                    count.setDepth(parentCount.getDepth() + 1);
                } else {
                    count.setDepth(0);
                }
            } else if ( parentCatalog != null ) {
                String pCatalog = parentCatalog.getUriString();
                if ( pCatalog.contains("#") ) {
                    pCatalog = pCatalog.substring(0, pCatalog.indexOf("#"));
                }
                count = new DatasetCounter();
                count.setDepth(0);
                counters.put(pCatalog, count);
            }
            if (dataset.hasAccess()) {
                InvAccess access = dataset.getAccess(ServiceType.OPENDAP);
                if (access != null) {
                    count.addAccessChild();
                }
            }
        }
        if ( dataset.hasNestedDatasets() ) {
            List<InvDataset> children = dataset.getDatasets();
            for (int i = 0; i < children.size(); i++) {
                InvDataset child = children.get(i);
                countDataset(child);
            }
        }
        if ( !dataset.hasNestedDatasets() && !dataset.hasAccess() ) {
            System.err.println("Date set " + dataset.getName() + " " + dataset.getCatalogUrl() + " has neither access nor child datasets.");
        }
    }
    private static void writeAccessInformation(InvDataset dataset) {
        OutputFiles count = null;
        if ( !dataset.getName().toLowerCase().contains("rubric")) {
            InvDataset parent = dataset.getParent();
            InvCatalog parentCatalog = dataset.getParentCatalog();

            if (parent != null) {
                InvDataset grandparent = parent.getParent();
                String catalogURL = parent.getParentCatalog().getUriString();
                if ( catalogURL.contains("#") ) {
                    catalogURL = catalogURL.substring(0, catalogURL.indexOf("#"));
                }
                count = output.get(catalogURL);
                if (count == null) {
                    count = new OutputFiles(catalogURL);
                    String pCat = parent.getCatalogUrl();
                    if ( pCat.contains("#") ) {
                        pCat = pCat.substring(0, pCat.indexOf("#"));
                    }
                    output.put(pCat, count);
                }
                if (grandparent != null) {
                    String gParent = grandparent.getParentCatalog().getUriString();
                    if ( gParent.contains("#") ) {
                        gParent = gParent.substring(0, gParent.indexOf("#"));
                    }
                    DatasetCounter parentCount = counters.get(gParent);
                    if (parentCount == null) {
                        parentCount = new DatasetCounter();
                        parentCount.setDepth(0);
                    }

                }
            } else if ( parentCatalog != null ) {
                String pCatalog = parentCatalog.getUriString();
                if ( pCatalog.contains("#") ) {
                    pCatalog = pCatalog.substring(0, pCatalog.indexOf("#"));
                }
                count = new OutputFiles(pCatalog);
                output.put(pCatalog, count);
            }
            if (dataset.hasAccess()) {
                InvAccess access = dataset.getAccess(ServiceType.OPENDAP);
                if (access != null) {
                    count.addAccessChild(access.getStandardUrlName());
                }
            }
        }
        if ( dataset.hasNestedDatasets() ) {
            List<InvDataset> children = dataset.getDatasets();
            for (int i = 0; i < children.size(); i++) {
                InvDataset child = children.get(i);
                writeAccessInformation(child);
            }
        }
    }
}
