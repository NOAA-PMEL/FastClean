package pmel.sdig.cleaner;



import com.fasterxml.jackson.databind.deser.DataFormatReaders;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.Content;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.Parent;
import org.jdom2.filter.ElementFilter;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.util.IteratorIterable;
import org.joda.time.DateTime;
import pmel.sdig.jdo.LeafNodeReference;
import pmel.sdig.jdom.DatasetNameFilter;
import pmel.sdig.jdom.JDOMUtils;
import pmel.sdig.jdom.UrlPathFilter;
import pmel.sdig.util.Util;
import thredds.catalog.InvAccess;
import thredds.catalog.InvCatalog;
import thredds.catalog.InvCatalogFactory;
import thredds.catalog.InvDataset;
import thredds.catalog.ServiceType;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rhs on 11/9/17.
 */
public class Clean {


    boolean verbose;

    private static String viewer_0 = "http://ferret.pmel.noaa.gov/uaf/las/getUI.do?data_url=";
    private static String viewer_0_description = ", Visualize with Live Access Server";

    private static String viewer_1 = "https://upwell.pfeg.noaa.gov/erddap/search/index.html?searchFor=";
    private static String viewer_1_description = ", Visualize with ERDDAP";

    private static String viewer_2 = "http://www.ncdc.noaa.gov/oa/wct/wct-jnlp.php?singlefile=";
    private static String viewer_2_description = ", Weather and Climate Toolkit";


    public static Namespace ns = Namespace.getNamespace("http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0");
    private static Namespace netcdfns = Namespace.getNamespace("http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2");
    public static Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");

    String threddsServerName = "thredds_server_name";
    String threddsContext = "thredds_context";

    static boolean hasBestTimeSeries = false;

    static List<String> exclude = new ArrayList<>();
    static List<String> excludeCatalog = new ArrayList<>();
    Map<String, List<String>> excludeDataset = new HashMap<>();

    String root = "";
    // These are pretty much the defaults, but are here so they can be adjusted as necessary.
    OkHttpClient client = new OkHttpClient.Builder()
                              .connectTimeout(10, TimeUnit.SECONDS)
                              .writeTimeout(10, TimeUnit.SECONDS)
                              .readTimeout(30, TimeUnit.SECONDS)
                              .build();

    public Clean(String threddsServerName, String threddsContext, List<String> exclude, List<String> excludeCatalog, Map<String, List<String>> excludeDataset, boolean verbose, String root) {
        this.verbose = verbose;
        this.threddsServerName = threddsServerName;
        this.threddsContext = threddsContext;
        this.exclude = exclude;
        this.excludeCatalog = excludeCatalog;
        this.excludeDataset = excludeDataset;
        this.root = root;
    }

    public void clean(String url) {

        if ( verbose ) System.out.println("Downloading "+url);
        Request request = new Request.Builder()
                .url(url)
                .build();

        List<String> segs = request.url().pathSegments();
        for (int i = 0; i < segs.size(); i++) {
            if ( segs.get(i).startsWith(".") ) {
                return; // It's a hidden file so don't want it
            }
        }

        String xml = null;
        try {
            Response response = client.newCall(request).execute();
            xml = response.body().string();
        } catch (IOException e) {
            try {
                writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
            } catch (IOException e1) {
                // Oh, wel.
            }
            e.printStackTrace();
            return;
        }

        // Prepare an XML document of the catalog.
        Document docWithCatalogRefs = new Document();

        if ( xml.length() == 0 ) return;

        try {
            JDOMUtils.XML2JDOM(xml, docWithCatalogRefs);
        } catch (Exception e) {
            try {
                writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
            } catch (IOException e1) {
                // Oh, wel.
            }
            return;
        }

        Document docWithoutRefsForDatasetSearch = docWithCatalogRefs.clone();

        List<Element> refs = new ArrayList<>();
        List<String> childRefs = new ArrayList<>();



        Iterator removeIt = docWithoutRefsForDatasetSearch.getDescendants(new ElementFilter("catalogRef"));
        Set<Parent> parents = new HashSet<Parent>();

        while ( removeIt.hasNext() ) {
            Element ref = (Element) removeIt.next();
            String orginalUrl = ref.getAttributeValue("href", xlink);
            String fullurl = null;
            try {
                fullurl = Util.getUrl(url, orginalUrl, "thredds");
            } catch (URISyntaxException e) {
                System.err.println("Expecption handling " + url + " Message: " + e.getMessage());
                e.printStackTrace();
                return;
            }
            childRefs.add(fullurl);
            parents.add(ref.getParent());

        }
        for ( Iterator parentIt = parents.iterator(); parentIt.hasNext(); ) {
            Parent parent = (Parent) parentIt.next();
            parent.removeContent(new ElementFilter("catalogRef"));
        }

        Iterator refIt = docWithCatalogRefs.getDescendants(new ElementFilter("catalogRef"));
        while ( refIt.hasNext() ) {
            Element ref = (Element) refIt.next();
            refs.add(ref);
        }

        List<LeafNodeReference> leaves = new ArrayList<>();
        try {
            hasBestTimeSeries = false;
            leaves = findAccessDatasets(url, docWithoutRefsForDatasetSearch, leaves);
            // Remove child refs for best time series catalog...
            if (hasBestTimeSeries) {
                remove(docWithCatalogRefs, "catalogRef");
                remove2D(docWithCatalogRefs);
            }
        } catch (URISyntaxException e) {
            try {
                writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
            } catch (IOException e1) {
                // Oh, wel.
            }
            e.printStackTrace();
            return;
        }

        if ( leaves.size() > 0 ) {
            if (appearsUnaggregated(leaves)) {
                if ( verbose ) System.out.println("Catalog " + url + " appears unaggregated.");
                try {
                    writeAppearsUnaggregated(url);
                } catch (IOException e) {
                    // Oh, well.
                }
                return;
            }

            if ( appearsJunk(leaves) ) {
                try {
                    writeAppearsJunk(url);
                } catch (IOException e) {
                    // Oh, well.
                }
                return;
            }

            // This is a huge risk.  Use the first one, we'll see what happens...
            String path = leaves.get(0).getUrlPath();
            String lurl = leaves.get(0).getUrl();
            String remoteBase = lurl.replace(path, "");
            // Remove the services
            Set<String> removed = removeRemoteServices(docWithCatalogRefs);
            addLocalServices(docWithCatalogRefs, remoteBase, removed);

            // Remove child refs for best time series catalog...
//            if (hasBestTimeSeries) {
//                remove(docWithCatalogRefs, "catalogRef");
//                remove2D(docWithCatalogRefs);
//            }

            for (int i = 0; i < leaves.size(); i++) {
                LeafNodeReference leaf = leaves.get(i);
                try {
                    addNCML(docWithCatalogRefs, "", leaf);
                } catch (MalformedURLException e) {
                    try {
                        writeError(url, "An error occurred processing " + url + "  Message: " + e.getMessage());
                    } catch (IOException e1) {
                        // Oh, wel.
                    }
                    e.printStackTrace();
                    return;
                }

            }
            // For removing uncrawled data sets, so we'll skip for now
//            removeEmptyLeaves(doc.getRootElement(), leaves);
        }
        if ( url.equals(root)) {
            // There won't be any data sets in the root catalog, but the local services have to be edited because of the URL re-writing in the firewall.
            // All of them have to be fixed, so send in an empty list of existing services.
            addLocalServices(docWithCatalogRefs, threddsServerName, new HashSet<String>());


        }
        if ( childRefs.size() == 0 && leaves.size() == 0 ) {
            try {
                writeEmptyCatalog(url);
            } catch (IOException e) {
                // Bummer.
            }
            return;
        }

        if ( excludeDataset.keySet().contains(url)) {
            List<String> datasets = excludeDataset.get(url);
            System.out.println("Excluding listed datasets.");
            removeExcludedDatasets(docWithCatalogRefs.getRootElement(), datasets);
        }

        try {

            updateCatalogReferences(docWithCatalogRefs.getRootElement(), url, refs);

        } catch (MalformedURLException e) {
            try {
                writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
            } catch (IOException e1) {
                // Oh, wel.
            }
            e.printStackTrace();
            return;
        } catch (URISyntaxException e) {
            try {
                writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
            } catch (IOException e1) {
                // Oh, wel.
            }
            e.printStackTrace();
            return;
        }

        XMLOutputter xout = new XMLOutputter();
        Format format = Format.getPrettyFormat();
        format.setLineSeparator(System.getProperty("line.separator"));
        xout.setFormat(format);
        PrintStream fout;

        if ( url.equals(root) ) {
            try {
                fout = new PrintStream("CleanCatalog.xml");
            } catch (FileNotFoundException e) {
                try {
                    writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
                } catch (IOException e1) {
                    // Oh, wel.
                }
                e.printStackTrace();
                return;
            }
        } else {
            String base = null;
            try {
                base = getFileBase(url);
            } catch (MalformedURLException e) {
                try {
                    writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
                } catch (IOException e1) {
                    // Oh, wel.
                }
                e.printStackTrace();
                return;
            }
            File ffile = new File(base);
            ffile.mkdirs();
            File outFile = null;
            try {
                outFile = new File(ffile.getPath()+File.separator+getFileName(url));
            } catch (MalformedURLException e) {
                try {
                    writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
                } catch (IOException e1) {
                    // Oh, wel.
                }
                e.printStackTrace();
                return;
            }
            try {
                fout = new PrintStream(outFile);
            } catch (FileNotFoundException e) {
                try {
                    writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
                } catch (IOException e1) {
                    // Oh, wel.
                }
                e.printStackTrace();
                return;
            }
            if ( verbose ) System.out.println("Writing "+url+" \n\t\tto "+outFile.getAbsolutePath());
        }


        Element tsp = new Element("property", ns);
        tsp.setAttribute("name", "CatalogCleanerTimeStamp");
        tsp.setAttribute("value", "Catalog generated by the TMAP Catalog Cleaner (FastClean v1.0) "+ DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        docWithCatalogRefs.getRootElement().addContent(tsp);

        Namespace parentNamespace = docWithCatalogRefs.getRootElement().getNamespace();
        Element rubricDataset = new Element("dataset", parentNamespace);
        rubricDataset.setAttribute("name", "TDS Quality Rubric");
        rubricDataset.setAttribute("ID", "TDS quality rubric");

        Element summaryDoc = new Element("documentation", parentNamespace);
        summaryDoc.setAttribute("type", "summary");
        summaryDoc.setText("The below is a link to a web page which will display statistics about the quality of the TDS catalog. The quality metrics are \"% of datasets that are valid\", \"% of qualified datasets that are aggregated\", and \"% of available services\". Datasets/Data are considered \"valid\" if they conform to the netcdf/Java Common Data Model, and if they are compliant with the Climate and Forecast Metadata conventions. The TDS access services required are: OPeNDAP, WCS, WMS and the NcISO services of UDDC, NCML, ISO.");

        Element rubricLink = new Element("documentation", parentNamespace);
        rubricLink.setAttribute("href", "http://ferret.pmel.noaa.gov/uaf/CE/?url="+url, xlink);
        rubricLink.setAttribute("title", "UAF Quality Rubric for this catalog.", xlink);

        rubricDataset.addContent(summaryDoc);
        rubricDataset.addContent(rubricLink);

        docWithCatalogRefs.getRootElement().addContent(rubricDataset);

        try {
            xout.output(docWithCatalogRefs, fout);
        } catch (IOException e) {
            try {
                writeError(url, "An error occurred processing " + url + " Message: " + e.getMessage());
            } catch (IOException e1) {
                // Oh, wel.
            }
            e.printStackTrace();
            return;
        }
        fout.close();

        for (int i = 0; i < childRefs.size(); i++) {
            if ( !excludeCatalog.contains(childRefs.get(i)) ) {
                clean(childRefs.get(i));
            } else {
                System.err.println("Skipping ... " + childRefs.get(i));
            }
        }

    }
    private void writeDebug(Document doc) {
        XMLOutputter xout = new XMLOutputter();
        Format format = Format.getPrettyFormat();
        format.setLineSeparator(System.getProperty("line.separator"));
        xout.setFormat(format);
        try {
            xout.output(doc, new OutputStreamWriter(System.out));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void removeExcludedDatasets(Element rootElement, List<String> datasets) {
        // TODO Can't use a hash set since they hav the same parent.
        // TODO two array lists...
        List<Parent> removeParent = new ArrayList<Parent>();
        List<Element> removeChild = new ArrayList<Element>();
        for ( Iterator rdIt = datasets.iterator(); rdIt.hasNext(); ) {
            String removeName = (String) rdIt.next();
            if ( verbose) System.out.println("Looking for "+removeName+" for removal.");

            Iterator removeDes = rootElement.getDescendants(new DatasetNameFilter(removeName));
            while ( removeDes.hasNext() ) {
                Element removeE = (Element) removeDes.next();
                if ( verbose ) System.out.println("Scheduling "+removeE.getAttributeValue("name")+" for removal.");
                Parent p = removeE.getParent();
                removeParent.add(p);
                removeChild.add(removeE);
            }
        }
        int i = 0;
        for ( Iterator removeIt = removeParent.iterator(); removeIt.hasNext(); ) {
            Parent parent = (Parent) removeIt.next();
            Element element = removeChild.get(i);
            if ( verbose ) System.out.println("Removing "+element.getAttributeValue("name"));
            parent.removeContent(element);
            i++;
        }
    }
    private void writeError(String url, String message) throws IOException {
        Document doc = new Document();
        Element catalogE = new Element("catalog");
        catalogE.setAttribute("name", "Error catalog for " + url);
        catalogE.addNamespaceDeclaration(xlink);
        //<dataset name="This folder is produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being cleaned contained no datasets that met the UAF standards.  That catalog may be accessed directly at http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html." urlPath="http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html" />
        Element dataset = new Element("dataset", ns);
        dataset.setAttribute("name", message  + " The catalog may be accessed directly at " + url + ".");
        dataset.setAttribute("urlPath", url);
        catalogE.addContent(dataset);
        doc.addContent(catalogE);
        XMLOutputter xout = new XMLOutputter();
        Format format = Format.getPrettyFormat();
        format.setLineSeparator(System.getProperty("line.separator"));
        xout.setFormat(format);
        PrintStream fout;
        URL catalogURL = new URL(url);
        File ffile = new File("CleanCatalogs" + File.separator + catalogURL.getHost() + File.separator + catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/")));
        ffile.mkdirs();
        fout = new PrintStream(ffile.getPath() + File.separator + catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/")));
        xout.output(doc, fout);
        fout.close();
        System.out.println("Exception handling " + url + " Message: " + message);
    }

    private void writeAppearsUnaggregated(String url) throws IOException {
        Document doc = new Document();
        Element catalogE = new Element("catalog");
        catalogE.setAttribute("name", "Empty catalog for " + url);
        catalogE.addNamespaceDeclaration(xlink);
        //<dataset name="This folder is produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being cleaned contained no datasets that met the UAF standards.  That catalog may be accessed directly at http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html." urlPath="http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html" />
        Element dataset = new Element("dataset", ns);
        dataset.setAttribute("name", "This catalog was produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being evaluated contains many data sets that appear to not be aggregated. Un-aggregated data sets do not meet the UAF standards.  That catalog may be accessed directly at " + url + ".");
        dataset.setAttribute("urlPath", url);
        catalogE.addContent(dataset);
        doc.addContent(catalogE);
        XMLOutputter xout = new XMLOutputter();
        Format format = Format.getPrettyFormat();
        format.setLineSeparator(System.getProperty("line.separator"));
        xout.setFormat(format);
        PrintStream fout;
        URL catalogURL = new URL(url);
        File ffile = new File("CleanCatalogs" + File.separator + catalogURL.getHost() + File.separator + catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/")));
        ffile.mkdirs();
        fout = new PrintStream(ffile.getPath() + File.separator + catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/")));
        xout.output(doc, fout);
        fout.close();
    }
    private void writeAppearsJunk(String url) throws IOException {
        Document doc = new Document();
        Element catalogE = new Element("catalog");
        catalogE.setAttribute("name", "Empty catalog for " + url);
        catalogE.addNamespaceDeclaration(xlink);
        //<dataset name="This folder is produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being cleaned contained no datasets that met the UAF standards.  That catalog may be accessed directly at http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html." urlPath="http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html" />
        Element dataset = new Element("dataset", ns);
        dataset.setAttribute("name", "This catalog was produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being evaluated contains many data sets that appear to be something other than netCDF API OPeNDAP accessible data.  That catalog may be accessed directly at " + url + ".");
        dataset.setAttribute("urlPath", url);
        catalogE.addContent(dataset);
        doc.addContent(catalogE);
        XMLOutputter xout = new XMLOutputter();
        Format format = Format.getPrettyFormat();
        format.setLineSeparator(System.getProperty("line.separator"));
        xout.setFormat(format);
        PrintStream fout;
        URL catalogURL = new URL(url);
        File ffile = new File("CleanCatalogs" + File.separator + catalogURL.getHost() + File.separator + catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/")));
        ffile.mkdirs();
        fout = new PrintStream(ffile.getPath() + File.separator + catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/")));
        xout.output(doc, fout);
        fout.close();
    }
    private void writeEmptyCatalog(String url) throws IOException {
        Document doc = new Document();
        Element catalogE = new Element("catalog");
        catalogE.setAttribute("name", "Empty catalog for " + url);
        catalogE.addNamespaceDeclaration(xlink);
        //<dataset name="This folder is produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being cleaned contained no datasets that met the UAF standards.  That catalog may be accessed directly at http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html." urlPath="http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html" />
        Element dataset = new Element("dataset", ns);
        dataset.setAttribute("name", "This catalog was produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being cleaned contained no datasets that met the UAF standards.  That catalog may be accessed directly at " + url + ".");
        dataset.setAttribute("urlPath", url);
        catalogE.addContent(dataset);
        doc.addContent(catalogE);
        XMLOutputter xout = new XMLOutputter();
        Format format = Format.getPrettyFormat();
        format.setLineSeparator(System.getProperty("line.separator"));
        xout.setFormat(format);
        PrintStream fout;
        URL catalogURL = new URL(url);
        File ffile = new File("CleanCatalogs" + File.separator + catalogURL.getHost() + File.separator + catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/")));
        ffile.mkdirs();
        fout = new PrintStream(ffile.getPath() + File.separator + catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/")));
        xout.output(doc, fout);
        fout.close();
    }
    public static String getFileName(String url) throws MalformedURLException {
        URL catalogURL = new URL(url);
        return catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/"));
    }
    public static String getFileBase(String url) throws MalformedURLException {
        URL catalogURL = new URL(url);
        return "CleanCatalogs"+File.separator+catalogURL.getHost()+catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/"));
    }
    private void updateCatalogReferences(Element element, String parent, List<Element> refs) throws MalformedURLException, URISyntaxException {
        List<Element> children = element.getChildren();
        List<Element> remove = new ArrayList<Element>();
        for ( Iterator refIt = children.iterator(); refIt.hasNext(); ) {
            Element child = (Element) refIt.next();
            if ( child.getName().equals("catalogRef") ) {
                boolean convert = true;
                String href = child.getAttributeValue("href", xlink);
                for ( int i = 0; i < exclude.size(); i++ ) {
                    if ( Pattern.matches(exclude.get(i), href)) {
                        remove.add(child);
                        convert = false;
                    }
                }
                if ( excludeCatalog.contains(href) ) {
                    remove.add(child);
                    convert=false;
                }

                if ( convert ) {
                    // If the relative href is the list of member catalogs, mark it as having been converted.
                    boolean converted = false;
                    for ( Iterator refsIt = refs.iterator(); refsIt.hasNext(); ) {
                        Element e = (Element) refsIt.next();
                        String reference = e.getAttributeValue("href", xlink);
                        // If it was in here it wasn't cleaned so it must be removed from the parent.
                        if ( !excludeCatalog.contains(reference) ) {
                            if ( reference.equals(href) ) {
                                converted = true;
                                if (href.startsWith("http")) {
                                    URL catalogURL = new URL(reference);
                                    if ( excludeCatalog.contains(catalogURL) ) {
                                        converted = false;
                                    } else {
                                        String dir = "CleanCatalogs" + File.separator + catalogURL.getHost() + catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/")) + catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/"));
                                        child.setAttribute("href", dir, xlink);
                                    }
                                } else if (href.startsWith("/") ) {
                                    String curl = Util.getUrl(parent, reference, "thredds");
                                    if ( excludeCatalog.contains(curl) ) {
                                        converted = false;
                                    } else {
                                        URL catalogURL = new URL(curl);
                                        // It has to point to the file location relative to the current directory since we can't form the URL because of the redirect in the context of the F5.
                                        String dir = "/CleanCatalogs" + File.separator + catalogURL.getHost() + catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/")) + catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/"));
                                        String full = dots(parent) + dir;
                                        child.setAttribute("href", full, xlink);
                                        // child.setAttribute("href", "/"+threddsContext+"/"+dir, xlink);
                                    }
                                } else {
                                    URL parentURL = new URL(parent);
                                    String curl = Util.getUrl(parent, reference, "thredds");
                                    if ( excludeCatalog.contains(curl) ) {
                                        converted = false;
                                    } else {
                                        URL catalogURL = new URL(curl);
                                        String pfile = "CleanCatalogs" + File.separator + parentURL.getHost() + parentURL.getPath().substring(0, parentURL.getPath().lastIndexOf('/') + 1);
                                        String cfile = "CleanCatalogs" + File.separator + catalogURL.getHost() + catalogURL.getPath();
                                        String ref = cfile.replace(pfile, "");
                                        child.setAttribute("href", ref, xlink);
                                    }
                                }
                            }
                        }
                    }

                    // If it wasn't converted it must have been excluded so removed it.
                    if ( !converted ) {
                        remove.add(child);
                    }
                }
            }
            updateCatalogReferences(child, parent, refs);
        }
        element.getChildren().removeAll(remove);
    }
    private void removeEmptyLeaves(Element rootElement, List<LeafNodeReference> leaves) {
        Element matchingDataset = null;
        for ( Iterator iterator = leaves.iterator(); iterator.hasNext(); ) {
            LeafNodeReference leafNode = (LeafNodeReference) iterator.next();
            IteratorIterable datasetIt = rootElement.getDescendants(new UrlPathFilter(leafNode.getUrlPath()));
            int index = 0;
            Parent p = null;
            while (datasetIt.hasNext() ) {
                if ( index == 0 ) {
                    Element dataset = (Element) datasetIt.next();
                    matchingDataset = dataset;
                    p = matchingDataset.getParent();
                }
                index++;
            }
            if ( matchingDataset != null ) {
                p.removeContent(matchingDataset);
            }
        }
    }
    //    private void writeEmptyCatalog(String parent, String url) throws IOException {
//        Document doc = new Document();
//        Element catalogE = new Element("catalog");
//        catalogE.setAttribute("name", "Empty catalog for "+url);
//        catalogE.addNamespaceDeclaration(xlink);
//        //<dataset name="This folder is produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being cleaned contained no datasets that met the UAF standards.  That catalog may be accessed directly at http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html." urlPath="http://sdf.ndbc.noaa.gov:8080/thredds/catalog/glider/catalog.html" />
//        Element dataset = new Element("dataset", ns);
//        dataset.setAttribute("name", "This catalog was produced through an automated cleaning process as part of the Unified Access framework (UAF) project (http://geo-ide.noaa.gov/).  The catalog being cleaned contained no datasets that met the UAF standards.  That catalog may be accessed directly at "+url+".");
//        dataset.setAttribute("urlPath", url);
//        catalogE.addContent(dataset);
//        doc.addContent(catalogE);
//        XMLOutputter xout = new XMLOutputter();
//        Format format = Format.getPrettyFormat();
//        format.setLineSeparator(System.getProperty("line.separator"));
//        xout.setFormat(format);
//        PrintStream fout;
//        URL catalogURL = new URL(url);
//        File ffile = new File("CleanCatalogs"+File.separator+catalogURL.getHost()+File.separator+catalogURL.getPath().substring(0, catalogURL.getPath().lastIndexOf("/")));
//        ffile.mkdirs();
//        fout = new PrintStream(ffile.getPath()+File.separator+catalogURL.getPath().substring(catalogURL.getPath().lastIndexOf("/")));
//        xout.output(doc, fout);
//        fout.close();
//    }
    private String dots(String dir) {
        if ( dir.startsWith("https://") ) {
            dir = dir.replace("https:/", "");
            dir = dir.substring(0, dir.lastIndexOf("/"));
        } else if ( dir.startsWith("http://") ) {
            dir = dir.replace("http:/", "");
            dir = dir.substring(0, dir.lastIndexOf("/"));
        }
        String dot = "..";
        int ddots = StringUtils.countMatches(dir, "..");
        int count  = StringUtils.countMatches(dir, '/');
        count = count - (ddots*2);
        for (int i = 0; i < count; i++ ) {
            dot = dot + "/..";
        }
        return dot;
    }
    private List<Element> remove(Document doc, String element) {
        List<Element> removed = new ArrayList<Element>();
        Iterator removeIt = doc.getDescendants(new ElementFilter(element));
        Set<Parent> parents = new HashSet<Parent>();
        while ( removeIt.hasNext() ) {
            Element ref = (Element) removeIt.next();
            removed.add(ref);
            parents.add(ref.getParent());
        }
        for ( Iterator parentIt = parents.iterator(); parentIt.hasNext(); ) {
            Parent parent = (Parent) parentIt.next();
            parent.removeContent(new ElementFilter(element));
        }
        return removed;
    }
    private List<Element> remove2D (Document doc) {
        List<Element> removed = new ArrayList<Element>();
        List<Parent> parents = new ArrayList<>();
        Iterator removeIt = doc.getDescendants(new ElementFilter("dataset"));
        while ( removeIt.hasNext() ) {
            Element ref = (Element) removeIt.next();
            if (ref.getAttributeValue("name").equals("Forecast Model Run Collection (2D time coordinates)")) {
                removed.add(ref);
                parents.add(ref.getParent());
            }
        }
        for (int i = 0; i < parents.size(); i++) {
            Parent parent = parents.get(i);
            parent.removeContent(removed.get(i));
        }
        return removed;
    }
    private void addNCML(Document doc, String parent, LeafNodeReference leafNodeReference) throws MalformedURLException {

        Element ncml = new Element("netcdf", netcdfns);

        ncml.setAttribute("location", leafNodeReference.getUrl());

        List<Element> properties = new ArrayList<Element>();




        Element service = new Element("serviceName", ns);
        service.addContent( threddsServerName+"_compound");

        Element matchingDataset = null;
        IteratorIterable datasetIt = doc.getRootElement().getDescendants(new UrlPathFilter(leafNodeReference.getUrlPath()));
        int index = 0;
        Parent p = null;
        boolean found = false;
        while (datasetIt.hasNext() ) {
            Element dataset = (Element) datasetIt.next();
            if ( !found ) {
                if ( dataset.getName().equals("dataset") ) {
                    found = true;
                    matchingDataset = dataset;
                    p = matchingDataset.getParent();
                }
            }
        }

        if ( matchingDataset == null ) {
            System.err.println("No matching data set found for " + leafNodeReference.getUrl());
        }
        // We're only going to add the location ncml to point to the data set if it's not on the skip
        // list. That way we cans skip individual bad URL's or patterns of data URL's.
        if ( !skip(leafNodeReference.getUrl()) && matchingDataset != null ) {
            matchingDataset.addContent(ncml);
            String tid = matchingDataset.getAttributeValue("ID");
            String id = fixid(tid, leafNodeReference.getUrl());
            matchingDataset.setAttribute("ID", id);
        } else if (skip(leafNodeReference.getUrl())) {
            System.err.println("Skipping " + leafNodeReference.getUrl() );
        }


        Element viewer0Property = new Element("property", ns);
        viewer0Property.setAttribute("name", "viewer_0");
        viewer0Property.setAttribute("value", viewer_0+leafNodeReference.getUrl()+viewer_0_description);

        properties.add(viewer0Property);

        Element viewer1Property = new Element("property", ns);
        viewer1Property.setAttribute("name", "viewer_1");
        String searchURL = leafNodeReference.getUrl();
        if ( searchURL.startsWith("https://") ) {
            searchURL = searchURL.replace("https://", "");
        } else if ( searchURL.startsWith("http://") ) {
            searchURL = searchURL.replace("http://","");
        }
        viewer1Property.setAttribute("value", viewer_1+searchURL+viewer_1_description);

        properties.add(viewer1Property);

//        Element viewer2Property = new Element("property", ns);
//        viewer2Property.setAttribute("name", "viewer_2");
//        viewer2Property.setAttribute("value", viewer_2+leafNodeReference.getUrl()+viewer_2_description);
//
//        properties.add(viewer2Property);

        if ( matchingDataset != null ) {
            matchingDataset.addContent(0, service);
            matchingDataset.addContent(0, properties);
        }


    }
    private String fixid(String id, String url) {
        if ( url != null  && url.contains("#") ) {
            url = url.substring(0, url.indexOf("#"));
        }
        if ( url != null && url.startsWith("http") ) {
            url = url.substring(url.indexOf("//") + 2);
        }
        if ( url != null && url.contains(":[0-9][0-9][0-9][0-9]") ) {
            url = url.replace(":[0-9][0-9][0-9][0-9]", "");
        }
        if ( url.contains(id) ) {
            return url;
        } else {
            return url + "_" + id;
        }

    }
    private Set<String> removeRemoteServices(Document doc ) {
        Set<String> removedTypes = new HashSet<String>();
        List<Element> removedServiceElements = remove(doc, "service");
        remove(doc, "serviceName");
        Iterator<Element> metaIt = doc.getRootElement().getDescendants(new ElementFilter("metadata"));
        List<Parent> parents = new ArrayList<Parent>();
        while ( metaIt.hasNext() ) {
            Element meta = metaIt.next();
            List<Element> children = meta.getChildren();
            if ( children == null || children.size() == 0 ) {
                parents.add(meta.getParent());
            }
        }
        for ( Iterator parentIt = parents.iterator(); parentIt.hasNext(); ) {
            Parent parent = (Parent) parentIt.next();
            parent.removeContent(new ElementFilter("metadata"));
        }
        for ( Iterator servIt = removedServiceElements.iterator(); servIt.hasNext(); ) {
            Element service = (Element) servIt.next();
            String type = service.getAttributeValue("serviceType").toUpperCase();
            if ( !type.equals("COMPOUND") ) {
                removedTypes.add(type);
            }
        }
        return removedTypes;
    }
    private void addLocalServices(Document doc, String remoteServiceBase, Set<String> remoteTypes) {
        Element service = new Element("service", ns);
        service.setAttribute("name", threddsServerName+"_compound");
        service.setAttribute("serviceType", "compound");
        // Local services should have a base with the value of the public thredds server.
        // E.g. http://ferret.pmel.noaa.gov/uaf/thredds
        // Or maybe https:// ...
        service.setAttribute("base", "");
        String localBaseProxyURL = threddsServerName;
        if ( !localBaseProxyURL.endsWith("/") ) localBaseProxyURL = localBaseProxyURL + "/";
        List<String> missing_services = new ArrayList<String>();
        if ( remoteTypes.contains("WMS") ) {
            addFullService(service, remoteServiceBase.replace("dodsC", "wms"), "WMS");
        } else {
            addService(service, localBaseProxyURL, "wms/", "WMS");
        }
        if ( remoteTypes.contains("WCS") ) {
            addFullService(service, remoteServiceBase.replace("dodsC", "wcs"), "WCS");
        } else {
            addService(service, localBaseProxyURL, "wcs/", "WCS");
        }
        if ( remoteTypes.contains("NCML") ) {
            addFullService(service, remoteServiceBase.replace("dodsC", "ncml"), "NCML");
        } else {
            addService(service, localBaseProxyURL,"ncml/", "NCML");
        }
        if ( remoteTypes.contains("ISO") ) {
            addFullService(service, remoteServiceBase.replace("dodsC", "iso"), "ISO");
        } else {
            addService(service, localBaseProxyURL, "iso/", "ISO");
        }
        if ( remoteTypes.contains("UDDC") ) {
            addFullService(service, remoteServiceBase.replace("dodsC", "uddc"), "UDDC");
        } else {
            addService(service, localBaseProxyURL, "uddc/", "UDDC");
        }
        if ( remoteTypes.contains("OPENDAP") ) {
            addFullService(service, remoteServiceBase, "OPENDAP");
        } else {
            addService(service, localBaseProxyURL, "dodsC/", "OPENDAP");
        }
        // Put this at the top of the document in index 0.
        doc.getRootElement().addContent(0, service);
    }
    private void addService ( Element compoundService, String base, String endpoint, String type) {
        Element service = new Element("service", ns);
        service.setAttribute("name", threddsServerName+"_"+type);
        // Make these relative so the F5 can do it's job.
        String fullbase = base;
        if ( !fullbase.endsWith("/") ) fullbase = fullbase + "/";
        fullbase = fullbase + threddsContext;
        if ( !fullbase.endsWith("/") ) fullbase = fullbase + "/";
        fullbase = fullbase + endpoint;
        service.setAttribute("base", fullbase);
        service.setAttribute("serviceType", type);
        compoundService.addContent(service);
    }
    private void addFullService(Element compoundService, String base, String type) {
        Element service = new Element("service", ns);
        service.setAttribute("name", base+"_"+type);
        service.setAttribute("base", base);
        service.setAttribute("serviceType", type);
        compoundService.addContent(service);
    }
    public static List<LeafNodeReference> findAccessDatasets(String url, Document doc, List<LeafNodeReference> datasets) throws URISyntaxException {
        InvCatalogFactory catfactory = new InvCatalogFactory("default", false);
        String strippedDoc = JDOMUtils.toString(doc);
        InvCatalog thredds = (InvCatalog) catfactory.readXML(strippedDoc, new URI(url));
        List<InvDataset> rootInvDatasets = thredds.getDatasets();
        return findAccessDatasets(rootInvDatasets, datasets);
    }
    private static List<LeafNodeReference> findAccessDatasets(List<InvDataset> invDatasets, List<LeafNodeReference> datasets) {
        for (Iterator dsIt = invDatasets.iterator(); dsIt.hasNext(); ) {
            InvDataset ds = (InvDataset) dsIt.next();
            if ( ds.hasAccess() ) {
                InvAccess access = ds.getAccess(ServiceType.OPENDAP);
                if(access!=null && !ds.getName().equals("Forecast Model Run Collection (2D time coordinates)")) {
                    String locationUrl = access.getStandardUrlName();
                    if ( locationUrl != null ) {
                        String urlPath = access.getUrlPath();
                        LeafNodeReference lnr = new LeafNodeReference(locationUrl, urlPath);
                        datasets.add(lnr);
                        if (ds.getName().toLowerCase().contains("best time")) {
                            lnr.setBestTimeSeries(true);
                            hasBestTimeSeries = true;
                        }
                    }
                }

            } else if ( ds.hasNestedDatasets()) {
                List<InvDataset> children = ds.getDatasets();
                findAccessDatasets(children, datasets);
            }
        }
        return datasets;
    }
    private boolean appearsJunk(List<LeafNodeReference> leaves) {
        boolean b = false;
        for (int i = 0; i < leaves.size(); i++) {
            LeafNodeReference leaf = leaves.get(i);
            if ( leaf.getUrl().contains(".csv") ) {
                return true;
            } else if ( leaf.getUrl().contains(".pdf") ) {
                return true;
            } else if ( leaf.getUrl().contains(".jpg") ) {
                return true;
            } else if ( leaf.getUrl().contains(".png") ) {
                return true;
            } else if ( leaf.getUrl().contains(".json") ) {
                return true;
            }

        }
        return b;
    }
    private boolean appearsUnaggregated(List<LeafNodeReference> leaves) {
        if ( leaves.size() > 1 ) {

            boolean b1 = unaggregatedYears(leaves, "20");
            boolean b2 = unaggregatedYears(leaves, "19");
            boolean b3 = unaggregatedYears(leaves, "18");
            boolean b4 = unaggregatedYears(leaves, "17");

            boolean b5 = unaggregatedIndex(leaves);

            return b1 || b2 || b3 || b4 || b5;
        }
        return false;
    }

    private boolean unaggregatedIndex(List<LeafNodeReference> leaves) {

        // Looking to find sets like this: http://geoport.whoi.edu/thredds/dodsC/usgs/data0/mvco_ce/mvco_output/spatial_7_ar0fd/catalog.html
        // There are a mix of aggregations and unaggregated data. We want to reject based on the unaggregated. Sophisticated would be to
        // re-write the catalog without the unaggregated data sets.

        boolean unagg_index = false;
        // Check the first few...
        String start = null;
        int startIndex = 0;
        int startFileIndex = 0;
        String compare = "";
        for (int i = 0; i < Math.min(6, leaves.size()); i++) {
            compare = "";
            String u1 = leaves.get(i).getUrl();
            int runCount = countRun(u1, '0');
            if ( runCount > 1 ) {

                for (int j = 0; j < runCount; j++) {
                    compare = compare + "0";
                }
                // There may be a file of "0000" or "000", but we don't care. If there's 001, 002 etc we'll flag it.
                if (u1.contains(compare+"1") && !u1.contains("2001")) {
                    start = compare + "1";
                    startIndex = i;
                    startFileIndex = 1;
                    // We can start looking for more indexes from here...
                    break;
                }
            }
        }

        // Suspect that these files are indexed as 000, 001, 002...
        if ( start != null ) {
            unagg_index = true;

            for (int i = startIndex; i < Math.min(6, leaves.size()); i++) {
                String url = leaves.get(i).getUrl();
                String test = compare + startFileIndex;
                unagg_index = unagg_index && url.contains(test);
                startFileIndex++;
            }
        }
        return unagg_index;
    }
    private boolean unaggregatedYears(List<LeafNodeReference> leaves, String century) {
        boolean b = false;

        List<String> centuries = new ArrayList<>();
        centuries.add("17");
        centuries.add("18");
        centuries.add("19");
        centuries.add("20");

        Pattern op1;
        Pattern op2;
        Pattern op3;
        Pattern op4;
        // Check to see if another century appears in the remainder for something like 1920-2020
        if ( century.equals("20") ) {
            op1 = Pattern.compile(".+" + centuries.get(0) + "([0-9]{2}-" + century + "[0-9]{2})");
            op2 = Pattern.compile(".+" + centuries.get(1) + "([0-9]{2}-" + century + "[0-9]{2})");
            op3 = Pattern.compile(".+" + centuries.get(2) + "([0-9]{2}-" + century + "[0-9]{2})");
            op4 = Pattern.compile(".+" + centuries.get(3) + "([0-9]{2}-" + century + "[0-9]{2})");
        } else if ( century.equals("19") ) {
            op1 = Pattern.compile(".+" + centuries.get(0) + "([0-9]{2}-" + century + "[0-9]{2})");
            op2 = Pattern.compile(".+" + centuries.get(1) + "([0-9]{2}-" + century + "[0-9]{2})");
            op3 = Pattern.compile(".+" + centuries.get(2) + "([0-9]{2}-" + century + "[0-9]{2})");
            op4 = Pattern.compile(".+" + century + "([0-9]{2}-" + centuries.get(3) + "[0-9]{2})");
        } else if ( century.equals("18") ) {
            op1 = Pattern.compile(".+" + centuries.get(0) + "([0-9]{2}-" + century + "[0-9]{2})");
            op2 = Pattern.compile(".+" + centuries.get(1) + "([0-9]{2}-" + century + "[0-9]{2})");
            op3 = Pattern.compile(".+" + century + "([0-9]{2}-" + centuries.get(2) + "[0-9]{2})");
            op4 = Pattern.compile(".+" + century + "([0-9]{2}-" + centuries.get(3) + "[0-9]{2})");
        } else {
            // "17"
            op1 = Pattern.compile(".+" + centuries.get(0) + "([0-9]{2}-" + century + "[0-9]{2})");
            op2 = Pattern.compile(".+" + century + "([0-9]{2}-" + centuries.get(1) + "[0-9]{2})");
            op3 = Pattern.compile(".+" + century + "([0-9]{2}-" + centuries.get(2) + "[0-9]{2})");
            op4 = Pattern.compile(".+" + century + "([0-9]{2}-" + centuries.get(3) + "[0-9]{2})");
        }


        // In some ESRL directories there are "hidden" aggregations that start with "."
        // Some directories have a "latest.nc" at the beginning, so in general search until you find
        // the year string you want.
        String u1 = null;
        int index = 0;
        for (int i = 0; i < leaves.size(); i++) {
            u1 = leaves.get(i).getUrl();
            if ( u1.contains(century) ) {
                index = i;
                break;
            }
        }
        if ( index < leaves.size() - 2 ) {

            Matcher mp1 = op1.matcher(u1);
            Matcher mp2 = op2.matcher(u1);
            Matcher mp3 = op3.matcher(u1);
            Matcher mp4 = op4.matcher(u1);
            // Found a match of the form 1950-1970 or 1950-2010
            boolean bp1 = mp1.find();
            boolean bp2 = mp2.find();
            boolean bp3 = mp3.find();
            boolean bp4 = mp4.find();

            if ( bp1 || bp2 || bp3 || bp4 )  return false;



            String u2 = leaves.get(index + 1).getUrl();

            Pattern p = Pattern.compile(".+" + century + "([0-9]{2})");
            Matcher m1 = p.matcher(u1);
            Matcher m2 = p.matcher(u2);
            if (m1.find() && m2.find()) {

                String sub1 = u1.substring(0, m1.end() - 4);
                String sub2 = u2.substring(0, m2.end() - 4);

                b = sub1.equals(sub2);

            }
        }
        return b;
    }
    // https://stackoverflow.com/questions/16849813/count-character-consecutively-in-java
    // except we start at the end and work back to the last "/"
    private int countRun( String s, char c )
    {
        int counter = 0;
        int start = s.lastIndexOf("/");
        boolean foundOne = false;
        for( int i = s.length() - 1; i >= start; i--)
        {
            if( s.charAt(i) == c )
            {
                counter += 1;
                foundOne = true;
            }
            else {
                if(foundOne) break;
            }
        }
        return counter;
    }
    public boolean skip(String url) {
        for ( int i = 0; i < exclude.size(); i++ ) {
            if ( Pattern.matches(exclude.get(i), url)) {
                return true;
            }
        }
        for ( int i = 0; i < excludeCatalog.size(); i++ ) {
            if ( excludeCatalog.get(i).equals(url) ) {
                return true;
            }
        }
        return false;
    }

}
