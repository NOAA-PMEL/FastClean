package pmel.sdig.util;

import java.util.ArrayList;
import java.util.List;

public class OutputFiles {
    String catalog;
    List<String> accessChildren = new ArrayList<>();

    public OutputFiles(String catalog) {
        this.catalog = catalog;
    }
    public void addAccessChild(String url) {
        accessChildren.add(url);
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public List<String> getAccessChildren() {
        return accessChildren;
    }

    public void setAccessChildren(List<String> accessChildren) {
        this.accessChildren = accessChildren;
    }
}
