package pmel.sdig.jdo;

import java.util.List;

public class LeafNodeReference {

    private String url;
    
    private String urlPath;

    boolean bestTimeSeries;


    String crawlDate;


    public LeafNodeReference(String url, String urlPath) {
        super();
        this.url = url;
        this.urlPath = urlPath;
    }
    
    public String getUrlPath() {
        return urlPath;
    }

    public void setUrlPath(String urlPath) {
        this.urlPath = urlPath;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
    public String getCrawlDate() {
        return crawlDate;
    }

    public void setCrawlDate(String crawlDate) {
        this.crawlDate = crawlDate;
    }

    @Override
    public String toString() {
        return url;
    }

    public boolean isBestTimeSeries() {
        return bestTimeSeries;
    }

    public void setBestTimeSeries(boolean bestTimeSeries) {
        this.bestTimeSeries = bestTimeSeries;
    }
}
