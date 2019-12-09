package pmel.sdig;

import org.jdom2.JDOMException;

import java.io.IOException;
import java.net.URISyntaxException;

public class RemoveEmpties {
    public static void main(String[] args) {
        try {
//            FastClean.removeEmpties("CleanCatalog.xml");
//            FastClean.removeChildless("CleanCatalog.xml");
            FastClean.removeRedundant("CleanCatalog.xml");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
