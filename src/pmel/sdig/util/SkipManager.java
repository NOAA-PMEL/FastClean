package pmel.sdig.util;


import java.io.InputStream;

/**
 * Created by rhs on 11/14/17.
 */
public class SkipManager {
    public InputStream getSkip() {
        ClassLoader classLoader = getClass().getClassLoader();
        return classLoader.getResourceAsStream("resources/skip.xml");
    }
}
