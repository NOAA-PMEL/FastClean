package pmel.sdig.util;

public class DatasetCounter {
    int depth;
    int acccessChildren;

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getAcccessChildren() {
        return acccessChildren;
    }

    public void setAcccessChildren(int acccessChildren) {
        this.acccessChildren = acccessChildren;
    }
    public void addAccessChild() {
        acccessChildren++;
    }
}
