package ecs;

public class ECSNode implements IECSNode {

    private String NodeName;
    private String NodeHost;
    private int NodePort;
    private String[] NodeHashRange;
    private boolean online;

    public ECSNode(String _NodeName, String _NodeHost, int _NodePort){
        NodeName = _NodeName;
        NodeHost = _NodeHost;
        NodePort = _NodePort;
        NodeHashRange = null;
        online = false;
    }

    public void setOnline(){
        online = true;
    }

    public void setOffline(){
        online = false;
    }

    @Override
    public String getNodeName(){
        return NodeName;
    }

    @Override
    public String getNodeHost(){
        return NodeHost;
    }

    @Override
    public int getNodePort(){
        return NodePort;
    }

    @Override
    public String[] getNodeHashRange(){
        return NodeHashRange;
    }
}
