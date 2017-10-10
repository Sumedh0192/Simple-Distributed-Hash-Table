package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sumedh on 4/12/17.
 */

public class JoinResponse implements Serializable{
    public List<String> chordsRing = new ArrayList<String>();
    public Map<String,String> keyMap = new HashMap<String, String>();

    public JoinResponse(List<String> chordsRing, Map<String,String> keyMap){
        this.chordsRing = chordsRing;
        this.keyMap = keyMap;
    }
}
