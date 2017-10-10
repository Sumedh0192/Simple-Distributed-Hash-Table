package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by sumedh on 4/12/17.
 */

public class JoinRequest implements Serializable {

    public String portNum;

    public JoinRequest(String portNum){
        this.portNum = portNum;
    }
}
