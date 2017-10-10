package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by sumedh on 4/12/17.
 */

public class OperationResponseClass implements Serializable {

    public Integer deletedRows;
    public Map<String, String> queriedRecords;

    public OperationResponseClass(Integer deletedRows, Map<String, String > queriedRecords){
        this.deletedRows = deletedRows;
        this.queriedRecords = queriedRecords;
    }
}