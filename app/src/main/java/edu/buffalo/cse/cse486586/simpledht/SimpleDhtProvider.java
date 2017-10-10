package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static Map<String, String> databaseMap;
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final int JOIN_TIMEOUT = 5000;
    static final int SERVER_PORT = 10000;
    private final Uri pa3Uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    static Set<String> nodes = new HashSet<String>();
    static String node_id;
    static String myPort;
    static List<String> chordRing = new ArrayList<String>();
    static Map<String,String> chordsKeyMap = new HashMap<String, String>();
    static Timer joinTimer = null;
    static Boolean timerActived = false;

    private Uri buildUri(String scheme, String authority) {

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try{
            int rowsDeleted = 0;
            if(selection.compareTo("\"*\"") == 0){
                if(chordRing.size()>0) {
                    Socket socket = null;
                    ObjectOutputStream out = null;
                    ObjectInputStream in = null;
                    Object receivedObject;
                    OperationResponseClass optResponse;
                    for (String remotePort : chordsKeyMap.values()) {
                        try {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort));
                            out = new ObjectOutputStream(socket.getOutputStream());
                            out.writeObject(new OperationClass("delete", "\"@\"", null, null));
                            SystemClock.sleep(100);
                            out.flush();
                            while (!socket.isClosed()) {
                                in = new ObjectInputStream(socket.getInputStream());
                                receivedObject = in.readObject();
                                if (receivedObject != null) {
                                    if (receivedObject instanceof OperationResponseClass) {
                                        optResponse = (OperationResponseClass) receivedObject;
                                        if (optResponse.deletedRows != null) {
                                            rowsDeleted += optResponse.deletedRows;
                                            break;
                                        }
                                    }
                                }
                            }
                        } catch (UnknownHostException e) {
                            Log.e(TAG, "SendTask UnknownHostException");
                            Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
                        } catch (IOException e) {
                            Log.e(TAG, "SendTask socket IOException");
                            Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
                        } finally {
                            try {
                                out.close();
                                socket.close();
                            } catch (IOException ex) {
                                Log.d(TAG, "Exception-->  " + ex.getMessage());
                                Log.d(TAG, "Exception--->  " + ex.getStackTrace());
                            }
                        }
                    }
                }else{
                    rowsDeleted = databaseMap.size();
                    databaseMap = new HashMap<String, String>();
                }
            }else if(selection.compareTo("\"@\"") == 0){
                rowsDeleted = databaseMap.size();
                databaseMap = new HashMap<String, String>();
            }else{
                String keyHash = genHash(selection);
                final String nodeToInsert = getOperationNode(keyHash);
                if(nodeToInsert.compareTo(node_id) == 0){
                    if(databaseMap.containsKey(selection)){
                        databaseMap.remove(selection);
                        rowsDeleted = 1;
                    }
                }else{
                    new MessageMultiCast().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new SenderClass(new OperationClass("delete", selection, null, null), new HashSet<String>(){{add(chordsKeyMap.get(nodeToInsert));}}));
                    rowsDeleted = 1;
                }
            }
            return rowsDeleted;
        }catch (Exception ex){
            Log.e(TAG, " Exception --->  " + ex.getMessage());
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        try{
            String keyHash = genHash(values.get("key").toString());
            final String nodeToInsert = getOperationNode(keyHash);
            if(nodeToInsert.compareTo(node_id) == 0){
                databaseMap.put(values.get("key").toString(), values.get("value").toString());
            }else{
                new MessageMultiCast().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new SenderClass(new OperationClass("insert", null, values.get("key").toString(), values.get("value").toString()), new HashSet<String>(){{add(chordsKeyMap.get(nodeToInsert));}}));
            }
        }catch (Exception ex){
            Log.e(TAG, " Exception --->  " + ex.getMessage());
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        try {
            Context context = getContext();
            TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            databaseMap = new HashMap<String, String>();
            node_id = genHash(portStr);
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new MessageReceiver().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
                if(portStr.compareTo("5554") != 0){
                    new MessageMultiCast().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new SenderClass(new JoinRequest(portStr), new HashSet<String>(){{add("11108");}}));
                }
            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
                Log.e(TAG,"Exception-->  " + e.getMessage());
                Log.e(TAG,"Exception--->  " + e.getStackTrace());
                return false;
            }
        }catch(Exception ex){
            Log.d(TAG,"Exception-->  " + ex.getMessage());
        }
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        try{
            MatrixCursor cursorBuilder = new MatrixCursor(new String[]{"key", "value"});
            Cursor returnCursor = null;
            if(selection.compareTo("\"*\"") == 0) {

                if (chordRing.size() > 0) {
                    Map<String, String> responseMap = new HashMap<String, String>();
                    Socket socket = null;
                    ObjectOutputStream out = null;
                    ObjectInputStream in = null;
                    Object receivedObject;
                    OperationResponseClass optResponse;
                    for (String remotePort : chordsKeyMap.values()) {
                        Log.d(TAG, remotePort + " #---# " + chordsKeyMap.get(node_id));
                        if (remotePort.compareTo(chordsKeyMap.get(node_id)) != 0) {
                            try {
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePort));
                                out = new ObjectOutputStream(socket.getOutputStream());
                                Object returnObj = new OperationClass("query", "\"@\"", null, null);
                                out.writeObject(returnObj);
                                SystemClock.sleep(100);
                                out.flush();
                                while (!socket.isClosed()) {
                                    in = new ObjectInputStream(socket.getInputStream());
                                    receivedObject = in.readObject();
                                    if (receivedObject != null) {
                                        if (receivedObject instanceof OperationResponseClass) {
                                            optResponse = (OperationResponseClass) receivedObject;
                                            if (optResponse.queriedRecords != null) {
                                                responseMap.putAll(optResponse.queriedRecords);
                                                break;
                                            }
                                        }
                                    }
                                }
                            } catch (UnknownHostException e) {
                                Log.e(TAG, "SendTask UnknownHostException");
                                Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
                            } catch (IOException e) {
                                Log.e(TAG, "SendTask socket IOException");
                                Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
                            } finally {
                                try {
                                    out.close();
                                    socket.close();
                                } catch (IOException ex) {
                                    Log.d(TAG, "Exception-->  " + ex.getMessage());
                                    Log.d(TAG, "Exception--->  " + ex.getStackTrace());
                                }

                            }
                        } else if (remotePort.compareTo(chordsKeyMap.get(node_id)) == 0) {
                            responseMap.putAll(databaseMap);
                        }
                    }
                    if (responseMap.size() > 0) {
                        for (String key : responseMap.keySet()) {
                            cursorBuilder.addRow(new String[]{key, responseMap.get(key)});
                        }
                        Cursor[] cursors = {cursorBuilder, returnCursor};
                        returnCursor = new MergeCursor(cursors);
                        return returnCursor;
                    }
                }else{
                    if (databaseMap.size() > 0) {
                        for (String key : databaseMap.keySet()) {
                            cursorBuilder.addRow(new String[]{key, databaseMap.get(key)});
                        }
                        Cursor[] cursors = {cursorBuilder, returnCursor};
                        returnCursor = new MergeCursor(cursors);
                        return returnCursor;
                    }
                }
            } else if (selection.compareTo("\"@\"") == 0) {
                if (databaseMap.size() > 0) {
                    for (String key : databaseMap.keySet()) {
                        cursorBuilder.addRow(new String[]{key, databaseMap.get(key)});
                    }
                    Cursor[] cursors = {cursorBuilder, returnCursor};
                    returnCursor = new MergeCursor(cursors);
                    return returnCursor;
                }
            } else {
                String keyHash = genHash(selection);
                final String nodeToInsert = getOperationNode(keyHash);
                if (nodeToInsert.compareTo(node_id) == 0) {
                    if (databaseMap.containsKey(selection)) {
                        cursorBuilder.addRow(new String[]{selection, databaseMap.get(selection)});
                        Cursor[] cursors = {cursorBuilder, returnCursor};
                        returnCursor = new MergeCursor(cursors);
                        return returnCursor;
                    }
                } else {
                    Map<String, String> responseMap = new HashMap<String, String>();
                    Socket socket = null;
                    ObjectOutputStream out = null;
                    ObjectInputStream in = null;
                    Object receivedObject;
                    OperationResponseClass optResponse;
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(chordsKeyMap.get(nodeToInsert)));
                        out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(new OperationClass("query", selection, null, null));
                        SystemClock.sleep(100);
                        out.flush();
                        while (!socket.isClosed()) {
                            in = new ObjectInputStream(socket.getInputStream());
                            receivedObject = in.readObject();
                            if (receivedObject != null) {
                                if (receivedObject instanceof OperationResponseClass) {
                                    optResponse = (OperationResponseClass) receivedObject;
                                    if (optResponse.queriedRecords != null) {
                                        responseMap.putAll(optResponse.queriedRecords);
                                        break;
                                    }
                                }
                            }
                        }
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "SendTask UnknownHostException");
                        Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
                    } catch (IOException e) {
                        Log.e(TAG, "SendTask socket IOException");
                        Log.d(TAG, "Exception: " + e.getStackTrace() + "  " + e.getMessage());
                    } finally {
                        try {
                            out.close();
                            socket.close();
                        } catch (IOException ex) {
                            Log.d(TAG, "Exception-->  " + ex.getMessage());
                            Log.d(TAG, "Exception--->  " + ex.getStackTrace());
                        }
                    }
                    if (responseMap.containsKey(selection)) {
                        cursorBuilder.addRow(new String[]{selection, responseMap.get(selection)});
                        Cursor[] cursors = {cursorBuilder, returnCursor};
                        returnCursor = new MergeCursor(cursors);
                        return returnCursor;
                    }
                }
            }
            return null;
        }catch (Exception ex){
            Log.e(TAG, " Exception --->  " + ex.getMessage());
        }
        return null;
        // TODO Auto-generated method stub
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private JoinResponse formChordsRing(Set<String> Nodes){
        try {
            int tempRingSize = 0;
            int i = 0;
            if (chordRing.size() == 0) {
                chordRing.add(genHash("5554"));
                chordsKeyMap.put(genHash("5554"), "11108");
            }
            for(String portnum : Nodes){
                String portKey = genHash(portnum);
                if(chordRing.size() == 1){
                    if(portKey.compareTo(chordRing.get(0)) > 0){
                        chordRing.add(portKey);
                    }else{
                        chordRing.add(0,portKey);
                    }
                }else {
                    tempRingSize = chordRing.size();
                    for (i = 0; i < tempRingSize; i++) {
                        if (portKey.compareTo(chordRing.get(i)) <= 0) {
                            chordRing.add(i, portKey);
                            break;
                        }
                    }
                    if(i == tempRingSize){
                        chordRing.add(portKey);
                    }
                }
                chordsKeyMap.put(portKey, String.valueOf(Integer.valueOf(portnum)*2));
            }
            return new JoinResponse(chordRing,chordsKeyMap);
        }catch(Exception ex){
            Log.e(TAG, "Exception ---->  " + ex.getMessage());
        }
        return null;
    }

    private String getOperationNode(String keyHash){
        try {
            if(chordRing.size() == 0){
                return node_id;
            }
            String operationNode = "";
            for(String nodeKey : chordRing){
                if(nodeKey.compareTo(keyHash) > 0){
                    operationNode = nodeKey;
                    break;
                }
            }
            if(operationNode == ""){
                return chordRing.get(0);
            }
            return operationNode;
        }catch (Exception ex) {
            Log.e(TAG, " Exception --->  " + ex.getMessage());
            return null;
        }
    }

    private class MessageReceiver extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket listner = null;
            ObjectOutputStream sendObjectStream;
            while(!serverSocket.isClosed()) {
                try {
                    listner = serverSocket.accept();
                    ObjectInputStream receivedObjectStream = new ObjectInputStream(listner.getInputStream());
                    Object receivedObject = receivedObjectStream.readObject();
                    JoinRequest joinRequestObject = null;
                    JoinResponse joinResponseObject = null;
                    OperationClass operationClassObject = null;
                    if(receivedObject instanceof JoinRequest){
                        joinRequestObject = (JoinRequest) receivedObject;
                    }else if(receivedObject instanceof JoinResponse){
                        joinResponseObject = (JoinResponse) receivedObject;
                    }else if(receivedObject instanceof OperationClass){
                        operationClassObject = (OperationClass) receivedObject;
                    }
                    if(joinRequestObject != null){
                        if(!timerActived){
                            timerActived = true;
                            joinTimer = new Timer();
                            joinTimer.schedule(new joinHandler(), JOIN_TIMEOUT);
                        }
                        nodes.add(joinRequestObject.portNum);
                    }else if(joinResponseObject != null){
                        chordRing = joinResponseObject.chordsRing;
                        chordsKeyMap = joinResponseObject.keyMap;
                    }else if(operationClassObject != null){
                        if(operationClassObject.type.compareTo("insert") == 0){
                            ContentValues values = new ContentValues();
                            values.put("key", operationClassObject.key);
                            values.put("value", operationClassObject.value);
                            insert(pa3Uri, values);
                        }else if(operationClassObject.type.compareTo("delete") == 0){
                            int rowsDeleted = delete(pa3Uri,operationClassObject.selection,null);
                            if(operationClassObject.selection.compareTo("\"@\"") == 0){
                                sendObjectStream = new ObjectOutputStream(listner.getOutputStream());
                                sendObjectStream.writeObject(new OperationResponseClass(rowsDeleted,null));
                                SystemClock.sleep(100);
                                sendObjectStream.flush();
                                sendObjectStream.close();
                            }
                        }else if(operationClassObject.type.compareTo("query") == 0){
                            Cursor cursor = query(pa3Uri, null, operationClassObject.selection, null, null);
                            Map<String, String> returnMap = new HashMap<String, String>();
                            if(cursor != null) {
                                while (cursor.moveToNext()) {
                                    returnMap.put(cursor.getString(cursor.getColumnIndexOrThrow("key")), cursor.getString(cursor.getColumnIndexOrThrow("value")));
                                }
                            }
                            sendObjectStream = new ObjectOutputStream(listner.getOutputStream());
                            sendObjectStream.writeObject(new OperationResponseClass(null,returnMap));
                            SystemClock.sleep(100);
                            sendObjectStream.flush();
                            sendObjectStream.close();
                        }
                    }
                } catch (Exception e) {
                    Log.e(TAG,"Error in Receiving the message");
                    Log.d(TAG, "Exception: " + e.getStackTrace()[0].getLineNumber() + "  " + e.getStackTrace()[0].getMethodName() + "  " + e.getStackTrace()[0].getClassName() + "  " + e.getStackTrace()[0].getFileName());
                    return null;
                } finally {
                    try {
                        listner.close();
                    } catch (IOException ex) {
                        Log.e(TAG,"Exception-->  " + ex.getMessage());
                        Log.e(TAG,"Exception--->  " + ex.getStackTrace());
                        return null;
                    }
                }
            }
            try {
                serverSocket.close();
            }catch(IOException ex){
                Log.e(TAG,"Exception-->  " + ex.getMessage());
                Log.e(TAG,"Exception--->  " + ex.getStackTrace());
            }
            return null;
        }

        protected void onProgressUpdate(String...msgs) {
            return;
        }
    }

    private class MessageMultiCast extends AsyncTask<SenderClass, Void, Void> {

        @Override
        protected Void doInBackground(SenderClass... msgs) {
            Socket socket = null;
            ObjectOutputStream out = null;
            for(String remotePort : msgs[0].portsToSend) {
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgs[0].objectToSend);
                    out.flush();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "SendTask UnknownHostException");
                    Log.d(TAG,"Exception: " + e.getStackTrace() + "  " + e.getMessage());
                } catch (IOException e) {
                    Log.e(TAG, "SendTask socket IOException");
                    Log.e(TAG,"Exception: " + e.getStackTrace() + "  " + e.getMessage());
                } finally {
                    try {
                        out.close();
                        socket.close();
                    } catch (IOException ex) {
                        Log.d(TAG,"Exception-->  " + ex.getMessage());
                        Log.d(TAG,"Exception--->  " + ex.getStackTrace());
                    }
                }
            }
            return null;
        }
    }

    private class joinHandler extends TimerTask {

        public Set<String> portNumbers = new HashSet<String>();

        public joinHandler(){

            joinTimer = null;
        }

        @Override
        public void run(){
            for(String node : nodes){
                this.portNumbers.add(String.valueOf(Integer.valueOf(node)*2));
            }
            new MessageMultiCast().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new SenderClass(formChordsRing(nodes), portNumbers));
        }
    }

    private class SenderClass{

        public Object objectToSend;
        public Set<String> portsToSend;

        public SenderClass(Object objectToSend, Set<String> portsToSend){
            this.objectToSend = objectToSend;
            this.portsToSend = portsToSend;
        }
    }
}