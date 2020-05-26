package edu.buffalo.cse.cse486586.simpledht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.content.SharedPreferences;

import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;
import android.content.Context;
import android.telephony.TelephonyManager;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.net.InetAddress;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.net.UnknownHostException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static android.content.ContentValues.TAG;
import static edu.buffalo.cse.cse486586.simpledht.Constants.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledht.Constants.SERVER_PORT;
import static edu.buffalo.cse.cse486586.simpledht.Constants.STORAGE_FILE;
import static edu.buffalo.cse.cse486586.simpledht.Constants.VALUE_FIELD;


public class SimpleDhtProvider extends ContentProvider {

    String[] REMOTE_PORTS = {"11108","11112","11116","11120","11124"};
    String myPort;
    int nodes = 1;
    ArrayList nodes_in_ring = new ArrayList(  );
    ArrayList<String> present_nodes;
    Utility utility;
    private Uri mUri;
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        HashMap<String,String> h = new HashMap<String, String>(  );

         if (selection.contains( "@" )  || selection.contains( "*" )) {
            utility.delAllLocalData( getContext() );
        }
        else {
            if (nodes == 1){
                SharedPreferences sp = getContext().getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);
                SharedPreferences.Editor e = sp.edit( );
                e.remove( selection );
                e.apply();

            }
            else {

                String next_node = findNextNode( present_nodes, selection );
                String msg = "DEL" + selection;
                new ClientTask( ).executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, msg, next_node );
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public String getNode( String key, TreeMap<String, String> ring ) throws  NoSuchAlgorithmException{
        String hashKey2 = genHash( key );
        String a2 = ring.higherKey( hashKey2 );
        if (a2 == null) {
            a2 = ring.firstKey( );
        }
        String firstNode = ring.get( a2 );
        return firstNode ;
    }


    public String findNextNode(ArrayList<String> arr, String key){
        TreeMap<String, String> ring = new TreeMap<String, String>(  );
        if (arr.size() == 0){
            return "11108";
        }
        String node = "";
        try {
            for (String k : arr) {
                ring.put( genHash( Integer.toString( Integer.parseInt( k ) / 2 ) ), k );

            }
            node = getNode( key,ring );
        }catch (NoSuchAlgorithmException e){
        }
        return node;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        if (nodes == 1) {

            SharedPreferences sp = getContext( ).getSharedPreferences( STORAGE_FILE, Context.MODE_PRIVATE );
            SharedPreferences.Editor e = sp.edit( );
            e.putString( values.getAsString( KEY_FIELD ), values.getAsString( VALUE_FIELD ) );
            e.apply( );

            return uri;
        }
        else {
            String next_node = findNextNode(present_nodes, values.getAsString( KEY_FIELD ) );
            String temp_key =values.getAsString( KEY_FIELD );
            String temp_value = values.getAsString( (VALUE_FIELD) );
            String temp_msg = "="+temp_key+ ":"  + temp_value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, temp_msg , next_node);
            return uri;

        }

    }

    public class sortUsingHash implements Comparator<String> {
        public int compare(String s1, String s2) {
            try {
                int is1 = Integer.parseInt( ( s1 ));
                int is2 = Integer.parseInt(  ( s2 ));
                return (genHash(Integer.toString(is1/2)).compareTo(genHash(Integer.toString(is2/2)))) ;


            }catch (NoSuchAlgorithmException e) {
                return 0;
            }
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        String scheme = "content";
        String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
        mUri = buildUri(scheme, authority);
        utility = new Utility();
        Context context = getContext();
        TelephonyManager tel = (TelephonyManager)   context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            return false;
        }
        if (!myPort.equals("11108" )) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort , myPort);
        }
        else {
            nodes_in_ring.add( myPort );
        }

        return true;
    }

    private HashMap<String, String> getAllLocalData() {

        HashMap<String, String> hm = new HashMap<String, String>();

        SharedPreferences sharedPref = getContext().getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);

        Map<String, ?> keys = sharedPref.getAll();

        for (Map.Entry<String, ?> entry : keys.entrySet()) {

            hm.put(entry.getKey(), entry.getValue().toString());

        }

        return hm;

    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        SharedPreferences sp = getContext().getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);
        Cursor c;
        HashMap<String, String> hm;
        if (nodes == 1) {

            if (selection.equals( Constants.GLOBAL ) || selection.equals( Constants.LOCAL )) {
                hm = getAllLocalData();
                MatrixCursor cursor = new MatrixCursor(
                        new String[]{KEY_FIELD, VALUE_FIELD}
                );

                for (Map.Entry<String, String> entry : hm.entrySet()) {

                    cursor.newRow()
                            .add(KEY_FIELD, entry.getKey())
                            .add(VALUE_FIELD, entry.getValue());
                }
                return cursor;


            }
            else {

                MatrixCursor c1 = new MatrixCursor(
                        new String[] {
                                KEY_FIELD,
                                VALUE_FIELD
                        }
                );
                c1.newRow()
                        .add(KEY_FIELD, selection)
                        .add(VALUE_FIELD, sp.getString(selection, null));
                return c1;

            }

        }
        else if (selection.equals( "@" )) {
            hm = getAllLocalData();

            MatrixCursor cursor = new MatrixCursor(
                    new String[]{KEY_FIELD, VALUE_FIELD}
            );

            for (Map.Entry<String, String> entry : hm.entrySet()) {

                cursor.newRow()
                        .add(KEY_FIELD, entry.getKey())
                        .add(VALUE_FIELD, entry.getValue());
            }

            return cursor;


        }
        else if (selection.equals( Constants.GLOBAL )) {

            hm = getAllLocalData();
            MatrixCursor cursor = new MatrixCursor(
                    new String[]{KEY_FIELD, VALUE_FIELD}
            );
            for (Map.Entry<String, String> entry : hm.entrySet()) {

                cursor.newRow()
                        .add(KEY_FIELD, entry.getKey())
                        .add(VALUE_FIELD, entry.getValue());
            }
            for (int i = 0; i<present_nodes.size(); i++){
                if (! myPort.equals(present_nodes.get( i ))) {
                    String temp_msg = "GLOBAL," + present_nodes.get( i );
                    try {
                        String res = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, temp_msg, myPort).get();
                        Log.i( "in query *", res );
                        if(!res.equals("NONE")){
                            String[] result_rows = res.split("\\;");
                            for (String row : result_rows) {
                                String[] kv_parts = row.split("\\?");
                                String res_key = kv_parts[0];
                                String res_value = kv_parts[1];
                                cursor.newRow()
                                        .add(KEY_FIELD, res_key)
                                        .add(VALUE_FIELD, res_value);
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace( );
                    } catch (ExecutionException e) {
                        e.printStackTrace( );
                    }
                }
            }
            return cursor;
        }
        else {
            String next_node = findNextNode(present_nodes, selection );
            String temp_key =selection;

            String temp_msg = "QUE"+temp_key;
            try {
                String res = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, temp_msg , next_node).get();
                String[] arr = res.split( "@" );
                MatrixCursor c1 = new MatrixCursor(
                        new String[] {
                                KEY_FIELD,
                                VALUE_FIELD
                        }
                );
                c1.newRow()
                        .add(KEY_FIELD, arr[0])
                        .add(VALUE_FIELD, arr[1]);
                return c1;
            } catch (InterruptedException e) {
                e.printStackTrace( );
            } catch (ExecutionException e) {
                e.printStackTrace( );
            }


        }
        return null;

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

    public ArrayList convertStringToArrayList (String s) {
        s = s.replace("[", "").replace("]", "").trim();
        String[] s1 = s.split(",",0);
        ArrayList al = new ArrayList();
        for (String a : s1) {
            a = a.trim();
            al.add(a.trim());
        }
        return al;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                String input;
                while (true) {
                    Socket socket = serverSocket.accept( );
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    if ((input = in.readLine()) != null) {

                        if (myPort.equals( "11108" )) {
                            if (input.equals( "11112" ) || input.equals( "11116" ) || input.equals( "11120" ) || input.equals( "11124" )  )
                            {
                                // new node joining
                                nodes_in_ring.add( input );
                                nodes = nodes + 1;
                                Log.v( "node svalie " , Integer.toString( nodes) );
                                Collections.sort( nodes_in_ring, new sortUsingHash() );
                                try {

                                    for (String remotePort : REMOTE_PORTS) {


                                        Socket socket_out = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(remotePort));

                                        try {

                                            PrintWriter out = new PrintWriter(socket_out.getOutputStream(), true);

                                            out.println(nodes_in_ring.toString());
                                            out.flush();

                                        } catch (IOException e) {

                                        }
                                        socket.close();
                                    }

                                } catch (UnknownHostException e) {
                                    Log.e(TAG, "Sending present_nodes: UnknownHostException");

                                } catch (IOException e) {
                                    Log.e(TAG, "Sending present_nodes: socket IOException");
                                }

                            }

                            else if(input.contains( "=" )) {
                                input = input.substring( 1 );
                                String[] k = input.split( ":" );
                                SharedPreferences sp = getContext( ).getSharedPreferences( STORAGE_FILE, Context.MODE_PRIVATE );
                                SharedPreferences.Editor e = sp.edit( );
                                e.putString( k[0],k[1] );
                                e.apply( );
                            }

                            else if (input.contains( "[" )) {
                                present_nodes = convertStringToArrayList(input);

                            }
                            else if (input.contains( "QUE" )){
                                String key = input.substring( 3 );
                                String res = utility.getLocalDataByKey(key, getContext());
                                try {
                                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                    out.println(res);
                                    out.flush();
                                } catch (IOException e) {
                                    Log.e(TAG, "not able to send messages");
                                }
                                socket.close();
                            }
                            else if(input.contains( "DEL" )){
                                String key = input.substring( 3 );
                                utility.delLocalDataByKey( key,getContext() );

                            }
                            else if (input.contains( "GLOBAL" )) {
                                String indicator = "@";
                                Cursor c = query( mUri,null,indicator,null, null );
                                String temp_result = "";
                                String out_strin = "";
                                HashMap<String, String> hm = new HashMap<String, String>(  );
                                if(c.moveToFirst()){
                                    while(c.isAfterLast() == false){
                                        int keyIndex = c.getColumnIndex(KEY_FIELD);
                                        int valueIndex = c.getColumnIndex(VALUE_FIELD);
                                        String returnKey = c.getString(keyIndex);
                                        String returnValue = c.getString(valueIndex);
                                        hm.put( returnKey, returnValue );
                                        temp_result = returnKey + "?" + returnValue;
                                        out_strin = out_strin + ";" + temp_result;

                                        c.moveToNext();
                                    }
                                }
                                try {
                                    if(out_strin.length()>0){
                                        String final_out = out_strin.substring(1,out_strin.length());
                                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                        out.println(final_out);
                                        out.flush();
                                        c.close();
                                    }
                                    else{
                                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                        out.println("NONE");
                                        out.flush();
                                        c.close();
                                    }
                                } catch (IOException e) {
                                }
                                socket.close();
                            }

                        }
                        else if ((myPort.equals("11112") || myPort.equals("11116") || myPort.equals("11120") || myPort.equals("11124"))) {

                             if(input.contains( "=" )) {

                                input = input.substring( 1 );
                                Log.i( "inpty msg", input );
                                String[] k = input.split( ":" );
                                SharedPreferences sp = getContext( ).getSharedPreferences( STORAGE_FILE, Context.MODE_PRIVATE );
                                SharedPreferences.Editor e = sp.edit( );
                                e.putString( k[0],k[1] );
                                e.apply( );

                            }

                            else if (input.contains( "[" )) {
                                nodes +=1;
                                present_nodes =  convertStringToArrayList(input);

                            }
                             else if (input.contains( "QUE" )){
                                 String key = input.substring( 3 );
                                 String res = utility.getLocalDataByKey(key, getContext());
                                 try {

                                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                     out.println(res);
                                     out.flush();



                                 } catch (IOException e) {
                                     Log.e(TAG, "not able to send messages");
                                 }
                                 socket.close();
                             }
                             else if(input.contains( "DEL" )){
                                 String key = input.substring( 3 );
                                 utility.delLocalDataByKey( key,getContext() );
                             }
                            else if(input.equals("GLOBAL")){
                                String select = "@";
                                Cursor star_cursor = query(mUri,null,select,null,null);
                                String temp_result = "";
                                String out_strin = "";
                                if(star_cursor.moveToFirst()){
                                    while(star_cursor.isAfterLast() == false){
                                        int keyIndex = star_cursor.getColumnIndex(KEY_FIELD);
                                        int valueIndex = star_cursor.getColumnIndex(VALUE_FIELD);
                                        String returnKey = star_cursor.getString(keyIndex);
                                        String returnValue = star_cursor.getString(valueIndex);
                                        temp_result = returnKey + "?" + returnValue;
                                        out_strin = out_strin + ";" + temp_result;
                                        star_cursor.moveToNext();
                                    }
                                }

                                try {

                                    if(out_strin.length()>0){
                                        String final_out = out_strin.substring(1,out_strin.length());
                                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                                        out.println(final_out);
                                        out.flush();
                                        star_cursor.close();

                                    }
                                    else{

                                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                                        out.println("NONE");
                                        out.flush();
                                        star_cursor.close();

                                    }

                                } catch (IOException e) {

                                }
                                socket.close();
                            }
                        }
                    }
                }
            }catch(Exception e) {

            }
            return null;

        }
    }


    private class ClientTask extends AsyncTask<String, Void , String> {

        @Override
        protected String doInBackground( String... msgs ){

            try {
                  if(msgs[0].contains( "=" )){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    String temp_msg = msgs[0];
                    try {
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(temp_msg);
                        out.flush();

                    } catch (IOException e) {
                        Log.e(TAG, "Can't  send messages");
                    }
                    socket.close();
                }
                else if(msgs[0].contains( "QUE" )){

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    try {

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(msgs[0]);
                        out.flush();
                        BufferedReader client_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String temp_in;
                        while(true) {
                            if ((temp_in = client_in.readLine()) != null){
                                break;
                            }
                        }
                        return temp_in;

                    } catch (IOException e) {
                        Log.e(TAG, "Can't  send messages");
                    }
                    socket.close();
                }
                else if(msgs[0].contains( "DEL" )){
                      Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                              Integer.parseInt(msgs[1]));
                      String temp_msg = msgs[0];

                      try {


                          PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                          out.println(temp_msg);
                          out.flush();

                      } catch (IOException e) {
                          Log.e(TAG, "Can't  send messages");
                      }
                      socket.close();
                  }


                else if(msgs[0].contains( "," )) {
                    // requesting for all data
                    String temp_port = msgs[0].split( "," )[1];
                    String temp_msg = msgs[0].split( "," )[0];

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(temp_port));
                    try {

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(temp_msg);
                        out.flush();
                        BufferedReader client_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String temp_in;
                        while(true) {
                            if ((temp_in = client_in.readLine()) != null){
                                break;
                            }
                        }
                        return temp_in;

                    } catch (IOException e) {
                        Log.e(TAG, "Can't  send messages");
                    }
                    socket.close();

                }
                else {

                    for (String remotePort : REMOTE_PORTS) {
                        Socket socket = new Socket( InetAddress.getByAddress( new byte[]{10, 0, 2, 2} ),
                                Integer.parseInt( remotePort ) );
                        String msgToSend = msgs[0];
                        try {
                            PrintWriter out = new PrintWriter( socket.getOutputStream( ), true );
                            out.println( msgToSend );
                            out.flush( );
                        } catch (IOException e) {

                        }
                        socket.close( );
                    }
                }
            }
            catch (IOException e) {

            }
            return null;
        }
    }


}

