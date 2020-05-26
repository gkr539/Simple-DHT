package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.content.SharedPreferences;
import android.text.TextUtils;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


public class Utility {

    private static final String STORAGE_FILE = "filedb";
    public String genHash(String input) throws NoSuchAlgorithmException{
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
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


    public void saveData(String msg, Context context) {
        String[] arr_msg = msg.split( "%%" );
        SharedPreferences sp = context.getSharedPreferences( STORAGE_FILE, Context.MODE_PRIVATE );
        SharedPreferences.Editor e = sp.edit( );
        e.putString( arr_msg[0], arr_msg[1] );
        e.apply( );
        Log.i( "insert done", "done");

    }


    public HashMap<String, String> stringToKeyValue( String msgs){
        HashMap<String, String> hm = new HashMap<String, String>(  );
        String[] arr = msgs.split( "!" );


        for (String m : arr) {
            Log.i( "in string to val",  m);
            String[] temp = m.split( "%%" );
            if (temp.length > 1) {
                Log.i( "teempo", Arrays.toString( temp ) );
                hm.put( temp[0], temp[1] );
            }
        }

        return hm;
    }

    public String convertMapToString(HashMap<String, String> h) {

        ArrayList<String> arr = new ArrayList<String>();
        Iterator it = h.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> pair = (Map.Entry) it.next();
            String temp = pair.getKey() + "%%" + pair.getValue() +
                    "%%" + "get" ;

            arr.add(temp);
            it.remove();
        }


        String a = TextUtils.join("!", arr);
        Log.i( "all data", a );
        return TextUtils.join("!", arr);


    }
    public String getLocalDataByKey(String key, Context context) {

        HashMap<String, String> h = new HashMap<String, String>();

        SharedPreferences sharedPref = context.getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);
        String str = "";
        Map<String, ?> keys = sharedPref.getAll();

        for (Map.Entry<String, ?> entry : keys.entrySet()) {
            String temp = entry.getKey();
            if (temp.equals( key )) {
                h.put( entry.getKey( ), entry.getValue( ).toString( ) );
                str = entry.getKey() + "@" + entry.getValue().toString();
            }

        }

        Log.i( "Str in quert ", str );

        return str;




    }

    public String getLocalData(Context context) {

        HashMap<String, String> hm = new HashMap<String, String>();

        SharedPreferences sharedPref = context.getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);

        Map<String, ?> keys = sharedPref.getAll();

        for (Map.Entry<String, ?> entry : keys.entrySet()) {

            hm.put(entry.getKey(), entry.getValue().toString());

        }

        Log.i( "hm data", hm.toString() );
        return convertMapToString( hm );


    }



    public HashMap<String, String> getAllLocalData(Context context) {

        HashMap<String, String> hm = new HashMap<String, String>();

        SharedPreferences sharedPref = context.getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);

        Map<String, ?> keys = sharedPref.getAll();

        for (Map.Entry<String, ?> entry : keys.entrySet()) {

            hm.put(entry.getKey(), entry.getValue().toString());

        }

        return hm;

    }

    public void delAllLocalData( Context context) {


        SharedPreferences sp = context.getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);

        SharedPreferences.Editor e = sp.edit( );
        e.clear();
        e.apply();

    }

    public void delLocalDataByKey(String key, Context context) {

        HashSet<String> h = new HashSet<String>();

        SharedPreferences sp = context.getSharedPreferences(STORAGE_FILE, Context.MODE_PRIVATE);
        Map<String, ?> keys = sp.getAll();

        SharedPreferences.Editor e = sp.edit( );
        for (Map.Entry<String, ?> entry : keys.entrySet()) {
            String temp = entry.getKey();
            if (temp.equals( key )) {
                h.add( entry.getKey( ) );
                e.remove( entry.getKey() );
                e.apply();
                break;
            }
        }

//        SharedPreferences.Editor e = sp.edit( );
//        Iterator<String> it = h.iterator();
//
//        while(it.hasNext()) {
//            e.remove( it.next() );
//        }
        //e.apply();

    }

}
