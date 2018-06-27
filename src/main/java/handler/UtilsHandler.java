package handler;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

public class UtilsHandler {
    public final static Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Kolkata"));
    public final static HashMap<String, String> sensorIdAndTopicMap = loadMap();

    public static String current_timestamp_str() {
        return new Date().toString();
    }

    public static String tsToStr(double timestamp) {
        return new Date((long) (timestamp * 1000)).toString();
    }

    public static double tsInSeconds(int year, int month, int day, int hour, int min, int sec) {
        calendar.set(year, month - 1, day, hour, min, sec);
        return calendar.getTime().getTime() / 1000;
    }

    public static void exit_thread() {
        System.exit(-1);
        Thread.currentThread().interrupt();
    }

    public static String makeGetRequest(String urlStr) {
        String responseStr = "";
        try {
            URL urlObj = new URL(urlStr);
            URLConnection urlConnection = null;
            urlConnection = urlObj.openConnection();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            urlConnection.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null)
                responseStr += inputLine;
            in.close();
        } catch (IOException e) {
            LogHandler.logError("[GET REQUEST(" + urlStr + ")]" + e.getMessage());
        }
        return responseStr;
    }

    public static String getTopic(String sensorId) {
        String topic = null;
        topic = sensorIdAndTopicMap.get(sensorId);
        return topic;
    }
    public static HashMap<String,String> loadMap(){
        HashMap<String,String> map = new HashMap<String, String>();
        String jsonStr = makeGetRequest("http://10.129.149.9:8080/meta/sensors/");
        Gson gson = new Gson();
        JsonArray sensorArray = gson.fromJson(jsonStr, JsonArray.class);
        for (JsonElement sensorElement : sensorArray) {
            JsonObject sensorObj = (JsonObject) sensorElement;
            String location = sensorObj.get("location").getAsString();
            String sensor_id = sensorObj.get("sensor_id").getAsString();
            String data = "data";
            int channel = sensorObj.get("channel").getAsInt();
            String mac_id = sensorObj.get("mac_id").getAsString();
            String sensor_type = "";
            switch (channel) {
                case 1:
                    sensor_type = "rish";
                    break;
                case 3:
                    sensor_type = "sch";
                    break;
                case 5:
                    sensor_type = "temp";
                    break;
                case 7:
                    sensor_type = "dht";
                    break;
                default:
                    sensor_type = "";
            }
            String topic  = data+"/"+location+"/"+sensor_type+"/"+mac_id;
            map.put(sensor_id,topic);
        }
        return map;
    }


}
