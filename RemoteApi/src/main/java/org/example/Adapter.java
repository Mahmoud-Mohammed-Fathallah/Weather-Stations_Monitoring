package org.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

import org.example.MocMessage;
import org.json.JSONObject;

import com.google.gson.Gson;
/*
 * Adapter class to convert the data from weather API to the format that the central station can understand
 * 
 */
public class Adapter {


    private static String buildUrl(double latitude, double longitude) {
        String baseUrl = "https://api.open-meteo.com/v1/forecast";
        String parameters = String.format("?latitude=%s&longitude=%s&hourly=relativehumidity_2m&current_weather=true&timeformat=unixtime&forecast_days=1", latitude, longitude);
        return baseUrl + parameters;
    }
public static String fetchWeatherData() throws Exception {
        String url =  buildUrl(getRandomLatitude(), getRandomLongitude()); 
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuilder content = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }

        in.close();
        conn.disconnect();

        return content.toString();
    }

    public static String formatMessage(String weatherData,long s_no) {
        
        JSONObject json = new JSONObject(weatherData);
        JSONObject currentWeather = json.getJSONObject("current_weather");

       

      
        int humidity  = json.getJSONObject("hourly").getJSONArray("relativehumidity_2m").getInt(0);
        int temperature = (int) (currentWeather.getDouble("temperature") * 9/5 + 32); // Convert to Fahrenheit
        int windSpeed = (int) currentWeather.getDouble("windspeed");

        weather weather = new weather();
        weather.setHumidity(humidity);
        weather.setTemperature(temperature);
        weather.setWindSpeed(windSpeed);

        MocMessage mocMessage = new MocMessage(Long.parseLong(System.getenv("STATION_ID")),s_no, "low",System.currentTimeMillis(), weather);

        // Randomly change battery status
        mocMessage.randomlyChangeBatteryStatus();
         Gson gson = new Gson();
         String jsonMessage = gson.toJson(mocMessage);
        return jsonMessage;
    }
       private static double getRandomLatitude() {
        Random random = new Random();
        // Latitude ranges from -90 to 90
        return -90 + (90 - (-90)) * random.nextDouble();
    }

    private static double getRandomLongitude() {
        Random random = new Random();
        // Longitude ranges from -180 to 180
        return -180 + (180 - (-180)) * random.nextDouble();
    }
}
