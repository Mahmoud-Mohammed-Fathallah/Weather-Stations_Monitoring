package org.example;

import java.util.Random;

public class MocMessage {
    private Long id;
    private long s_no;
    private String batteryStatus;
    private Long statusTimestamp;
    private weather weather;

    // Constructor
    public MocMessage(Long id,long s_no, String batteryStatus, Long statusTimestamp, weather target) {
        this.id = id;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = target;
        this.s_no=s_no;
    }
    public MocMessage() {

    }
    // Method to randomly change the battery status based on specified distribution
    public void randomlyChangeBatteryStatus() {
        Random rand = new Random();
        int randomNumber = rand.nextInt(100); // Generate a random number between 0 and 99

        // Change battery status based on specified distribution
        if (randomNumber < 30) {
            batteryStatus = "Low";
        } else if (randomNumber < 70) {
            batteryStatus = "Medium";
        } else {
            batteryStatus = "High";
        }
    }

    // Getters and setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public void setBatteryStatus(String batteryStatus) {
        this.batteryStatus = batteryStatus;
    }

    public Long getStatusTimestamp() {
        return statusTimestamp;
    }

    public void setStatusTimestamp(Long statusTimestamp) {
        this.statusTimestamp = statusTimestamp;
    }

    public weather getTarget() {
        return weather;
    }

    public void setTarget(weather target) {
        this.weather = target;
    }
}
