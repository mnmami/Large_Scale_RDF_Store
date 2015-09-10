package sbd.model;

/**
 *
 * @author mohamed
 */
public class Stop {
    public String lat;
    public String lon;
    public String place;

    public Stop(String lat, String lon, String place) {
        this.lat = lat;
        this.lon = lon;
        this.place = place;
    }
    
    public Stop() {}


    public String getLat() {
        return lat;
    }

    public String getLon() {
        return lon;
    }
    
    public String getPlace() {
        return place;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public void setPlace(String place) {
        this.place = place;
    }
    
}
