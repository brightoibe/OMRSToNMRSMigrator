/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

/**
 *
 * @author The Bright
 */
public class ConnectionParameters {

    /**
     * @return the hostIP
     */
    public String getHostIP() {
        return hostIP;
    }

    /**
     * @param hostIP the hostIP to set
     */
    public void setHostIP(String hostIP) {
        this.hostIP = hostIP;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the portNo
     */
    public String getPortNo() {
        return portNo;
    }

    /**
     * @param portNo the portNo to set
     */
    public void setPortNo(String portNo) {
        this.portNo = portNo;
    }

    /**
     * @return the database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * @param database the database to set
     */
    public void setDatabase(String database) {
        this.database = database;
    }
    private String hostIP;
    private String username;
    private String password;
    private String portNo;
    private String database;
}
