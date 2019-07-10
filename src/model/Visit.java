/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

import java.util.Date;
import java.util.Objects;

/**
 *
 * @author The Bright
 */
public class Visit {

    /**
     * @return the visitID
     */
    public int getVisitID() {
        return visitID;
    }

    /**
     * @param visitID the visitID to set
     */
    public void setVisitID(int visitID) {
        this.visitID = visitID;
    }

    /**
     * @return the patientID
     */
    public int getPatientID() {
        return patientID;
    }

    /**
     * @param patientID the patientID to set
     */
    public void setPatientID(int patientID) {
        this.patientID = patientID;
    }

    /**
     * @return the visitTypeID
     */
    public int getVisitTypeID() {
        return visitTypeID;
    }

    /**
     * @param visitTypeID the visitTypeID to set
     */
    public void setVisitTypeID(int visitTypeID) {
        this.visitTypeID = visitTypeID;
    }

    /**
     * @return the dateStarted
     */
    public Date getDateStarted() {
        return dateStarted;
    }

    /**
     * @param dateStarted the dateStarted to set
     */
    public void setDateStarted(Date dateStarted) {
        this.dateStarted = dateStarted;
    }

    /**
     * @return the dateStopped
     */
    public Date getDateStopped() {
        return dateStopped;
    }

    /**
     * @param dateStopped the dateStopped to set
     */
    public void setDateStopped(Date dateStopped) {
        this.dateStopped = dateStopped;
    }

    /**
     * @return the locationID
     */
    public int getLocationID() {
        return locationID;
    }

    /**
     * @param locationID the locationID to set
     */
    public void setLocationID(int locationID) {
        this.locationID = locationID;
    }

    /**
     * @return the creator
     */
    public int getCreator() {
        return creator;
    }

    /**
     * @param creator the creator to set
     */
    public void setCreator(int creator) {
        this.creator = creator;
    }

    /**
     * @return the dateCreated
     */
    public Date getDateCreated() {
        return dateCreated;
    }

    /**
     * @param dateCreated the dateCreated to set
     */
    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    /**
     * @return the voided
     */
    public int getVoided() {
        return voided;
    }

    /**
     * @param voided the voided to set
     */
    public void setVoided(int voided) {
        this.voided = voided;
    }

    /**
     * @return the uuid
     */
    public String getUuid() {
        return uuid;
    }

    /**
     * @param uuid the uuid to set
     */
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
    
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + this.patientID;
        //hash = 47 * hash + this.encounterID;
        hash = 47 * hash + Objects.hashCode(this.dateStarted);
        //hash = 47 * hash + this.formID;
        return hash;
    }
    @Override
    public boolean equals(Object encTarget){
        boolean ans=false;
        Visit enc1=null;
        if(encTarget instanceof Visit){
            enc1=(Visit)encTarget;
            if(this.patientID==enc1.patientID && this.dateStarted.equals(enc1.dateStarted)){
                ans=true;
            }
        }
        return ans;
     }
    
    
    private int visitID;
    private int patientID;
    private int visitTypeID;
    private Date dateStarted;
    private Date dateStopped;
    private int locationID;
    private int creator;
    private Date dateCreated;
    private int voided;
    private String uuid;
    private boolean exists;

    /**
     * @return the exists
     */
    public boolean isExists() {
        return exists;
    }

    /**
     * @param exists the exists to set
     */
    public void setExists(boolean exists) {
        this.exists = exists;
    }
    
    
}
