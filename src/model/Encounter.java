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
public class Encounter {

    /**
     * @return the allowed
     */
    public boolean isAllowed() {
        return allowed;
    }

    /**
     * @param allowed the allowed to set
     */
    public void setAllowed(boolean allowed) {
        this.allowed = allowed;
    }

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

    /**
     * @return the encounterID
     */
    public int getEncounterID() {
        return encounterID;
    }

    /**
     * @param encounterID the encounterID to set
     */
    public void setEncounterID(int encounterID) {
        this.encounterID = encounterID;
    }

    /**
     * @return the encounterType
     */
    public int getEncounterType() {
        return encounterType;
    }

    /**
     * @param encounterType the encounterType to set
     */
    public void setEncounterType(int encounterType) {
        this.encounterType = encounterType;
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
     * @return the formID
     */
    public int getFormID() {
        return formID;
    }

    /**
     * @param formID the formID to set
     */
    public void setFormID(int formID) {
        this.formID = formID;
    }

    /**
     * @return the encounterDatetime
     */
    public Date getEncounterDatetime() {
        return encounterDatetime;
    }

    /**
     * @param encounterDatetime the encounterDatetime to set
     */
    public void setEncounterDatetime(Date encounterDatetime) {
        this.encounterDatetime = encounterDatetime;
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
        hash = 47 * hash + this.encounterID;
        //hash = 47 * hash + Objects.hashCode(this.encounterDatetime);
        hash = 47 * hash + this.formID;
        return hash;
    }
    @Override
    public boolean equals(Object encTarget){
        boolean ans=false;
        Encounter enc1=null;
        if(encTarget instanceof Encounter){
            enc1=(Encounter)encTarget;
            if(this.encounterID==enc1.encounterID){
                ans=true;
            }
        }
        return ans;
     }
    private int encounterID;
    private int encounterType;
    private int patientID;
    private int locationID;
    private int formID;
    private Date encounterDatetime;
    private int creator;
    private Date dateCreated;
    private int voided;
    private int visitID;
    private int providerID;
    private String uuid;
    private boolean exists;
    private boolean allowed;

    /**
     * @return the providerID
     */
    public int getProviderID() {
        return providerID;
    }

    /**
     * @param providerID the providerID to set
     */
    public void setProviderID(int providerID) {
        this.providerID = providerID;
    }
}
