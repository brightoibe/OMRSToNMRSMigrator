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
public class EncounterProvider {

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
     * @return the encounterRoleID
     */
    public int getEncounterRoleID() {
        return encounterRoleID;
    }

    /**
     * @param encounterRoleID the encounterRoleID to set
     */
    public void setEncounterRoleID(int encounterRoleID) {
        this.encounterRoleID = encounterRoleID;
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
        hash = 47 * hash + this.encounterID;
        hash = 47 * hash + Objects.hashCode(this.providerID);
        hash = 47 * hash + this.providerID;
        return hash;
    }
    @Override
    public boolean equals(Object encTarget){
        boolean ans=false;
        EncounterProvider enc1=null;
        if(encTarget instanceof EncounterProvider){
            enc1=(EncounterProvider)encTarget;
            if(this.encounterID==enc1.encounterID && this.providerID==enc1.providerID && this.encounterRoleID==enc1.encounterRoleID){
                ans=true;
            }
        }
        return ans;
     }
    private int encounterID;
    private int providerID;
    private int creator;
    private int encounterRoleID;
    private Date dateCreated;
    private int voided;
    private String uuid;
    private boolean exists;
}
