/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

import java.util.Date;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author The Bright
 */
public class Provider {

    /**
     * @return the exist
     */
    public boolean isExist() {
        return exist;
    }

    /**
     * @param exist the exist to set
     */
    public void setExist(boolean exist) {
        this.exist = exist;
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
     * @return the personID
     */
    public int getPersonID() {
        return personID;
    }

    /**
     * @param personID the personID to set
     */
    public void setPersonID(int personID) {
        this.personID = personID;
    }

    /**
     * @return the providerName
     */
    public String getProviderName() {
        return providerName;
    }

    /**
     * @param providerName the providerName to set
     */
    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    /**
     * @return the identifier
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @param identifier the identifier to set
     */
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
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
     * @return the retired
     */
    public int getRetired() {
        return retired;
    }

    /**
     * @param retired the retired to set
     */
    public void setRetired(int retired) {
        this.retired = retired;
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

    /**
     * @return the providerRoleID
     */
    public int getProviderRoleID() {
        return providerRoleID;
    }

    /**
     * @param providerRoleID the providerRoleID to set
     */
    public void setProviderRoleID(int providerRoleID) {
        this.providerRoleID = providerRoleID;
    }
      @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + this.personID;
        hash = 47 * hash + Objects.hashCode(this.identifier);
        return hash;
    }
    @Override
    public boolean equals(Object encTarget){
        boolean ans=false;
        Provider enc1=null;
        if(encTarget!=null && encTarget instanceof Provider){
            enc1=(Provider)encTarget;
            if(this.personID==enc1.personID && StringUtils.equals(this.identifier, enc1.getIdentifier())){
                ans=true;
                
            }
        }
        return ans;
     }
    private int providerID;
    private int personID;
    private String providerName;
    private String identifier;
    private int creator;
    private Date dateCreated;
    private int retired;
    private String uuid;
    private int providerRoleID;
    private boolean exist;
    
}
