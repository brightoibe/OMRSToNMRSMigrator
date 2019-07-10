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
public class Obs {

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
    private int obsID;
    private int patientID;
    private int encounterID;
    private String pepfarID;
    private String hospID;
    private Date visitDate;
    private String formName;
    private int conceptID;
    private String variableName;
    private String variableValue;
    private String enteredBy;
    private Date dateEntered;
    private Date dateChanged;
    private String provider;
    private String uuid;
    private String locationName;
    private int locationID;
    private Date valueDate;
    private String valueText;
    private int valueCoded;
    private double valueNumeric;
    private boolean valueBoolean;
    private int creator;
    private int obsGroupID;
    private int providerID;
    private int voided;
    private int voidedBy;
    private int changedBy;
    private Date dateVoided;
    private int formID;
    private boolean exist;
    private boolean allowed;

    /**
     * @return the obsID
     */
    public int getObsID() {
        return obsID;
    }

    /**
     * @param obsID the obsID to set
     */
    public void setObsID(int obsID) {
        this.obsID = obsID;
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
     * @return the pepfarID
     */
    public String getPepfarID() {
        return pepfarID;
    }

    /**
     * @param pepfarID the pepfarID to set
     */
    public void setPepfarID(String pepfarID) {
        this.pepfarID = pepfarID;
    }

    /**
     * @return the hospID
     */
    public String getHospID() {
        return hospID;
    }

    /**
     * @param hospID the hospID to set
     */
    public void setHospID(String hospID) {
        this.hospID = hospID;
    }

    /**
     * @return the visitDate
     */
    public Date getVisitDate() {
        return visitDate;
    }

    /**
     * @param visitDate the visitDate to set
     */
    public void setVisitDate(Date visitDate) {
        this.visitDate = visitDate;
    }

    /**
     * @return the formName
     */
    public String getFormName() {
        return formName;
    }

    /**
     * @param formName the formName to set
     */
    public void setFormName(String formName) {
        this.formName = formName;
    }

    /**
     * @return the conceptID
     */
    public int getConceptID() {
        return conceptID;
    }

    /**
     * @param conceptID the conceptID to set
     */
    public void setConceptID(int conceptID) {
        this.conceptID = conceptID;
    }

    /**
     * @return the variableName
     */
    public String getVariableName() {
        return variableName;
    }

    /**
     * @param variableName the variableName to set
     */
    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    /**
     * @return the variableValue
     */
    public String getVariableValue() {
        return variableValue;
    }

    /**
     * @param variableValue the variableValue to set
     */
    public void setVariableValue(String variableValue) {
        this.variableValue = variableValue;
    }

    /**
     * @return the enteredBy
     */
    public String getEnteredBy() {
        return enteredBy;
    }

    /**
     * @param enteredBy the enteredBy to set
     */
    public void setEnteredBy(String enteredBy) {
        this.enteredBy = enteredBy;
    }

    /**
     * @return the dateEntered
     */
    public Date getDateEntered() {
        return dateEntered;
    }

    /**
     * @param dateEntered the dateEntered to set
     */
    public void setDateEntered(Date dateEntered) {
        this.dateEntered = dateEntered;
    }

    /**
     * @return the dateChanged
     */
    public Date getDateChanged() {
        return dateChanged;
    }

    /**
     * @param dateChanged the dateChanged to set
     */
    public void setDateChanged(Date dateChanged) {
        this.dateChanged = dateChanged;
    }

    /**
     * @return the provider
     */
    public String getProvider() {
        return provider;
    }

    /**
     * @param provider the provider to set
     */
    public void setProvider(String provider) {
        this.provider = provider;
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
     * @return the locationName
     */
    public String getLocationName() {
        return locationName;
    }

    /**
     * @param locationName the locationName to set
     */
    public void setLocationName(String locationName) {
        this.locationName = locationName;
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
     * @return the valueDate
     */
    public Date getValueDate() {
        return valueDate;
    }

    /**
     * @param valueDate the valueDate to set
     */
    public void setValueDate(Date valueDate) {
        this.valueDate = valueDate;
    }

    /**
     * @return the valueText
     */
    public String getValueText() {
        return valueText;
    }

    /**
     * @param valueText the valueText to set
     */
    public void setValueText(String valueText) {
        this.valueText = valueText;
    }

    /**
     * @return the valueCoded
     */
    public int getValueCoded() {
        return valueCoded;
    }

    /**
     * @param valueCoded the valueCoded to set
     */
    public void setValueCoded(int valueCoded) {
        this.valueCoded = valueCoded;
    }

    /**
     * @return the valueNumeric
     */
    public double getValueNumeric() {
        return valueNumeric;
    }

    /**
     * @param valueNumeric the valueNumeric to set
     */
    public void setValueNumeric(double valueNumeric) {
        this.valueNumeric = valueNumeric;
    }

    /**
     * @return the valueBoolean
     */
    public boolean isValueBoolean() {
        return valueBoolean;
    }

    /**
     * @param valueBoolean the valueBoolean to set
     */
    public void setValueBoolean(boolean valueBoolean) {
        this.valueBoolean = valueBoolean;
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
     * @return the obsGroupID
     */
    public int getObsGroupID() {
        return obsGroupID;
    }

    /**
     * @param obsGroupID the obsGroupID to set
     */
    public void setObsGroupID(int obsGroupID) {
        this.obsGroupID = obsGroupID;
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
     * @return the voidedBy
     */
    public int getVoidedBy() {
        return voidedBy;
    }

    /**
     * @param voidedBy the voidedBy to set
     */
    public void setVoidedBy(int voidedBy) {
        this.voidedBy = voidedBy;
    }

    /**
     * @return the changedBy
     */
    public int getChangedBy() {
        return changedBy;
    }

    /**
     * @param changedBy the changedBy to set
     */
    public void setChangedBy(int changedBy) {
        this.changedBy = changedBy;
    }

    /**
     * @return the dateVoided
     */
    public Date getDateVoided() {
        return dateVoided;
    }

    /**
     * @param dateVoided the dateVoided to set
     */
    public void setDateVoided(Date dateVoided) {
        this.dateVoided = dateVoided;
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
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + this.patientID;
        hash = 47 * hash + this.encounterID;
        //hash = 47 * hash + Objects.hashCode(this.dateStarted);
        hash = 47 * hash + this.conceptID;
        return hash;
    }
    
     @Override
    public boolean equals(Object obsTarget){
        boolean ans=false;
        Obs obs1=null;
        if(obsTarget instanceof Obs){
            obs1=(Obs)obsTarget;
            if(this.obsID==obs1.obsID){
                ans=true;
            }
        }
        return ans;
     }
     
}
