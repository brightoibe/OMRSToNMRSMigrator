/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author The Bright
 */
public class Demographics {

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
     * @return the errorList
     */
    public List<String> getErrorList() {
        return errorList;
    }

    /**
     * @param errorList the errorList to set
     */
    public void setErrorList(List<String> errorList) {
        this.errorList = errorList;
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
     * @return the patientUUID
     */
    public String getPatientUUID() {
        return patientUUID;
    }

    /**
     * @param patientUUID the patientUUID to set
     */
    public void setPatientUUID(String patientUUID) {
        this.patientUUID = patientUUID;
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
     * @return the eHNID
     */
    public String geteHNID() {
        return eHNID;
    }

    /**
     * @param eHNID the eHNID to set
     */
    public void seteHNID(String eHNID) {
        this.eHNID = eHNID;
    }

    /**
     * @return the otherID
     */
    public String getOtherID() {
        return otherID;
    }

    /**
     * @param otherID the otherID to set
     */
    public void setOtherID(String otherID) {
        this.otherID = otherID;
    }

    /**
     * @return the firstName
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName the firstName to set
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return the lastName
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName the lastName to set
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return the middleName
     */
    public String getMiddleName() {
        return middleName;
    }

    /**
     * @param middleName the middleName to set
     */
    public void setMiddleName(String middleName) {
        this.middleName = middleName;
    }

    /**
     * @return the adultEnrollmentDt
     */
    public Date getAdultEnrollmentDt() {
        return adultEnrollmentDt;
    }

    /**
     * @param adultEnrollmentDt the adultEnrollmentDt to set
     */
    public void setAdultEnrollmentDt(Date adultEnrollmentDt) {
        this.adultEnrollmentDt = adultEnrollmentDt;
    }

    /**
     * @return the peadEnrollmentDt
     */
    public Date getPeadEnrollmentDt() {
        return peadEnrollmentDt;
    }

    /**
     * @param peadEnrollmentDt the peadEnrollmentDt to set
     */
    public void setPeadEnrollmentDt(Date peadEnrollmentDt) {
        this.peadEnrollmentDt = peadEnrollmentDt;
    }

    /**
     * @return the pmtctEnrollmentDt
     */
    public Date getPmtctEnrollmentDt() {
        return pmtctEnrollmentDt;
    }

    /**
     * @param pmtctEnrollmentDt the pmtctEnrollmentDt to set
     */
    public void setPmtctEnrollmentDt(Date pmtctEnrollmentDt) {
        this.pmtctEnrollmentDt = pmtctEnrollmentDt;
    }

    /**
     * @return the heiEnrollmentDt
     */
    public Date getHeiEnrollmentDt() {
        return heiEnrollmentDt;
    }

    /**
     * @param heiEnrollmentDt the heiEnrollmentDt to set
     */
    public void setHeiEnrollmentDt(Date heiEnrollmentDt) {
        this.heiEnrollmentDt = heiEnrollmentDt;
    }

    /**
     * @return the pepEnrollmentDt
     */
    public Date getPepEnrollmentDt() {
        return pepEnrollmentDt;
    }

    /**
     * @param pepEnrollmentDt the pepEnrollmentDt to set
     */
    public void setPepEnrollmentDt(Date pepEnrollmentDt) {
        this.pepEnrollmentDt = pepEnrollmentDt;
    }

    /**
     * @return the dateOfBirth
     */
    public Date getDateOfBirth() {
        return dateOfBirth;
    }

    /**
     * @param dateOfBirth the dateOfBirth to set
     */
    public void setDateOfBirth(Date dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    /**
     * @return the age
     */
    public int getAge() {
        return age;
    }

    /**
     * @param age the age to set
     */
    public void setAge(int age) {
        this.age = age;
    }

    /**
     * @return the ageMnt
     */
    public int getAgeMnt() {
        return ageMnt;
    }

    /**
     * @param ageMnt the ageMnt to set
     */
    public void setAgeMnt(int ageMnt) {
        this.ageMnt = ageMnt;
    }

    /**
     * @return the gender
     */
    public String getGender() {
        return gender;
    }

    /**
     * @param gender the gender to set
     */
    public void setGender(String gender) {
        this.gender = gender;
    }

    /**
     * @return the address1
     */
    public String getAddress1() {
        return address1;
    }

    /**
     * @param address1 the address1 to set
     */
    public void setAddress1(String address1) {
        this.address1 = address1;
    }

    /**
     * @return the address2
     */
    public String getAddress2() {
        return address2;
    }

    /**
     * @param address2 the address2 to set
     */
    public void setAddress2(String address2) {
        this.address2 = address2;
    }

    /**
     * @return the address_lga
     */
    public String getAddress_lga() {
        return address_lga;
    }

    /**
     * @param address_lga the address_lga to set
     */
    public void setAddress_lga(String address_lga) {
        this.address_lga = address_lga;
    }

    /**
     * @return the address_state
     */
    public String getAddress_state() {
        return address_state;
    }

    /**
     * @param address_state the address_state to set
     */
    public void setAddress_state(String address_state) {
        this.address_state = address_state;
    }

    /**
     * @return the creatorID
     */
    public int getCreatorID() {
        return creatorID;
    }

    /**
     * @param creatorID the creatorID to set
     */
    public void setCreatorID(int creatorID) {
        this.creatorID = creatorID;
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
     * @return the creatorName
     */
    public String getCreatorName() {
        return creatorName;
    }

    /**
     * @param creatorName the creatorName to set
     */
    public void setCreatorName(String creatorName) {
        this.creatorName = creatorName;
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
     * @return the enrollDate
     */
    public Date getEnrollDate() {
        return enrollDate;
    }

    /**
     * @param enrollDate the enrollDate to set
     */
    public void setEnrollDate(Date enrollDate) {
        this.enrollDate = enrollDate;
    }
    private int patientID;
    private String patientUUID;
    private String pepfarID;
    private String hospID;
    private String eHNID;
    private String otherID;
    private String firstName;
    private String lastName;
    private String middleName;
    private Date adultEnrollmentDt;
    private Date peadEnrollmentDt;
    private Date pmtctEnrollmentDt;
    private Date heiEnrollmentDt;
    private Date pepEnrollmentDt;
    private Date dateOfBirth;
    private int age;
    private int ageMnt;
    private String gender;
    private String address1;
    private String address2;
    private String address_lga;
    private String address_state;
    private int creatorID;
    private Date dateCreated;
    private int voided;
    private Date dateChanged;
    private int locationID;
    private String creatorName;
    private String locationName;
    private Date enrollDate;
    private String errorString;
    private boolean exist;
    private List<String> errorList=new ArrayList<>();

    /**
     * @return the errorString
     */
    public String getErrorString() {
        String error="";
        for(String ele: errorList){
            error+=","+ele;
        }
        return error;
    }

    /**
     * @param errorString the errorString to set
     */
    public void addError(String errorString) {
        this.errorList.add(errorString);
    }
    public void clearError(){
        this.errorList.clear();
    }
    
}
