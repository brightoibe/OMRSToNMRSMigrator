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
public class MapConfig {

    /**
     * @return the omrsFormName
     */
    public String getOmrsFormName() {
        return omrsFormName;
    }

    /**
     * @param omrsFormName the omrsFormName to set
     */
    public void setOmrsFormName(String omrsFormName) {
        this.omrsFormName = omrsFormName;
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
     * @return the csvFileName
     */
    public String getCsvFileName() {
        return csvFileName;
    }

    /**
     * @param csvFileName the csvFileName to set
     */
    public void setCsvFileName(String csvFileName) {
        this.csvFileName = csvFileName;
    }

    /**
     * @return the migrate
     */
    public boolean isMigrate() {
        return migrate;
    }

    /**
     * @param migrate the migrate to set
     */
    public void setMigrate(boolean migrate) {
        this.migrate = migrate;
    }
    private int formID;
    private String omrsFormName;
    private String csvFileName;
    private boolean migrate;
}
