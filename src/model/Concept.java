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
public class Concept {

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
     * @return the conceptName
     */
    public String getConceptName() {
        return conceptName;
    }

    /**
     * @param conceptName the conceptName to set
     */
    public void setConceptName(String conceptName) {
        this.conceptName = conceptName;
    }

    /**
     * @return the dataType
     */
    public int getDataType() {
        return dataType;
    }

    /**
     * @param dataType the dataType to set
     */
    public void setDataType(int dataType) {
        this.dataType = dataType;
    }
    private int conceptID;
    private String conceptName;
    private int dataType;
}
