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
public class DrugObs {
    private int obsID;
    private int conceptID;
    private int valueCoded;
    private int obsGroupID;
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

}
