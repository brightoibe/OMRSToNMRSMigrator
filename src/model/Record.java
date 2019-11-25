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
public class Record {

    /**
     * @return the hasMigrated
     */
    public boolean isHasMigrated() {
        return hasMigrated;
    }

    /**
     * @param hasMigrated the hasMigrated to set
     */
    public void setHasMigrated(boolean hasMigrated) {
        this.hasMigrated = hasMigrated;
    }
   private int medicationNameConceptID;
   private int wrappingGroupingConceptID;
   private boolean isResolved;
   private boolean hasMigrated;
    /**
     * @return the isResolved
     */
    public boolean isIsResolved() {
        isResolved = false;
        if (wrappingGroupingConceptID != 0 && medicationNameConceptID != 0) {
            isResolved = true;
        }

        return isResolved;
    }

    /**
     * @param isResolved the isResolved to set
     */
    public void setIsResolved(boolean isResolved) {
        this.isResolved = isResolved;
    }
    

    /**
     * @return the medicationNameConceptID
     */
    public int getMedicationNameConceptID() {
        return medicationNameConceptID;
    }

    /**
     * @param medicationNameConceptID the medicationNameConceptID to set
     */
    public void setMedicationNameConceptID(int medicationNameConceptID) {
        this.medicationNameConceptID = medicationNameConceptID;
    }

    /**
     * @return the wrappingGroupingConceptID
     */
    public int getWrappingGroupingConceptID() {
        return wrappingGroupingConceptID;
    }

    /**
     * @param wrappingGroupingConceptID the wrappingGroupingConceptID to set
     */
    public void setWrappingGroupingConceptID(int wrappingGroupingConceptID) {
        this.wrappingGroupingConceptID = wrappingGroupingConceptID;
    }

}
