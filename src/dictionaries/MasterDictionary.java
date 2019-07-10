/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dictionaries;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import model.Concept;
import model.ConceptMap;
import model.Demographics;
import model.DisplayScreen;
import model.Encounter;
import model.MapConfig;
import model.Obs;
import model.User;
import org.apache.commons.lang3.StringUtils;
import utils.Converter;
import utils.FileManager;

/**
 *
 * @author The Bright
 */
public class MasterDictionary {

    private DisplayScreen screen;
    private List<Concept> omrsConceptsList;
    private List<Concept> nmrsConceptsList;
    private FileManager mgr;
    private Map<Integer, MapConfig> mapConfigMap = null;
    private Map<Integer, Integer> formIDMap = new HashMap<Integer, Integer>();
    private Map<Integer, Integer> encounterTypeIDMap = new HashMap<Integer, Integer>();
    //private Map<Integer,Integer> encounterTypeMap=new HashMap<Integer,Integer>();
    private Map<Integer, List<ConceptMap>> csvMapDictionaries = new HashMap<Integer, List<ConceptMap>>();
    private List<ConceptMap> conceptMapList = null;

    public MasterDictionary() {
        mgr = new FileManager();
        loadDictionaries();

    }

    public void setDisplayScreen(DisplayScreen screen) {
        this.screen = screen;
        mgr.setScreen(screen);
    }

    public void loadMapConfig() {
        //Load the map config file
        //List<String[]> dataArr = mgr.loadAllData("mapconfig.csv");
        //mapConfigMap = Converter.convertToMapConfigMap(dataArr);
    }

    public void loadMapFiles() {
        List<String[]> dataArr = mgr.loadAllDataInFolder("map");
        conceptMapList = Converter.convertToConceptMapList(dataArr);
    }

    public void loadFormIDMap() {
        formIDMap.put(56, 14);
        formIDMap.put(18, 22);
        formIDMap.put(46, 27);
        formIDMap.put(53, 27);
        formIDMap.put(67, 21);
        formIDMap.put(28, 23);
        formIDMap.put(19, 23);
        formIDMap.put(65, 23);
        formIDMap.put(29, 11);
        formIDMap.put(20, 20);
        formIDMap.put(1, 23);
        formIDMap.put(71, 13);
        formIDMap.put(84, 52);
        formIDMap.put(85, 52);
        formIDMap.put(66, 30);
        formIDMap.put(64, 30);
        formIDMap.put(69, 10);
        formIDMap.put(47, 1);
        formIDMap.put(17, 16);
        formIDMap.put(76, 51);
        formIDMap.put(80, 8);
        formIDMap.put(77, 38);
        formIDMap.put(79, 13);
    }

    public void initializeUserErrorLog() {
        String errorUserFile;
        String[] errorUserHeader;
        
        errorUserHeader = new String[]{
            "user_id",
            "person_id",
            "username",
            "fullname",
            "given_name",
            "family_name",
            "gender",
            "date_created",
            "creator",
            "uuid",
            "date_changed",
            "retired",
            "retired_by"

        };
        
        
        Date currentDate=new Date();
        errorUserFile="ErrorUsers"+Converter.formatHHMMSSDDMONYYYYNoSlash(currentDate)+".csv";
        
        //mgr.initializeWriter(errorDemoFile);
        mgr.initializeWriter(errorUserFile);
        mgr.writeHeaders(errorUserHeader);
        
    }
    public void initializeDemographicErrorLog(){
       String[]  errorDemoHeader = new String[]{
            "person_source_pk",
            "person_uuid",
            "pepfar_id",
            "hosp_id",
            "ehnid",
            "other_id",
            "first_name",
            "last_name",
            "middle_name",
            "adult_enrollment_dt",
            "pead_enrollment_dt",
            "pmtct_enrollment_dt",
            "hei_enrollment_dt",
            "pep_enrollment_dt",
            "dob",
            "age",
            "gender",
            "address1",
            "address2",
            "address_lga",
            "address_state",
            "creator_id",
            "date_created",
            "location_id",
            "creator",
            "location",
            "date_changed"
        };
       Date currentDate=new Date();
       String errorDemoFile="ErrorDemographics"+Converter.formatHHMMSSDDMONYYYYNoSlash(currentDate)+".csv";
       mgr.initializeWriter(errorDemoFile);
       mgr.writeHeaders(errorDemoHeader);
    }
    public void initializeObsErrorLog(){
       String[]  errorObsHeader = new String[]{
            "OBS_ID",
            "PATIENT_ID",
            "ENCOUNTER_ID",
            "PEPFAR_ID",
            "HOSP_ID",
            "VISIT_DATE",
            "PMM_FORM",
            "CONCEPT_ID",
            "VARIABLE_NAME",
            "VARIABLE_VALUE",
            "ENTERED_BY",
            "DATE_CREATED",
            "DATE_CHANGED",
            "PROVIDER",
            "UUID",
            "LOCATION",
            "LOCATION_ID",
            "CREATOR_ID",
            "PROVIDER_ID",
            "VALUE_NUMERIC",
            "VALUE_DATETIME",
            "VALUE_CODED",
            "VALUE_TEXT",
            "VALUE_BOOL",
            "OBS_GROUP_ID",
            "VOIDED",
            "DATE_VOIDED",
            "VOIDED_BY",
            "CHANGED_BY",
            "FORM_ID"};
       Date currentDate=new Date();
       String errorObsFile = "ErrorObs" + Converter.formatHHMMSSDDMONYYYYNoSlash(currentDate)+".csv";
       mgr.initializeWriter(errorObsFile);
       mgr.writeHeaders(errorObsHeader);
       
    }
    public void log(Obs obs){
        mgr.writeCSV(obs);
    }
    public void log(User usr){
        mgr.writeCSV(usr);
    }
    public void log(Demographics demo){
        mgr.writeCSV(demo);
    }
    public void loadEncounterTypeIDMap() {
        encounterTypeIDMap.put(1, 7);
        encounterTypeIDMap.put(2, 3);
        encounterTypeIDMap.put(3, 2);
        encounterTypeIDMap.put(4, 1);
        encounterTypeIDMap.put(7, 5);
        encounterTypeIDMap.put(8, 18);
        encounterTypeIDMap.put(10, 2);
        encounterTypeIDMap.put(13, 15);
        encounterTypeIDMap.put(14, 12);
        encounterTypeIDMap.put(15, 16);
        encounterTypeIDMap.put(16, 10);
        encounterTypeIDMap.put(19, 9);
        encounterTypeIDMap.put(20, 8);
        encounterTypeIDMap.put(21, 11);
        encounterTypeIDMap.put(22, 8);
        encounterTypeIDMap.put(23, 14);
        encounterTypeIDMap.put(27, 13);
        encounterTypeIDMap.put(28, 17);
        encounterTypeIDMap.put(30, 6);
        encounterTypeIDMap.put(40, 19);
        encounterTypeIDMap.put(46, 7);
        encounterTypeIDMap.put(47, 6);
        encounterTypeIDMap.put(50, 2);
        encounterTypeIDMap.put(51, 21);
        encounterTypeIDMap.put(52, 22);
        encounterTypeIDMap.put(53, 23);
    }

    public void loadDictionaries() {
        loadFormIDMap();
        loadEncounterTypeIDMap();
        loadMapFiles();

    }
    public void closeAllResources(){
        mgr.closeAll();
    }
    public ConceptMap isMapped(Obs omrsObs) {
        ConceptMap cmap = null;
        if (omrsObs.getValueCoded() != 0) {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getValueCoded(), omrsObs.getConceptID());
        } else {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getConceptID());
        }
        return cmap;
    }

    public void mapToNMRS(Obs omrsObs) {
        ConceptMap cmap = null;
        if (omrsObs.getValueCoded() != 0) {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getValueCoded(), omrsObs.getConceptID());
        } else {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getConceptID());
        }
        if (cmap != null) {
            if (omrsObs.getValueCoded() != 0) {
                omrsObs.setFormID(formIDMap.get(omrsObs.getFormID()));
                omrsObs.setConceptID(cmap.getNmrsQuestionConcept());
                omrsObs.setValueCoded(cmap.getNmrsConceptID());
            } else {
                omrsObs.setFormID(formIDMap.get(omrsObs.getFormID()));
                omrsObs.setConceptID(cmap.getNmrsConceptID());
            }
        } else {
            omrsObs.setAllowed(false);
        }
    }

    public void mapToNMRS(Encounter omrsEncounter) {
        if (omrsEncounter != null && formIDMap.containsKey(omrsEncounter.getFormID()) && encounterTypeIDMap.containsKey(formIDMap.get(omrsEncounter.getFormID()))) {
            int omrsFormID = omrsEncounter.getFormID();
            omrsEncounter.setFormID(formIDMap.get(omrsFormID));
            int nmrsFormID = formIDMap.get(omrsFormID);
            int nmrsEncounterType = encounterTypeIDMap.get(nmrsFormID);
            omrsEncounter.setEncounterType(nmrsEncounterType);
            System.out.println("Encounter mapping done...");
        } else {
            if (omrsEncounter != null) {
                omrsEncounter.setAllowed(false);
            }
        }
    }

    public ConceptMap getConceptMapFor(int omrsFormID, int omrsConceptID) {
        ConceptMap conceptMap = null;
        for (ConceptMap cmap : conceptMapList) {
            if (cmap.getOmrsFormID() == omrsFormID && cmap.getOmrsConceptID() == omrsConceptID && cmap.getOmrsQuestionConcept() == 0) {
                conceptMap = cmap;
            }
        }
        return conceptMap;
    }

    public ConceptMap getConceptMapFor(int omrsFormID, int omrsConceptID, int questionConceptID) {
        ConceptMap conceptMap = null;
        for (ConceptMap cmap : conceptMapList) {
            if (cmap.getOmrsFormID() == omrsFormID && cmap.getOmrsConceptID() == omrsConceptID && cmap.getOmrsQuestionConcept() == questionConceptID) {
                conceptMap = cmap;
            }
        }
        return conceptMap;
    }

}
