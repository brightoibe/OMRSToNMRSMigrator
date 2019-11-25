/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dictionaries;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.commons.lang3.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
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

    private Integer[] regimenConceptArr = {7778531, 7778532, 7778533, 7778611};

    private final static int REGIMEN_LINE_AT_ART_START = 7778531;
    private final static int FIRST_LINE_REGIMEN_AT_ART_START = 7778532;
    private final static int SECOND_LINE_REGIMEN_AT_ART_START = 7778533;
    private final static int THIRD_LINE_REGIMEN_AT_ART_START = 7778611;

    private final static int ADULT_FIRST_LINE_REGIMEN = 164506;
    private final static int ADULT_SECOND_LINE_REGIMEN = 164513;
    private final static int ADULT_THIRD_LINE_REGIMEN = 165702;

    private final static int CHILD_FIRST_LINE_REGIMEN = 164507;
    private final static int CHILD_SECOND_LINE_REGIMEN = 164514;
    private final static int CHILD_THIRD_LINE_REGIMEN = 165703;

    private final static int FIRST_LINE_REGIMEN = 7778108;
    private final static int SECOND_LINE_REGIMEN = 7778109;
    private final static int THIRD_LINE_REGIMEN = 7778611;

    private final static int NMRS_NEXT_APPOINTMENT_DATE_CONCEPT_ID = 165036;

    private final static int OMRS_DRUG_NAME_CONCEPT_ID = 7778364;

    private final static int OMRS_ADULT_PHARMACY_ORDER_FORM_ID = 46;
    private final static int OMRS_PED_PHARMACY_ORDER_FORM_ID = 53;

    private final static int OMRS_DRUG_ORDER_GROUPING_CONCEPT = 7778408;

    private final static int NMRS_ARV_GROUPING_CONCEPT_ID = 162240;
    private final static int NMRS_OI_GROUPING_CONCEPT_ID = 165726;
    private final static int NMRS_ANTI_TB_GROUPING_CONCEPT_ID = 165728;

    private final static int NMRS_PHARMACY_FORM_ID = 27;

    private Integer[] specialConceptArr = {7778408};
    private List<Integer> regimenConceptlist = new ArrayList<Integer>();
    private List<Integer> specialConceptList = new ArrayList<Integer>();
    private List<Integer> arvConceptList = new ArrayList<Integer>();
    private List<Integer> oiConceptList = new ArrayList<Integer>();
    private List<Integer> tbConceptList = new ArrayList<Integer>();

    ;


    public MasterDictionary() {
        mgr = new FileManager();
        loadDictionaries();
        /*
            First Line, Second Line, ThirdLine
            FirstLineRegimenAtStart,SecondLineRegimenAtStart,
         */

    }

    public ConceptMap getConceptMapForRegimenConcepts(int age, ConceptMap cmap) {
        int omrsQuestionConceptID = cmap.getOmrsQuestionConcept();
        int omrsAnswerConceptID = cmap.getOmrsConceptID();
        if (age < 15) {
            switch (omrsQuestionConceptID) {
                case REGIMEN_LINE_AT_ART_START:
                    if (omrsAnswerConceptID == FIRST_LINE_REGIMEN) {
                        cmap.setNmrsConceptID(CHILD_FIRST_LINE_REGIMEN);
                    }
                    if (omrsAnswerConceptID == SECOND_LINE_REGIMEN) {
                        cmap.setNmrsConceptID(CHILD_SECOND_LINE_REGIMEN);
                    }
                    if (omrsAnswerConceptID == THIRD_LINE_REGIMEN) {
                        cmap.setNmrsConceptID(CHILD_THIRD_LINE_REGIMEN);
                    }
                    break;
                case FIRST_LINE_REGIMEN_AT_ART_START:
                    cmap.setNmrsQuestionConcept(CHILD_FIRST_LINE_REGIMEN);
                    break;
                case SECOND_LINE_REGIMEN_AT_ART_START:
                    cmap.setNmrsQuestionConcept(CHILD_SECOND_LINE_REGIMEN);
                    break;
                case THIRD_LINE_REGIMEN_AT_ART_START:
                    cmap.setNmrsQuestionConcept(CHILD_THIRD_LINE_REGIMEN);
                    break;
                default:
                    break;
            }
        }
        if (age >= 15) {
            switch (omrsQuestionConceptID) {
                case REGIMEN_LINE_AT_ART_START:
                    if (omrsAnswerConceptID == FIRST_LINE_REGIMEN) {
                        cmap.setNmrsConceptID(ADULT_FIRST_LINE_REGIMEN);
                    }
                    if (omrsAnswerConceptID == SECOND_LINE_REGIMEN) {
                        cmap.setNmrsConceptID(ADULT_SECOND_LINE_REGIMEN);
                    }
                    if (omrsAnswerConceptID == THIRD_LINE_REGIMEN) {
                        cmap.setNmrsConceptID(ADULT_THIRD_LINE_REGIMEN);
                    }
                    break;
                case FIRST_LINE_REGIMEN_AT_ART_START:
                    cmap.setNmrsQuestionConcept(ADULT_FIRST_LINE_REGIMEN);
                    break;
                case SECOND_LINE_REGIMEN_AT_ART_START:
                    cmap.setNmrsQuestionConcept(ADULT_SECOND_LINE_REGIMEN);
                    break;
                case THIRD_LINE_REGIMEN_AT_ART_START:
                    cmap.setNmrsQuestionConcept(ADULT_THIRD_LINE_REGIMEN);
                    break;
                default:
                    break;
            }
        }
        return cmap;
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

    private void loadARVConceptList() {
        Integer[] arvArray = {960,
            953, 959, 952, 962,
            18, 1187, 961, 957,
            1190, 956, 955, 958,
            1213, 1221, 1222, 1224,
            1225, 1227, 1228, 1533,
            23, 1189, 1186, 1188, 1184,
            17, 954, 1185, 1235, 1236,
            1237, 7778159, 1219, 1232,
            1223, 1233, 1234, 1226, 1184
        };
        arvConceptList.addAll(Arrays.asList(arvArray));
    }

    private void loadOIConceptList() {
        Integer[] oiArray = {
            1192, 1193, 1194, 599,
            1195, 1196, 1198, 1199,
            1200, 1201, 594, 1202,
            1203, 1204, 1206, 1207,
            1208, 1209, 1210, 1211,
            1212, 37, 1214, 1215,
            1216, 963, 1217, 1218,
            1239, 1240, 1241, 949,
            950, 10, 7778153, 7778154,
            592, 7778155, 68, 7778156,
            58, 590, 7778158, 1197,
            1205, 608
        };
        oiConceptList.addAll(Arrays.asList(oiArray));

    }

    private void loadTBConceptList() {
        Integer[] tbArray = {1594, 11, 35, 34,
            66, 36, 39, 38, 40};
        oiConceptList.addAll(Arrays.asList(tbArray));
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
        //formIDMap.put(1, 23);
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
        formIDMap.put(1, 56);
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

        Date currentDate = new Date();
        errorUserFile = "ErrorUsers" + Converter.formatHHMMSSDDMONYYYYNoSlash(currentDate) + ".csv";

        //mgr.initializeWriter(errorDemoFile);
        mgr.initializeWriter(errorUserFile);
        mgr.writeHeaders(errorUserHeader);

    }

    public void initializeDemographicErrorLog() {
        String[] errorDemoHeader = new String[]{
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
        Date currentDate = new Date();
        String errorDemoFile = "ErrorDemographics" + Converter.formatHHMMSSDDMONYYYYNoSlash(currentDate) + ".csv";
        mgr.initializeWriter(errorDemoFile);
        mgr.writeHeaders(errorDemoHeader);
    }

    public void initializeObsErrorLog() {
        String[] errorObsHeader = new String[]{
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
        Date currentDate = new Date();
        String errorObsFile = "ErrorObs" + Converter.formatHHMMSSDDMONYYYYNoSlash(currentDate) + ".csv";
        mgr.initializeWriter(errorObsFile);
        mgr.writeHeaders(errorObsHeader);

    }

    public void log(Obs obs) {
        mgr.writeCSV(obs);
    }

    public void log(User usr) {
        mgr.writeCSV(usr);
    }

    public void log(Demographics demo) {
        mgr.writeCSV(demo);
    }

    public boolean isARVMedication(int valueCoded) {
        boolean ans = false;
        if (arvConceptList.contains(valueCoded)) {
            ans = true;
        }
        return ans;
    }

    public boolean isOIMedication(int valueCoded) {
        boolean ans = false;
        if (oiConceptList.contains(valueCoded)) {
            ans = true;
        }
        return ans;
    }

    public boolean isTBMedication(int valueCoded) {
        boolean ans = false;
        if (tbConceptList.contains(valueCoded)) {
            ans = true;
        }
        return ans;
    }

    public Obs getGroupingConceptObs(Obs obs) {
        Obs groupingObs = null;
        int groupingConceptID = 0;
        if (obs.getFormID() == OMRS_ADULT_PHARMACY_ORDER_FORM_ID || obs.getFormID() == OMRS_PED_PHARMACY_ORDER_FORM_ID) {
            if (obs.getConceptID() == OMRS_DRUG_NAME_CONCEPT_ID) {
                if (isOIMedication(obs.getValueCoded())) {
                    groupingObs = createObsGroupWithID(NMRS_OI_GROUPING_CONCEPT_ID, obs);
                }
                if (isARVMedication(obs.getValueCoded())) {
                    groupingObs = createObsGroupWithID(NMRS_ARV_GROUPING_CONCEPT_ID, obs);
                }
                if (isTBMedication(obs.getValueCoded())) {
                    groupingObs = createObsGroupWithID(NMRS_ANTI_TB_GROUPING_CONCEPT_ID, obs);
                }
            }
        }
        return groupingObs;
    }

    public Set<Obs> createObsGroupSet(List<Obs> obsList) {
        Set<Obs> obsSet = new HashSet<Obs>();
        Obs obsGroupingConcept = null;
        for (Obs ele : obsList) {
            obsGroupingConcept = getGroupingConceptObs(ele);
            obsSet.add(obsGroupingConcept);
        }
        return obsSet;
    }

    public Obs createObsGroupWithID(int obsGroupConceptId, Obs obs) {
        Obs groupingObs = new Obs();
        groupingObs.setObsID(obs.getObsGroupID());
        groupingObs.setConceptID(obsGroupConceptId);
        groupingObs.setCreator(obs.getCreator());
        groupingObs.setEncounterID(obs.getEncounterID());
        groupingObs.setFormID(obs.getFormID());
        groupingObs.setVoided(obs.getVoided());
        groupingObs.setDateVoided(obs.getDateVoided());
        groupingObs.setVoidedBy(obs.getVoidedBy());
        groupingObs.setDateChanged(obs.getDateChanged());
        groupingObs.setChangedBy(obs.getChangedBy());
        groupingObs.setDateEntered(obs.getDateEntered());
        groupingObs.setEnteredBy(obs.getEnteredBy());
        groupingObs.setFormName(obs.getFormName());
        groupingObs.setPatientID(obs.getPatientID());
        groupingObs.setHospID(obs.getHospID());
        groupingObs.setPepfarID(obs.getPepfarID());
        groupingObs.setProvider(obs.getProvider());
        groupingObs.setLocationID(obs.getLocationID());
        groupingObs.setLocationName(obs.getLocationName());
        groupingObs.setVisitDate(obs.getVisitDate());
        groupingObs.setUuid(Converter.generateUUID());
        return groupingObs;
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
        encounterTypeIDMap.put(5, 16);
        encounterTypeIDMap.put(16, 10);
        //encounterTypeIDMap.put(18, 26);
        encounterTypeIDMap.put(19, 9);
        encounterTypeIDMap.put(20, 8);
        encounterTypeIDMap.put(21, 11);
        encounterTypeIDMap.put(22, 26);
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
        encounterTypeIDMap.put(56, 25);
    }

    public void loadDictionaries() {
        loadFormIDMap();
        loadEncounterTypeIDMap();
        loadMapFiles();
        loadRegimenSpecialConcepts();
        loadSpecialConcepts();
        loadARVConceptList();
        loadOIConceptList();
        loadTBConceptList();

    }

    public void closeAllResources() {
        mgr.closeAll();
    }

    public ConceptMap isMapped(Obs omrsObs) {
        ConceptMap cmap = null;
        if (omrsObs.getValueCoded() != 0) {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getValueCoded(), omrsObs.getConceptID());
        } else {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getConceptID());
        }
        if(specialConceptList.contains(omrsObs.getConceptID())){
            cmap=handleSpecialConcepts(omrsObs);
        }
        return cmap;
    }

    public void loadRegimenSpecialConcepts() {
        regimenConceptlist.addAll(Arrays.asList(regimenConceptArr));
    }

    public void loadSpecialConcepts() {
        specialConceptList.addAll(Arrays.asList(specialConceptArr));
    }

    public boolean isRegimenObs(Obs omrsobs) {
        boolean ans = false;
        int conceptID = omrsobs.getConceptID();
        if (regimenConceptlist.contains(conceptID)) {
            ans = true;
        }
        return ans;
    }
    
    public boolean isSpecialConcept(Obs omrsObs){
        boolean ans=false;
        if(specialConceptList.contains(omrsObs.getConceptID())){
            ans=true;
        }
        return ans;
    }

    public void mapToNMRS(Obs omrsObs, Map<Integer, Date> dateMap) {

        ConceptMap cmap = null;
        Date birthdate = dateMap.get(omrsObs.getPatientID());
        LocalDateTime birthDateTime = new LocalDateTime(birthdate);
        LocalDateTime visitDateTime = new LocalDateTime(omrsObs.getVisitDate());
        LocalDateTime nextAppointmentDate = null;
        Period period = new Period(birthDateTime, visitDateTime);
        int age = Math.abs(period.getYears());

        if (omrsObs.getValueCoded() != 0) {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getValueCoded(), omrsObs.getConceptID());
            if (isRegimenObs(omrsObs)) {
                cmap = getConceptMapForRegimenConcepts(age, cmap);
            }
        } else {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getConceptID());

        }
        if(specialConceptList.contains(omrsObs.getConceptID())){
            cmap=handleSpecialConcepts(omrsObs);
        }

        if (cmap != null) {
            if (omrsObs.getValueCoded() != 0) {
                //omrsObs.setFormID(formIDMap.get(omrsObs.getFormID()));
                omrsObs.setFormID(cmap.getNmrsFormID());
                omrsObs.setConceptID(cmap.getNmrsQuestionConcept());
                omrsObs.setValueCoded(cmap.getNmrsConceptID());
            } else {
                //omrsObs.setFormID(formIDMap.get(omrsObs.getFormID()));
                omrsObs.setFormID(cmap.getNmrsFormID());
                omrsObs.setConceptID(cmap.getNmrsConceptID());
            }
           

        } else {
            omrsObs.setAllowed(false);
        }
    }
    public ConceptMap createConceptMap(int omrsFormID,int omrsConceptID,int nmrsFormID,int nmrsConceptID){
        ConceptMap cmap=new ConceptMap();
        cmap.setOmrsFormID(omrsFormID);
        cmap.setOmrsConceptID(omrsConceptID);
        cmap.setOmrsConceptDataType("ConvSet");
        cmap.setNmrsFormID(nmrsFormID);
        cmap.setNmrsConceptID(nmrsConceptID);
        return cmap;
    }
    public int getNMRSGroupingConceptID(int valueCoded){
        int nmrsGroupingConceptID=NMRS_ARV_GROUPING_CONCEPT_ID;
        if(isARVMedication(valueCoded)){
            nmrsGroupingConceptID=NMRS_ARV_GROUPING_CONCEPT_ID;
        }else if(isOIMedication(valueCoded)){
            nmrsGroupingConceptID=NMRS_OI_GROUPING_CONCEPT_ID;
        }else if(isTBMedication(valueCoded)){
            nmrsGroupingConceptID=NMRS_ANTI_TB_GROUPING_CONCEPT_ID;
        }
        return nmrsGroupingConceptID;
    }
    public ConceptMap handleSpecialConcepts(Obs omrsObs) {
        ConceptMap cmap = null;
        if (specialConceptList.contains(omrsObs.getConceptID())) {
            int omrsConceptID = omrsObs.getConceptID();
            int omrsValueCoded = omrsObs.getValueCoded();
            int nmrsConceptID=0;
            if(isARVMedication(omrsValueCoded)){
                cmap=createConceptMap(omrsObs.getFormID(),omrsObs.getConceptID(),NMRS_PHARMACY_FORM_ID,NMRS_ARV_GROUPING_CONCEPT_ID);
            } 
            if(isOIMedication(omrsValueCoded)){
                cmap=createConceptMap(omrsObs.getFormID(),omrsObs.getConceptID(), NMRS_PHARMACY_FORM_ID, NMRS_OI_GROUPING_CONCEPT_ID);
            }
            if(isTBMedication(omrsValueCoded)){
                cmap=createConceptMap(omrsObs.getFormID(),omrsObs.getConceptID(), NMRS_PHARMACY_FORM_ID, NMRS_ANTI_TB_GROUPING_CONCEPT_ID);
            }
            
            
        }
        //if(cmap !=null){
            //omrsObs.setFormID(cmap.getNmrsFormID());
            //omrsObs.setConceptID(cmap.getNmrsConceptID());
       // }
        return cmap;
    }

    public void handleSpecialConcepts2(Obs omrsObs, ConceptMap cmap) {
        if (specialConceptList.contains(omrsObs.getConceptID())) {
            int omrsConceptID = omrsObs.getConceptID();
            int omrsValueCoded = omrsObs.getValueCoded();
            LocalDateTime visitDateTime = new LocalDateTime(omrsObs.getVisitDate());
            LocalDateTime nextAppointmentDate = null;
            if (omrsConceptID == 7777821 && omrsObs.getVisitDate() != null) {
                switch (omrsValueCoded) {
                    case 1570:
                        nextAppointmentDate = visitDateTime.plusWeeks(1);
                        break;
                    case 1571:
                        nextAppointmentDate = visitDateTime.plusWeeks(2);
                        break;
                    case 1628:
                        nextAppointmentDate = visitDateTime.plusWeeks(4);
                        break;
                    case 1574:
                        nextAppointmentDate = visitDateTime.plusMonths(2);
                        break;
                    case 1575:
                        nextAppointmentDate = visitDateTime.plusMonths(3);
                        break;
                    default:
                        break;
                }
                cmap.setNmrsConceptID(NMRS_NEXT_APPOINTMENT_DATE_CONCEPT_ID);
                if (nextAppointmentDate != null) {
                    omrsObs.setValueDate(nextAppointmentDate.toDate());
                    omrsObs.setConceptID(NMRS_NEXT_APPOINTMENT_DATE_CONCEPT_ID);
                }

            }
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

    public ConceptMap getConceptMapFor(int age, Obs omrsObs) {
        ConceptMap cmap = null;
        if (omrsObs.getValueCoded() != 0) {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getValueCoded(), omrsObs.getConceptID());
            if (isRegimenObs(omrsObs)) {
                cmap = getConceptMapForRegimenConcepts(age, cmap);
            }
        } else {
            cmap = getConceptMapFor(omrsObs.getFormID(), omrsObs.getConceptID());

        }
        return cmap;
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
