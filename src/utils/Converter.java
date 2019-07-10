/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.XMLEvent;
import model.ConceptMap;
import model.Demographics;
import model.MapConfig;
import model.Obs;
import model.User;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author openmrsdev
 */
public class Converter {

    private static SimpleDateFormat formatter;
    private static DecimalFormat df = new DecimalFormat();
    private final static String USERSYSTEMIDPREFIX="IHVN";

    public static Date stringToDate(String dateString) {
        Date date = null;
        formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setLenient(false);
        if (dateString == null) {
            return date;
        }
        if (dateString.isEmpty()) {
            return date;
        }
        try {
            date = formatter.parse(dateString);
        } catch (ParseException ex) {
            date = null;

            return date;
        }
        return date;
    }

    public static java.sql.Date convertToSQLDate(java.util.Date olddate) {
        java.sql.Date sqlDate = null;
        if (olddate != null) {
            sqlDate = new java.sql.Date(olddate.getTime());
        }
        return sqlDate;
    }
    public static String generateUserSystemID(User usr){
        String id=null;
        id=StringUtils.trim(USERSYSTEMIDPREFIX+"-"+usr.getUser_id()+"-"+usr.getPerson_id());
        return id;
    }
    public static String formatDateDDMMYYYY(Date date) {
        String dateString = "";
        if (date != null) {
            SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy");
            dateString = df.format(date);
        }
        return dateString;

    }

    public static String formatDDMONYYYYHHMMSS(Date date) {
        String dateString = "";
        if (date != null) {
            SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
            dateString = df.format(date);
        }
        return dateString;
    }

    public static String formatDateYYYYMMDD(Date date) {
        String dateString = "";
        if (date != null) {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            dateString = df.format(date);
        }
        return dateString;

    }

    public static String formatDDMONYYYY(Date date) {
        String dateString = "";
        if (date != null) {
            SimpleDateFormat df = new SimpleDateFormat("dd-MMM-yyyy");
            dateString = df.format(date);
        }
        return dateString;

    }
    public static String formatHHMMSSDDMONYYYYNoSlash(Date date) {
        String dateString = "";
        if (date != null) {
            SimpleDateFormat df = new SimpleDateFormat("hhmmssddMMMyyyy");
            dateString = df.format(date);
        }
        return dateString;
    }
    public static String codeGender(String gender) {
        String sex = "";
        if (gender.equalsIgnoreCase("Male")) {
            sex = "M";
        } else if (gender.equalsIgnoreCase("Female")) {
            sex = "F";
        } else if (gender.equalsIgnoreCase("M")) {
            sex = "M";
        } else if (gender.equalsIgnoreCase("F")) {
            sex = "F";
        }
        return sex;
    }

    public static List<Demographics> createDemographicsList(List<String[]> data) {
        List<Demographics> demoTempList = new ArrayList<Demographics>();
        Demographics demo;
        for (String[] ele : data) {
            demo = convertTodemographics(ele);
            demoTempList.add(demo);
        }
        return demoTempList;
    }
    public static int convertEventToInt(XMLEvent xmlEvent){
        int val=0;
        Characters eventCharacters = null;
        if (xmlEvent instanceof Characters) {
            eventCharacters = (Characters) xmlEvent;
            val = Converter.convertToInt(eventCharacters.getData());
        }
        return val;
    }
    public static double convertEventToDouble(XMLEvent xmlEvent){
        double val=0;
        Characters eventCharacters = null;
        if (xmlEvent instanceof Characters) {
            eventCharacters = (Characters) xmlEvent;
            val = Converter.convertToDouble(eventCharacters.getData());
        }
        return val;
    }
    public static boolean convertEventToBoolean(XMLEvent xmlEvent){
        boolean ans=false;
        Characters eventCharacters = null;
        if (xmlEvent instanceof Characters) {
            eventCharacters = (Characters) xmlEvent;
            ans = Converter.convertToBoolean(eventCharacters.getData());
        }
        return ans;
    }
    public static String convertEventToString(XMLEvent xmlEvent){
        String val=null;
        Characters eventCharacters = null;
        if (xmlEvent instanceof Characters) {
            eventCharacters = (Characters) xmlEvent;
            val = eventCharacters.getData();
        }
        return val;
    }
    public static Date convertEventToDate(XMLEvent xmlEvent) {
        Date dateVal = null;
        Characters eventCharacters = null;
        if (xmlEvent instanceof Characters) {
            eventCharacters = (Characters) xmlEvent;
            dateVal = Converter.stringToDate(eventCharacters.getData());
        }
        return dateVal;
    }

    /*
    private String pepfarID;0
    private String hospID;1
    private String eHNID;2
    private String otherID;3
    private String firstName;4
    private String lastName;5
    private String middleName;6
    private Date adultEnrollmentDt;7
    private Date pmtctEnrollmentDt;8
    private Date dateOfBirth;9
    private String gender;10
    private String address1;11
    private String address2;12
    private String address_lga;13
    private String address_state;14
    private int creatorID;15
    private Date dateCreated;16
    private int locationID;17
    private Date enrollDate;18
    private Date artStartDate;19
    private String errorString;20
     */
    public static Demographics convertTodemographics(String[] ele) {
        Demographics demo = new Demographics();
        demo.setPatientID(convertToInt(ele[0]));
        demo.setPatientUUID(ele[1]);
        demo.setPepfarID(ele[2]);
        demo.setHospID(ele[3]);
        demo.seteHNID(ele[4]);
        demo.setOtherID(ele[5]);
        demo.setFirstName(ele[6]);
        demo.setLastName(ele[7]);
        demo.setMiddleName(ele[8]);
        demo.setAdultEnrollmentDt(stringToDate(ele[9]));
        demo.setPepEnrollmentDt(stringToDate(ele[10]));
        demo.setPmtctEnrollmentDt(stringToDate(ele[11]));
        demo.setHeiEnrollmentDt(stringToDate(ele[12]));
        demo.setPepEnrollmentDt(stringToDate(ele[13]));
        demo.setDateOfBirth(stringToDate(ele[14]));
        demo.setAge(Integer.parseInt(ele[15]));
        demo.setGender(ele[16]);
        demo.setAddress1(ele[17]);
        demo.setAddress2(ele[18]);
        demo.setAddress_lga(ele[19]);
        demo.setAddress_state(ele[20]);
        //demo.setCreatorID(Integer.parseInt(ele[21]));
        int creator = convertToInt(ele[15]);
        if (creator != 0) {
            demo.setCreatorID(creator);
            demo.setDateCreated(stringToDate(ele[16]));
        }
        demo.setDateChanged(stringToDate(ele[17]));
        int locationID = convertToInt(ele[18]);
        demo.setLocationID(locationID);
        demo.setCreatorName(ele[19]);
        demo.setLocationName(ele[20]);

        //demo.setArtStartDate(stringToDate(ele[18]));
        demo.addError(ele[21]);
        return demo;
    }


    /*public static Obs convertToObs(String[] ele) {
        Obs obs = new Obs();
        obs.setObsID(convertToInt(ele[0]));
        obs.setPatientID(convertToInt(ele[1]));
        obs.setEncounterID(convertToInt(ele[2]));
        obs.setPepfarID(ele[3]);
        obs.setHospID(ele[4]);
        obs.setVisitDate(stringToDate(ele[5]));
        obs.setFormName(ele[6]);
        obs.setConceptID(convertToInt(ele[7]));
        obs.setVariableName(ele[8]);
        obs.setVariableValue(ele[9]);
        obs.setEnteredBy(ele[10]);
        obs.setDateEntered(stringToDate(ele[11]));
        obs.setDateChanged(stringToDate(ele[12]));
        obs.setProvider(ele[13]);
        obs.setUuid(ele[14]);
        obs.setLocationName(ele[15]);
        obs.setLocationID(convertToInt(ele[16]));
        obs.setCreator(convertToInt(ele[17]));
        obs.setProviderID(convertToInt(ele[18]));
        obs.setValueNumeric(convertToDouble(ele[19]));
        obs.setValueDate(stringToDate(ele[20]));
        obs.setValueCoded(convertToInt(ele[21]));
        obs.setValueText(ele[22]);
        obs.setValueBoolean(Boolean.valueOf(ele[23]));
        obs.setObsGroupID(convertToInt(ele[24]));
        obs.setVoided(convertToInt(ele[25]));
        obs.setDateVoided(stringToDate(ele[26]));
        obs.setVoidedBy(convertToInt(ele[27]));
        obs.setChangedBy(convertToInt(ele[28]));
        obs.setFormID(convertToInt(ele[29]));
        return obs;
    }*/
    public Obs createObs(int patientID, String pepfarID, int locationID, Date visitDate, int conceptID, int providerID, int formID, int encounterID, int creator, Date dateCreated, double valueNumeric, Date valueDate, String valueText, int valueCoded) {
        Obs obs = new Obs();
        obs.setPatientID(patientID);
        obs.setPepfarID(pepfarID);
        obs.setLocationID(locationID);
        obs.setVisitDate(visitDate);
        obs.setConceptID(conceptID);
        obs.setProviderID(providerID);
        obs.setFormID(formID);
        obs.setEncounterID(encounterID);
        obs.setCreator(creator);
        obs.setDateEntered(dateCreated);
        obs.setValueCoded(valueCoded);
        obs.setValueDate(valueDate);
        obs.setValueNumeric(valueNumeric);
        obs.setValueText(valueText);
        return obs;
    }

    /*public static ArrayList<Obs> convertToObsList(List<String[]> dataList, DisplayScreen screen) {
        Obs obs;
        int count = 0;
        ArrayList<Obs> obsList = new ArrayList<Obs>();
        for (String[] ele : dataList) {
            //if(count!=0){
                 obs = convertToObs(ele);
                 obsList.add(obs);
            //}
            count++;
            //screen.updateStatus(count + " obs loaded");
        }
        return obsList;
    }*/

 /*public static ArrayList<Demographics> convertToDemographicList(List<String[]> dataList, DisplayScreen screen) {
        ArrayList<Demographics> demoList = new ArrayList<Demographics>();
        Demographics demo = null;
        int i = 0;

        for (String[] ele : dataList) {
            String str = "";
            for (int p = 0; p < ele.length; p++) {
                str += p + ": " + ele[p] + " ";
            }

            if (i != 0) {
                demo = new Demographics();
                demo.setPatientID(Converter.convertToInt(ele[0]));
                demo.setPatientUUID(ele[1]);
                demo.setPepfarID(ele[2]);
                demo.setHospID(ele[3]);
                demo.seteHNID(ele[4]);
                demo.setOtherID(ele[5]);
                demo.setFirstName(ele[6]);
                demo.setLastName(ele[7]);
                demo.setMiddleName(ele[8]);
                demo.setGender(ele[9]);
                demo.setDateOfBirth(stringToDate(ele[10]));
                demo.setAdultEnrollmentDt(stringToDate(ele[11]));
                demo.setAddress2(ele[12]);
                demo.setAddress1(ele[13]);
                demo.setAddress_lga(ele[14]);
                demo.setAddress_state(ele[15]);
                int creator = convertToInt(ele[16]);
                if (creator != 0) {
                    demo.setCreatorID(creator);
                    demo.setDateCreated(stringToDate(ele[17]));
                }
                int locationID = convertToInt(ele[18]);
                demo.setLocationID(locationID);
                demoList.add(demo);
            }
            screen.updateStatus("Converted " + i++);

        }
        return demoList;
    }*/
    public static int convertToInt(String val) {
        int id = 0;
        if (!val.isEmpty()) {
              try{
              id = Integer.parseInt(StringUtils.replacePattern(StringUtils.trim(val),"[^0-9]", ""));
              }catch(NumberFormatException nfe){
                  nfe.printStackTrace();
              }
        }
        return id;
    }

    public static double convertToDouble(String txt) {
        double val = 0.0;
        if (!txt.isEmpty() && isValidDouble(txt)) {
            try {
                val = Double.parseDouble(txt);
            } catch (NumberFormatException nfe) {
                nfe.printStackTrace();
                //System.out.println(nfe.getMessage());
            }
        }
        return val;
    }

    public static boolean isValidDouble(String numStr) {
        boolean ans = true;
        double num = 0;
        if (!numStr.isEmpty()) {
            try {
                num = Double.parseDouble(numStr);
            } catch (NumberFormatException nfe) {
                nfe.printStackTrace();
                ans = false;
            }
        }
        return ans;
    }

    public static boolean isValidInteger(String numStr) {
        boolean ans = true;
        int num = 0;
        if (!numStr.isEmpty()) {
            try {
                num = Integer.parseInt(numStr);
            } catch (NumberFormatException nfe) {
                nfe.printStackTrace();
                ans = false;
            }
        }
        return ans;
    }

    public static String generateUUID() {
        UUID uid;
        uid = UUID.randomUUID();
        String uidStr = String.valueOf(uid);
        return uidStr;
    }

    public static boolean convertToBoolean2(String ele) {
        boolean ans = true;
        int num = convertToInt(ele);
        if (num == 0) {
            ans = false;
        }
        return ans;
    }
     public static boolean convertToBoolean(String ele) {
        boolean ans = false;
        if(ele!=null){
            ans=Boolean.valueOf(ele);
        }
        return ans;
    }

    public static MapConfig convertToMapConfig(String[] data) {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setFormID(convertToInt(data[0]));
        mapConfig.setCsvFileName(data[1]);
        mapConfig.setMigrate(Converter.convertToBoolean(data[2]));
        return mapConfig;

    }

    public static Map<Integer, MapConfig> convertToMapConfigMap(List<String[]> dataArr) {
        MapConfig config = null;
        Map<Integer, MapConfig> configMap = new HashMap<Integer, MapConfig>();
        for (String[] ele : dataArr) {
            config = convertToMapConfig(ele);
            configMap.put(config.getFormID(), config);
        }
        return configMap;
    }

    /*
    private int omrsFormID;
    private boolean Allowed;
    private int omrsConceptID;
    private int isAnswerToWhichOMRSConcept;
    private int nmrsFormID;
    private int nmrsConceptID;
    private int isAnswerToWhichNMRSConcept;
     */
    public static ConceptMap convertToConceptMap(String[] data) {
        ConceptMap conceptMap = new ConceptMap();
        conceptMap.setOmrsFormID(convertToInt(StringUtils.trim(data[0])));
        conceptMap.setOmrsConceptID(convertToInt(StringUtils.trim(data[1])));
        conceptMap.setOmrsConceptName(StringUtils.trim(data[2]));
        conceptMap.setOmrsConceptDataType(StringUtils.trim(data[3]));
        conceptMap.setOmrsQuestionConcept(convertToInt(StringUtils.trim(data[4])));
        conceptMap.setNmrsFormID(convertToInt(StringUtils.trim(data[5])));
        conceptMap.setNmrsConceptID(convertToInt(StringUtils.trim(data[6])));
        conceptMap.setNmrsConceptName(StringUtils.trim(data[7]));
        conceptMap.setNmrsConceptDataType(StringUtils.trim(data[8]));
        conceptMap.setNmrsQuestionConcept(convertToInt(StringUtils.trim(data[9])));
        return conceptMap;
    }

    public static List<ConceptMap> convertToConceptMapList(List<String[]> dataArr) {
        List<ConceptMap> conceptMapList = new ArrayList<ConceptMap>();
        for (String[] ele : dataArr) {
            conceptMapList.add(convertToConceptMap(ele));
        }
        return conceptMapList;
    }
    public static void main(String[] arg){
        int i=0;
        while(i<1000000){
            i++;
            System.out.println(Converter.generateUUID());
        }
    }

}
