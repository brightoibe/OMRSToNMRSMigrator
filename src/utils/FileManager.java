/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import java.util.ArrayList;
import java.io.FileWriter;
import java.io.IOException;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import model.Demographics;
import model.DisplayScreen;
import model.Obs;
import model.User;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author openmrsdev
 */
public class FileManager {

    private CSVReader csvReader;
    private CSVWriter csvWriter;
    private DisplayScreen screen;

    public void InitializeReaders(String fileName, String logFileName, DisplayScreen screen) {
        this.screen = screen;
        File sourceFile = new File(fileName);
        File logFile = new File(logFileName);
        try {
            csvReader = new CSVReader(new BufferedReader(new FileReader(sourceFile)));
            csvWriter = new CSVWriter(new FileWriter(logFile));
            screen.updateStatus("Initialized readers and writers");
        } catch (IOException ex) {
            handleException(ex, screen);
        }
    }
   public void initializeReader(String fileName,DisplayScreen screen){
       this.screen=screen;
       File file=new File(fileName);
        try {
            csvReader = new CSVReader(new BufferedReader(new FileReader(file)));
            screen.updateStatus("Initialized readers...");
        } catch (IOException ex) {
            handleException(ex, screen);
        }
   }
   
    public String[] readRecord() {
        String[] nextLine = null;
        try {
            nextLine = csvReader.readNext();
        } catch (IOException ex) {
            handleException(ex, screen);
        }
        return nextLine;
    }
    public void closeReader(){
        try{
            if(csvReader!=null){
                csvReader.close();
            }
        }catch(IOException ex){
            handleException(ex, screen);
        }
    }

    public void dumpToFile(String fileName, List<String[]> data) {
        File sourceFile = new File(fileName);
        try {
            csvWriter = new CSVWriter(new FileWriter(sourceFile));
            csvWriter.writeAll(data);
            if (csvWriter != null) {
                csvWriter.close();
            }
        } catch (IOException ex) {
            handleException(ex, screen);
        }
    }
    
    public List<String[]> readCSV(String fileName, DisplayScreen screen) {
        screen.updateStatus("Loading csv file...");
        List<String[]> dataList = new ArrayList<String[]>();
        String[] nextLine;
        int count = 0;
        try {

            //dataList = csvReader.readAll();
            while ((nextLine = csvReader.readNext()) != null) {
                dataList.add(nextLine);
                screen.updateStatus(count + " Records loaded in memory");
                count++;
            }
            System.out.println(dataList.size() + " records loaded...");
            screen.updateStatus(dataList.size() + " records loaded...");
        } catch (FileNotFoundException fne) {
            handleException(fne, screen);
        } catch (IOException ioe) {
            handleException(ioe, screen);
        }
        return dataList;
    }
    public List<String[]> loadAllDataAsStream(String fileName) {
        InputStream is=FileManager.class.getResourceAsStream(fileName);
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        List<String[]> data=null;
        try {
            csvReader = new CSVReader(br);
            data = csvReader.readAll();
        } catch (FileNotFoundException fen) {
            fen.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        closeAll();
        return data;
    }
    public List<String[]> loadAllData(String fileName) {
        File sourceFile = new File(fileName);
        List<String[]> data = null;
        try {
            csvReader = new CSVReader(new BufferedReader(new FileReader(sourceFile)));
            data = csvReader.readAll();
        } catch (FileNotFoundException fen) {
            fen.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        data.remove(0);
        closeAll();
        return data;
    }
    public List<String[]> loadAllDataInFolder(String folderName){
        List<String[]> allData=new ArrayList<String[]>();
        List<String[]> data=null;
        File directory = new File(folderName);
        File[] fList = directory.listFiles();
        String fileName=null;
        for (File file : fList){
            if (file.isFile() ){//&& StringUtils.contains("csv", file.getName())){
                fileName=file.getAbsolutePath();
                System.out.println("File loaded: "+fileName);
               // screen.updateStatus("Loading "+file.getName()+"...");
                //System.out.println("Loading "+file.getName()+"...");
                data=loadAllData(fileName);
                allData.addAll(data);
            } 
        }
        closeAll();
        return allData;
    }
    public List<String[]> loadAllDataInFolder(String folderName,String csvFileName){
        List<String[]> allData=new ArrayList<String[]>();
        List<String[]> data=null;
        File directory = new File(folderName);
        File[] fList = directory.listFiles();
        String fileName=null;
        for (File file : fList){
            if (file.isFile() && file.getName().equalsIgnoreCase(csvFileName)){//&& StringUtils.contains("csv", file.getName())){
                fileName=file.getAbsolutePath();
                System.out.println("File loaded: "+fileName);
               // screen.updateStatus("Loading "+file.getName()+"...");
                //System.out.println("Loading "+file.getName()+"...");
                data=loadAllData(fileName);
                allData.addAll(data);
            } 
        }
        closeAll();
        return allData;
    }


    private void handleException(Exception e, DisplayScreen screen) {
        screen.updateStatus(e.getMessage());
        e.printStackTrace();
    }

    public void writeHeaders(String[] headers) {
        csvWriter.writeNext(headers);
    }

    public void write(List<String[]> dataList, String fileName, DisplayScreen screen) {
        //screen.updateStatus("Writing " + dataList.size() + " records to file...");
        csvWriter.writeAll(dataList);
        //screen.updateStatus("Writing completed");

    }

    public void closeAll() {
        try {
            if (csvWriter != null) {
                csvWriter.close();
            }
            if (csvReader != null) {
                csvReader.close();
            }
        } catch (IOException ex) {
            handleException(ex, screen);
        }
    }
    public void initializeWriter(String fileName){
        File logFile = new File(fileName);
        try {
            //csvReader = new CSVReader(new BufferedReader(new FileReader(sourceFile)));
            csvWriter = new CSVWriter(new FileWriter(logFile));
            screen.updateStatus("Initialized writers");
        } catch (IOException ex) {
            handleException(ex, screen);
        }
    }
    public void setScreen(DisplayScreen screen){
        this.screen=screen;
    }
    public void writeLog(String[] data) {
        csvWriter.writeNext(data);
    }
    public void writeCSV(Obs obs){
         writeLog(toArray(obs));
    }
    public void writeCSV(Demographics demo){
        writeLog(toArray(demo));
    }
    public void writeCSV(User usr){
        writeLog(toArray(usr));
    }
    public String[] toArray(Obs obs) {
        String[] obsArr = new String[30];
        obsArr[0] = String.valueOf(obs.getObsID());
        obsArr[1]=String.valueOf(obs.getPatientID());
        obsArr[2]=String.valueOf(obs.getEncounterID());
        obsArr[3] = obs.getPepfarID();
        obsArr[4] = obs.getHospID();
        obsArr[5] = Converter.formatDateYYYYMMDD(obs.getVisitDate());
        obsArr[6] = obs.getFormName();
        obsArr[7]=String.valueOf(obs.getConceptID());
        obsArr[8] = obs.getVariableName();
        obsArr[9] = obs.getVariableValue();
        obsArr[10] = obs.getEnteredBy();
        obsArr[11] = Converter.formatDateYYYYMMDD(obs.getDateEntered());
        obsArr[12] = Converter.formatDateYYYYMMDD(obs.getDateChanged());
        obsArr[13] = obs.getProvider();
        obsArr[14] = obs.getUuid();
        obsArr[15] = obs.getLocationName();
        obsArr[16] = String.valueOf(obs.getLocationID());
        obsArr[17] = String.valueOf(obs.getCreator());
        obsArr[18] = String.valueOf(obs.getProviderID());
        obsArr[19] = String.valueOf(obs.getValueNumeric());
        obsArr[20] = Converter.formatDateYYYYMMDD(obs.getValueDate());
        obsArr[21] = String.valueOf(obs.getValueCoded());
        obsArr[22] = String.valueOf(obs.getValueText());
        obsArr[23]=String.valueOf(obs.isValueBoolean());
        obsArr[24]=String.valueOf(obs.getObsGroupID());
        obsArr[25]=String.valueOf(obs.getVoided());
        obsArr[26]=Converter.formatDateYYYYMMDD(obs.getDateVoided());
        obsArr[27]=String.valueOf(obs.getVoidedBy());
        obsArr[28]=String.valueOf(obs.getChangedBy());
        obsArr[29]=String.valueOf(obs.getFormID());
        return obsArr;
    }
    public String[] toArray(User usr){
        String[] usrArr=new String[12];
        usrArr[0]=String.valueOf(usr.getUser_id());
        usrArr[1]=String.valueOf(usr.getUserName());
        usrArr[2]=String.valueOf(usr.getFullName());
        usrArr[3]=String.valueOf(usr.getFirstName());
        usrArr[4]=String.valueOf(usr.getLastName());
        usrArr[5]=String.valueOf(usr.getGender());
        usrArr[6]=String.valueOf(Converter.formatDateYYYYMMDD(usr.getDateCreated()));
        usrArr[7]=String.valueOf(usr.getCreator());
        usrArr[8]=String.valueOf(usr.getUuid());
        usrArr[9]=String.valueOf(Converter.formatDateYYYYMMDD(usr.getDateChanged()));
        usrArr[10]=String.valueOf(usr.getRetired());
        usrArr[11]=String.valueOf(usr.getRetiredBy());
        return usrArr;
    }
    public String[] toArray(Demographics demo) {
        String[] demoArr = new String[27];
        demoArr[0] = String.valueOf(demo.getPatientID());
        demoArr[1] = String.valueOf(demo.getPatientUUID());
        demoArr[2] = String.valueOf(demo.getPepfarID());
        demoArr[3] = String.valueOf(demo.getHospID());
        demoArr[4] = String.valueOf(demo.geteHNID());
        demoArr[5] = String.valueOf(demo.getOtherID());
        demoArr[6] = String.valueOf(demo.getFirstName());
        demoArr[7] = String.valueOf(demo.getLastName());
        demoArr[8] = String.valueOf(demo.getMiddleName());
        demoArr[9] = Converter.formatDateYYYYMMDD(demo.getAdultEnrollmentDt());
        demoArr[10] = Converter.formatDateYYYYMMDD(demo.getPeadEnrollmentDt());
        demoArr[11] = Converter.formatDateYYYYMMDD(demo.getPmtctEnrollmentDt());
        demoArr[12] = Converter.formatDateYYYYMMDD(demo.getHeiEnrollmentDt());
        demoArr[13] = Converter.formatDateYYYYMMDD(demo.getPepEnrollmentDt());
        demoArr[14] = Converter.formatDateYYYYMMDD(demo.getDateOfBirth());
        demoArr[15] = String.valueOf(demo.getAge());
        demoArr[16] = demo.getGender();
        demoArr[17] = demo.getAddress1();
        demoArr[18] = demo.getAddress2();
        demoArr[19] = demo.getAddress_lga();
        demoArr[20] = String.valueOf(demo.getCreatorID());
        demoArr[21] = Converter.formatDateYYYYMMDD(demo.getDateCreated());
        demoArr[22] = String.valueOf(demo.getVoided());
        demoArr[23] = Converter.formatDateYYYYMMDD(demo.getDateChanged());
        demoArr[24] = String.valueOf(demo.getLocationID());
        demoArr[25] = String.valueOf(demo.getCreatorName());
        demoArr[26] = demo.getLocationName();
        return demoArr;
    }

    public static void main(String[] arg){
        FileManager mgr=new FileManager();
        //List<String[]> dataArr=mgr.loadAllData("config/nmrsconcepts.csv");
        List<String[]> dataArr=mgr.loadAllDataInFolder("map");
        for(String[] ele: dataArr){
            System.out.println(ele);
        }
    }

}
