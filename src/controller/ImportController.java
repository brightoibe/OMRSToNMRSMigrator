/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controller;

import daos.ImportDAO;
import java.util.List;
import javax.swing.DefaultComboBoxModel;
import model.ConnectionParameters;
import model.DisplayScreen;
import model.Location;

/**
 *
 * @author The Bright
 */
public class ImportController {
    private ImportDAO dao;
    private DisplayScreen screen;
    public ImportController(){
        dao=new ImportDAO();
    }
    public DefaultComboBoxModel getTemplateTypeModel() {
        DefaultComboBoxModel comboBoxModel = new DefaultComboBoxModel();
        String[] import_types = {
            "Users",
            "Demographics",
            "Obs",
            
        };
        for (String ele : import_types) {
            comboBoxModel.addElement(ele);
        }
        return comboBoxModel;
    }
    public DefaultComboBoxModel getlocationComboModel() {
        DefaultComboBoxModel comboBoxModel = new DefaultComboBoxModel();
        List<Location> locList = dao.getLocations();
        for(Location ele : locList) {
            comboBoxModel.addElement(ele);
        }
        return comboBoxModel;
    }
    public boolean connect(ConnectionParameters con){
       return dao.connect(con);
    }
    public void closeConnection(){
        dao.closeConnections();
    }
    public void migrate(String xmlFilePath,String exportType, int locationID){
        if(exportType.equalsIgnoreCase("Obs")){
            //dao.migrateMigrateForms(xmlFilePath, locationID);//Obs(xmlFilePath, locationID);
            
            dao.migrateObs(xmlFilePath, locationID);
        }else if(exportType.equalsIgnoreCase("Demographics")){
            dao.migrateDemographics(xmlFilePath, locationID);
        }else if(exportType.equalsIgnoreCase("Users")){
            dao.migrateUsers(xmlFilePath);
        }
    }
    public void registerDisplay(DisplayScreen screen){
        this.screen=screen;
        dao.registerDisplay(screen);
    }
    
    public void validate(String xmlPath,String importType){
        
    }
}
