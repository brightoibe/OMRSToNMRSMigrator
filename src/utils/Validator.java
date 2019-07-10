/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import model.Demographics;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author The Bright
 * This class will validate all objects before they
 * are inserted into the database, objects that have error are written to a log file
 */
public class Validator {
    
    public static boolean validateDemographics(Demographics demo){
        boolean ans=true;
        if(demo.getDateOfBirth()==null){
            ans=false;
            demo.addError("Date of birth missing");
        }
        if(StringUtils.isEmpty(demo.getGender())){
            ans=false;
            demo.addError("Gender is missing");
        }
        if(StringUtils.isEmpty(demo.getPepfarID())){
            ans=false;
            demo.addError("PepfarID is missing");
        }
        if(StringUtils.isEmpty(demo.getFirstName())){
            ans=false;
            demo.addError("First name is missing");
        }
        if(StringUtils.isEmpty(demo.getGender())){
            ans=false;
            demo.addError("Gender is missing");
        }
        if(demo.getPatientID()==1 || demo.getPatientID()==10){
            ans=false;
            demo.addError("Admin user was skipped");
        }
        return ans;
    }
    
}
