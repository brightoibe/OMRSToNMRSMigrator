/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
 
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import org.apache.commons.lang3.StringUtils;
 
/**
 *
 * @author The Bright
 */
public class Test {
    

    public static void main(String[] args) throws ParseException
    {
       int[] allowedForms = {1, 56, 65, 67, 18};
       if(Arrays.binarySearch(allowedForms, 18)>0){
           System.out.println("Found it");
       }else{
            System.out.println("Wahala");
       }

    }
}
 

