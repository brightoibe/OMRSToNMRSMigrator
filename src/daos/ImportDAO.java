/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package daos;

import dictionaries.MasterDictionary;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import model.ConnectionParameters;
import model.DisplayScreen;
import model.Location;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import model.ConceptMap;
import model.Demographics;
import model.DrugObs;
import model.Encounter;
import model.EncounterProvider;
import model.Obs;
import model.Provider;
import model.Record;
import model.User;
import model.Visit;
import org.apache.commons.lang3.StringUtils;
import utils.Converter;
import utils.FileManager;
import utils.Validator;

/**
 *
 * @author The Bright
 */
public class ImportDAO {

    private static final String OPENMRS_USER = "admin";
    private static final String OPENMRS_PASS = "Admin123";
    private DisplayScreen screen;
    private Connection connection;
    private final static String USERENCRYPTPASSWORD = "9caac041e4f0e1834c28e5b51c985881438450d7c6c0d302dc8a92247fa57eb1820fa817479df48f393e41ed5da0e13814b2d4a2667ba775ee03f4829701e858";
    private final static String USERENCRYPTSALT = "77eb9c12ad5335292bf6fae7f421a5983bc7ffcb02764ae2de6c758abdc536e4ac4a1b57e3609448eca9a39445742e906466b8f8d8950d1df8d09e4a151409dc";
    private final static int BIRTHDATEESTIMATED = 0;
    private final static int DEAD = 0;
    private final static int ADMIN_USER_ID = 1;
    private final static int USER_RETIRED = 0;
    private final static int PREFERRED = 1;
    private final static int HIV_CARE_PROGRAM = 1;
    private final static int PMTCT_PROGRAM = 2;
    private final static int HIV_EXPOSED_INFANT = 3;
    private final static int HIV_TESTING_SERVICES = 4;
    private final static int PEPFAR_IDENTIFIER = 4;
    private final static int OPENMRSID_IDENTIFIER = 1;
    private final static int OLD_IDENTIFIER=2;
    private final static int HOSPITAL_IDENTIFIER=5;
    private final static int SERVICE_PROVIDER_ENCOUNTER_ROLE = 3;
    private final static int DATAENTRY_PROVIDER_ENCOUNTER_ROLE = 1;
    private final static int VISIT_TYPE_ID = 1;
    private final static int VOIDED = 0;
    private final static String ADDRESS_COUNTRY = "NIGERIA";
    private final static int OMRS_DRUG_NAME_CONCEPT_ID=7778364;
    
    private Set<Integer> providerIdSet = new HashSet<Integer>();
    private FileManager mgr;
    private MasterDictionary dictionary;
    private Map<Integer, User> usersMap = new HashMap<Integer, User>();
    Map<Integer, Provider> providerMap = new HashMap<Integer, Provider>();
    private Map<Integer, Date> dateOfBirthMap = new HashMap<Integer, Date>();
    private int visitID;
    private Map<Integer,Record> trackMap=new HashMap<Integer,Record>();

    public ImportDAO() {
        dictionary = new MasterDictionary();
    }

    public void registerDisplay(DisplayScreen screen) {
        this.screen = screen;
        dictionary.setDisplayScreen(screen);
    }

    public boolean loadDriver() {
        boolean ans;
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            ans = true;
            screen.updateStatus("Mysql jdbc driver loaded......");
        } catch (ClassNotFoundException e) {
            ans = false;
            screen.updateStatus(e.getMessage());
        } catch (InstantiationException ie) {
            System.err.println(ie.getMessage());
            ans = false;
            screen.updateStatus(ie.getMessage());
        } catch (IllegalAccessException iae) {
            System.err.println(iae.getMessage());
            screen.updateStatus(iae.getMessage());
            ans = false;
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            screen.updateStatus(ex.getMessage());
            ans = false;
        }
        return ans;
    }

    public PreparedStatement prepareQuery(String query) {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(query);
        } catch (SQLException e) {
            displayErrors(e);
            return ps;
        }
        return ps;
    }

    public void disableForeignKeyConstraint() {
        String query = "SET FOREIGN_KEY_CHECKS=0";
        PreparedStatement ps = prepareQuery(query);
        try {
            ps.executeUpdate();
            ps.close();
        } catch (SQLException ex) {
            processException(ex, query);
        }
    }

    private void processException(SQLException ex, String query) {
        screen.showError(ex.getMessage() + " " + query);
        displayErrors(ex);
    }

    public void loadProviderMap() {
        String sql_text = "select * from provider";
        Map<Integer, Provider> providerMap = new HashMap<Integer, Provider>();
        Provider provider = null;
        PreparedStatement ps = prepareQuery(sql_text);
        int count = 0;
        try {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                provider = new Provider();
                provider.setProviderID(rs.getInt("provider_id"));
                provider.setPersonID(rs.getInt("person_id"));
                provider.setProviderName(rs.getString("name"));
                provider.setIdentifier(rs.getString("identifier"));
                provider.setCreator(rs.getInt("creator"));
                provider.setDateCreated(rs.getDate("date_created"));
                provider.setRetired(rs.getInt("retired"));
                provider.setUuid(rs.getString("uuid"));
                provider.setProviderRoleID(rs.getInt("provider_role_id"));
                providerMap.put(provider.getPersonID(), provider);
                count++;
                screen.updateStatus("Loading provider..." + count);
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        }
        this.providerMap = providerMap;
    }

    public void loadUserMap() {
        String sql_text = "select * from users";
        Map<Integer, User> usersMap = new HashMap<Integer, User>();
        User user = null;
        PreparedStatement ps = prepareQuery(sql_text);
        int count = 0;
        try {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                user = new User();
                user.setUser_id(rs.getInt("user_id"));
                user.setUserName(rs.getString("username"));
                user.setCreator(rs.getInt("creator"));
                user.setDateCreated(rs.getDate("date_created"));
                user.setPerson_id(rs.getInt("person_id"));
                usersMap.put(user.getUser_id(), user);
                count++;
                screen.updateStatus("Loading user..." + count);
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        }
        this.usersMap = usersMap;
    }

    public boolean connect(ConnectionParameters con) {
        boolean ans;
        Properties p = new Properties();
        p.setProperty("user", con.getUsername());
        p.setProperty("password", con.getPassword());
        p.setProperty("MaxPooledStatements", "200");
        p.setProperty("rewriteBatchedStatements", "true");
        try {
            //String conString = "jdbc:mysql://" + con.getHostIP()+ ":" + con.getPortNo()+ "/" + con.getDatabase() + "?user=" + con.getUsername() + "&password=" + con.getPassword();
            String conString = "jdbc:mysql://" + con.getHostIP() + ":" + con.getPortNo() + "/" + con.getDatabase();
            connection = DriverManager.getConnection(conString, p);
            ans = true;
            connection.setAutoCommit(false);
            disableForeignKeyConstraint();
        } catch (SQLException ex) {
            displayErrors(ex);
            ans = false;
        }
        return ans;
    }

    public void displayErrors(SQLException ex) {
        System.out.println("SQLException: " + ex.getMessage());
        System.out.println("SQLState: " + ex.getSQLState());
        System.out.println("VendorError: " + ex.getErrorCode());
        ex.printStackTrace();
    }

    public void closeConnections() {
        try {
            if (connection != null) {
                connection.commit();
                connection.close();
            }
        } catch (SQLException ex) {
            screen.showError(ex.getMessage());
        }
    }

    public void cleanup(ResultSet rs, Statement stmt) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException ex) {
            processException(ex, "");
        }
    }

    public Demographics setDemographicPropertyFromXMLEvent(XMLEventReader eventReader, Demographics demo, StartElement startElement) throws XMLStreamException {
        switch (startElement.getName().getLocalPart()) {
            case "person_source_pk":
                Characters personIDDataEvent = (Characters) eventReader.nextEvent();
                demo.setPatientID(Converter.convertToInt(personIDDataEvent.getData()));
                break;
            case "person_uuid":
                Characters personUUIDDataEvent = (Characters) eventReader.nextEvent();
                demo.setPatientUUID(personUUIDDataEvent.getData());
                break;
            case "pepfar_id":
                Characters pepfarIDDataEvent = (Characters) eventReader.nextEvent();
                demo.setPepfarID(pepfarIDDataEvent.getData());
                break;
            case "hosp_id":
                Characters hospIDDataEvent = (Characters) eventReader.nextEvent();
                demo.setHospID(hospIDDataEvent.getData());
                break;
            case "ehnid":
                Characters ehnidIDDataEvent = (Characters) eventReader.nextEvent();
                demo.seteHNID(ehnidIDDataEvent.getData());
                break;
            case "other_id":
                Characters otherIDDataEvent = (Characters) eventReader.nextEvent();
                demo.setOtherID(otherIDDataEvent.getData());
                break;
            case "first_name":
                Characters firstNameDataEvent = (Characters) eventReader.nextEvent();
                demo.setFirstName(firstNameDataEvent.getData());
                break;
            case "last_name":
                Characters lastNameDataEvent = (Characters) eventReader.nextEvent();
                demo.setLastName(lastNameDataEvent.getData());
                break;
            case "middle_name":
                if (eventReader.nextEvent() instanceof Characters) {
                    Characters middleNameDataEvent = (Characters) eventReader.nextEvent();
                    demo.setMiddleName(middleNameDataEvent.getData());
                }
                break;
            case "adult_enrollment_dt":
                Characters adultEnrollmentDateDataEvent = (Characters) eventReader.nextEvent();
                demo.setAdultEnrollmentDt(Converter.stringToDate(adultEnrollmentDateDataEvent.getData()));
                break;
            case "pead_enrollment_dt":
                Characters pedEnrollmentDateDataEvent = (Characters) eventReader.nextEvent();
                demo.setPeadEnrollmentDt(Converter.stringToDate(pedEnrollmentDateDataEvent.getData()));
                break;
            case "pmtct_enrollment_dt":
                Characters pmtctEnrollmentDateDataEvent = (Characters) eventReader.nextEvent();
                demo.setPmtctEnrollmentDt(Converter.stringToDate(pmtctEnrollmentDateDataEvent.getData()));
                break;
            case "hei_enrollment_dt":
                Characters heiEnrollmentDateDataEvent = (Characters) eventReader.nextEvent();
                demo.setHeiEnrollmentDt(Converter.stringToDate(heiEnrollmentDateDataEvent.getData()));
                break;
            case "pep_enrollment_dt":
                Characters pepEnrollmentDateDataEvent = (Characters) eventReader.nextEvent();
                demo.setPepEnrollmentDt(Converter.stringToDate(pepEnrollmentDateDataEvent.getData()));
                break;
            case "dob":
                Characters dobDataEvent = (Characters) eventReader.nextEvent();
                demo.setDateOfBirth(Converter.stringToDate(dobDataEvent.getData()));
                break;
            case "age":
                Characters ageDataEvent = (Characters) eventReader.nextEvent();
                demo.setAge(Converter.convertToInt(ageDataEvent.getData()));
                break;
            case "gender":
                Characters genderDataEvent = (Characters) eventReader.nextEvent();
                demo.setGender(genderDataEvent.getData());
                break;
            case "address1":
                Characters address1DataEvent = (Characters) eventReader.nextEvent();
                demo.setAddress1(address1DataEvent.getData());
                break;
            case "address2":
                Characters address2DataEvent = (Characters) eventReader.nextEvent();
                demo.setAddress2(address2DataEvent.getData());
                break;
            case "address_lga":
                Characters addressLGADataEvent = (Characters) eventReader.nextEvent();
                demo.setAddress_lga(addressLGADataEvent.getData());
                break;
            case "address_state":
                Characters addressStateDataEvent = (Characters) eventReader.nextEvent();
                demo.setAddress_state(addressStateDataEvent.getData());
                break;
            case "creator_id":
                Characters creatorIDDataEvent = (Characters) eventReader.nextEvent();
                demo.setCreatorID(Converter.convertToInt(creatorIDDataEvent.getData()));
                break;
            case "date_created":
                Characters dateCreatedDataEvent = (Characters) eventReader.nextEvent();
                demo.setDateCreated(Converter.stringToDate(dateCreatedDataEvent.getData()));
                break;
            case "location_id":
                Characters locationIDDataEvent = (Characters) eventReader.nextEvent();
                demo.setLocationID(Converter.convertToInt(locationIDDataEvent.getData()));
                break;
            case "creator":
                Characters creatorDataEvent = (Characters) eventReader.nextEvent();
                demo.setCreatorName(creatorDataEvent.getData());
                break;
            case "location":
                Characters locationNameDataEvent = (Characters) eventReader.nextEvent();
                demo.setLocationName(locationNameDataEvent.getData());
                break;
            case "date_changed":
                Characters dateChangedDataEvent = (Characters) eventReader.nextEvent();
                demo.setDateChanged(Converter.stringToDate(dateChangedDataEvent.getData()));
                break;
        }
        return demo;
    }

    public int countRecords(String xmlFile, String tag) throws FileNotFoundException, XMLStreamException {
        File file = new File(xmlFile);
        screen.updateStatus("Counting " + tag + " records in " + file.getName());
        int recordCount = 0;
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLEventReader eventReader = factory.createXMLEventReader(new FileReader(file));
        XMLEvent xmlEvent = null;
        while (eventReader.hasNext()) {
            xmlEvent = eventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if (tag.equalsIgnoreCase(startElement.getName().getLocalPart())) {
                    recordCount++;
                    screen.updateStatus("Counting " + tag + " records " + recordCount);
                }
            }
        }
        return recordCount;
    }

    public void migrateDemographics(List<Demographics> demoList, int locationID) {
        savePersons(demoList);
        savePatients(demoList);
        savePatientIdentifier(demoList, locationID);
        savePatientProgram(demoList, locationID);
        savePersonNames(demoList);
        savePersonAddress(demoList);
    }

    public void savePatientIdentifier(List<Demographics> demoList, int locationID) {
        String sql_text = "insert into patient_identifier (patient_id,identifier,identifier_type,preferred,location_id,creator,date_created,voided,uuid) values (?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Demographics demo : demoList) {
                if (StringUtils.isNotEmpty(demo.getPepfarID())) {
                    ps.setInt(1, demo.getPatientID());
                    ps.setString(2, demo.getPepfarID());
                    ps.setInt(3, PEPFAR_IDENTIFIER);
                    ps.setInt(4, PREFERRED);
                    ps.setInt(5, locationID);
                    ps.setInt(6, ADMIN_USER_ID);
                    ps.setDate(7, Converter.convertToSQLDate(demo.getDateCreated()));
                    ps.setInt(8, VOIDED);
                    ps.setString(9, Converter.generateUUID());
                    ps.addBatch();
                }
                if (StringUtils.isNotEmpty(demo.geteHNID())) {
                    ps.setInt(1, demo.getPatientID());
                    ps.setString(2, demo.geteHNID());
                    ps.setInt(3, OPENMRSID_IDENTIFIER);
                    ps.setInt(4, 0);
                    ps.setInt(5, locationID);
                    ps.setInt(6, demo.getCreatorID());
                    ps.setDate(7, Converter.convertToSQLDate(demo.getDateCreated()));
                    ps.setInt(8, VOIDED);
                    ps.setString(9, Converter.generateUUID());
                    ps.addBatch();
                }
                if (StringUtils.isNotEmpty(demo.getHospID())) {
                    ps.setInt(1, demo.getPatientID());
                    ps.setString(2, demo.getHospID());
                    ps.setInt(3, HOSPITAL_IDENTIFIER);
                    ps.setInt(4, 0);
                    ps.setInt(5, locationID);
                    ps.setInt(6, demo.getCreatorID());
                    ps.setDate(7, Converter.convertToSQLDate(demo.getDateCreated()));
                    ps.setInt(8, VOIDED);
                    ps.setString(9, Converter.generateUUID());
                    ps.addBatch();
                }
                if (StringUtils.isNotEmpty(demo.getOtherID())) {
                    ps.setInt(1, demo.getPatientID());
                    ps.setString(2, demo.getOtherID());
                    ps.setInt(3, OLD_IDENTIFIER);
                    ps.setInt(4, 0);
                    ps.setInt(5, locationID);
                    ps.setInt(6, demo.getCreatorID());
                    ps.setDate(7, Converter.convertToSQLDate(demo.getDateCreated()));
                    ps.setInt(8, VOIDED);
                    ps.setString(9, Converter.generateUUID());
                    ps.addBatch();
                }
                
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }

    }

    public void migrateDemographics(String xmlFile, int locationID) {
        File file = new File(xmlFile);
        int count = 0;
        int batch_count = 1;
        List<Demographics> demoList = new ArrayList<Demographics>();
        Demographics demo = null;
        dictionary.initializeDemographicErrorLog();
        try {
            int size = countRecords(xmlFile, "patient");
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLEventReader eventReader = factory.createXMLEventReader(new FileReader(file));
            //StartElement startElement=null;
            screen.updateMinMaxProgress(0, size);
            XMLEvent xmlEvent2 = null;
            XMLEvent xmlEvent = null;
            Characters eventCharacters = null;
            while (eventReader.hasNext()) {
                xmlEvent = eventReader.nextEvent();
                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    if ("patient".equalsIgnoreCase(startElement.getName().getLocalPart())) {
                        demo = new Demographics();
                    }
                    switch (startElement.getName().getLocalPart()) {
                        case "person_source_pk":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setPatientID(Converter.convertToInt(eventCharacters.getData()));
                            }

                            break;
                        case "person_uuid":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setPatientUUID(eventCharacters.getData());
                            }

                            break;
                        case "pepfar_id":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setPepfarID(eventCharacters.getData());
                            }
                            break;
                        case "hosp_id":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setHospID(eventCharacters.getData());
                            }

                            break;
                        case "ehnid":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.seteHNID(eventCharacters.getData());
                            }
                            break;
                        case "other_id":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setOtherID(eventCharacters.getData());
                            }
                            break;
                        case "first_name":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setFirstName(eventCharacters.getData());
                            }
                            break;
                        case "last_name":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setLastName(eventCharacters.getData());
                            }

                            break;
                        case "middle_name":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setMiddleName(eventCharacters.getData());
                            }

                            break;
                        case "adult_enrollment_dt":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setAdultEnrollmentDt(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;
                        case "pead_enrollment_dt":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setPeadEnrollmentDt(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;
                        case "pmtct_enrollment_dt":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setPmtctEnrollmentDt(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;
                        case "hei_enrollment_dt":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setHeiEnrollmentDt(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;
                        case "pep_enrollment_dt":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setPepEnrollmentDt(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;
                        case "dob":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setDateOfBirth(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;
                        case "age":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setAge(Converter.convertToInt(eventCharacters.getData()));
                            }

                            break;
                        case "gender":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setGender(eventCharacters.getData());
                            }

                            break;
                        case "address1":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setAddress1(eventCharacters.getData());
                            }

                            break;
                        case "address2":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setAddress2(eventCharacters.getData());
                            }

                            break;
                        case "address_lga":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setAddress_lga(eventCharacters.getData());
                            }

                            break;
                        case "address_state":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setAddress_state(eventCharacters.getData());
                            }
                            break;
                        case "creator_id":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setCreatorID(Converter.convertToInt(eventCharacters.getData()));
                            }
                            break;
                        case "date_created":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setDateCreated(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;
                        case "location_id":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setLocationID(Converter.convertToInt(eventCharacters.getData()));
                            }

                            break;
                        case "creator":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setCreatorName(eventCharacters.getData());
                            }
                            break;
                        case "location":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setLocationName(eventCharacters.getData());
                            }

                            break;
                        case "date_changed":
                            xmlEvent2 = eventReader.nextEvent();
                            if (xmlEvent2 instanceof Characters) {
                                eventCharacters = (Characters) xmlEvent2;
                                demo.setDateChanged(Converter.stringToDate(eventCharacters.getData()));
                            }
                            break;

                    }
                }

                if (xmlEvent.isEndElement()) {
                    EndElement endElement = xmlEvent.asEndElement();
                    //If employee tag is closed then add the employee object to list;
                    //and be ready to read next employee data
                    if ("patient".equalsIgnoreCase(endElement.getName().getLocalPart())) {
                        //if (Validator.validateDemographics(demo)) {
                        if (demo != null && demo.getPatientID() != 1 && !StringUtils.isEmpty(demo.getPepfarID())) {
                            demoList.add(demo);
                            count++;
                            screen.updateProgress(count);
                            screen.updateStatus(count + " of " + size + " patient created in batch: " + batch_count);
                        } else {
                            dictionary.log(demo);
                        }
                        if ((count % 200) == 0) {
                            migrateDemographics(demoList, locationID);
                            demoList.clear();
                            batch_count++;
                        }

                    }

                }
            }
            if (!demoList.isEmpty()) {
                migrateDemographics(demoList, locationID);
            }
            commitConnection();
            dictionary.closeAllResources();
        } catch (XMLStreamException xstr) {
            handleException(xstr);
        } catch (FileNotFoundException fne) {
            handleException(fne);
        } catch (SQLException ex) {
            handleException(ex);
        }

    }

    public void savePatients(List<Demographics> demoList) {
        String sql_text = "insert into patient (patient_id,creator,date_created,voided) values (?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Demographics demo : demoList) {
                if (demo.getDateOfBirth() != null) {
                    ps.setInt(1, demo.getPatientID());
                    ps.setInt(2, ADMIN_USER_ID);
                    ps.setDate(3, Converter.convertToSQLDate(demo.getDateCreated()));
                    ps.setInt(4, VOIDED);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            ps.close();

        } catch (SQLException ex) {
            handleException(ex);
        }

    }

    public void loadDateOfBirths() {
        String sql_text = "select person_id,birthdate from person where voided=0";
        PreparedStatement ps = null;
        ResultSet rs = null;
        int count = 0;
        try {
            ps = prepareQuery(sql_text);
            rs = ps.executeQuery();
            while (rs.next()) {
                dateOfBirthMap.put(rs.getInt("person_id"), rs.getDate("birthdate"));
                count++;
                screen.updateStatus("Loading Date of Births..." + count);
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
    }

    public void savePersons(List<Demographics> demoList) {
        String sql_text = "insert into person (person_id,gender,birthdate,dead,creator,date_created,voided,uuid) values(?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Demographics demo : demoList) {
                ps.setInt(1, demo.getPatientID());
                ps.setString(2, demo.getGender());
                ps.setDate(3, Converter.convertToSQLDate(demo.getDateOfBirth()));
                ps.setInt(4, DEAD);
                ps.setInt(5, ADMIN_USER_ID);
                ps.setDate(6, Converter.convertToSQLDate(demo.getDateCreated()));
                ps.setInt(7, VOIDED);
                ps.setString(8, demo.getPatientUUID());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();

        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void savePatientProgram(List<Demographics> demoList, int locationID) {
        enrollPatientsHIVCare(demoList, locationID);
        enrollPatientsToPMTCT(demoList, locationID);
    }

    public void enrollPatientsHIVCare(List<Demographics> demoList, int locationID) {
        String sql_text = "insert into patient_program (patient_id,program_id,date_enrolled,location_id,creator,date_created,voided,uuid) values (?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Demographics demo : demoList) {
                if (demo.getAdultEnrollmentDt() != null) {
                    ps.setInt(1, demo.getPatientID());
                    ps.setInt(2, HIV_CARE_PROGRAM);
                    ps.setDate(3, Converter.convertToSQLDate(demo.getAdultEnrollmentDt()));
                    ps.setInt(4, locationID);
                    ps.setInt(5, ADMIN_USER_ID);
                    ps.setDate(6, Converter.convertToSQLDate(demo.getDateCreated()));
                    ps.setInt(7, VOIDED);
                    ps.setString(8, Converter.generateUUID());
                    ps.addBatch();
                }

            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void enrollPatientsToPMTCT(List<Demographics> demoList, int locationID) {
        String sql_text = "insert into patient_program (patient_id,program_id,date_enrolled,location_id,creator,date_created,voided,uuid) values (?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Demographics demo : demoList) {
                if (demo.getPmtctEnrollmentDt() != null) {
                    ps.setInt(1, demo.getPatientID());
                    ps.setInt(2, PMTCT_PROGRAM);
                    ps.setDate(3, Converter.convertToSQLDate(demo.getPmtctEnrollmentDt()));
                    ps.setInt(4, locationID);
                    ps.setInt(5, ADMIN_USER_ID);
                    ps.setDate(6, Converter.convertToSQLDate(demo.getDateCreated()));
                    ps.setInt(7, VOIDED);
                    ps.setString(8, Converter.generateUUID());
                    ps.addBatch();
                }

            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void savePersonNames(List<Demographics> demoList) {
        String sql_text = "insert into person_name (preferred,given_name,family_name,creator,date_created,voided,uuid,person_id) values(?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Demographics demo : demoList) {
                ps.setInt(1, PREFERRED);
                ps.setString(2, demo.getFirstName());
                ps.setString(3, demo.getLastName());
                ps.setInt(4, ADMIN_USER_ID);
                ps.setDate(5, Converter.convertToSQLDate(demo.getDateCreated()));
                ps.setInt(6, VOIDED);
                ps.setString(7, Converter.generateUUID());
                ps.setInt(8, demo.getPatientID());
                ps.addBatch();
            }

            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void savePersonAddress(List<Demographics> demoList) {
        String sql_text = "insert into person_address "
                + "(person_id,preferred,address1,address2,"
                + "city_village,state_province,country,"
                + "creator,date_created,voided,uuid) "
                + "values(?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Demographics demo : demoList) {
                ps.setInt(1, demo.getPatientID());
                ps.setInt(2, PREFERRED);
                ps.setString(3, demo.getAddress1());
                ps.setString(4, demo.getAddress2());
                ps.setString(5, demo.getAddress_lga());
                ps.setString(6, demo.getAddress_state());
                ps.setString(7, ADDRESS_COUNTRY);
                ps.setInt(8, ADMIN_USER_ID);
                ps.setDate(9, Converter.convertToSQLDate(demo.getDateCreated()));
                ps.setInt(10, VOIDED);
                ps.setString(11, Converter.generateUUID());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }

    }

    public boolean isPatientExisting(int personID) {
        boolean ans = false;
        String sql_text = "select * from patient where patient_id=?";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        try {
            ps.setInt(1, personID);
            rs = ps.executeQuery();
            if (rs.next()) {
                ans = true;
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        }
        return ans;

    }

    public Date getDOBOfPatient(int patientID) {
        Date birthDate = null;
        String sql_text = "select birthdate from person where person_id=? and voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        try {
            ps.setInt(1, patientID);
            rs = ps.executeQuery();
            while (rs.next()) {
                birthDate = rs.getDate("birthdate");
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
        return birthDate;
    }

    public void handleException(Exception ex) {
        screen.updateStatus(ex.getMessage());
        ex.printStackTrace();
    }

    public boolean isProvider(int person_id) throws SQLException {
        boolean ans = false;
        String sql_text = "select user_id from users where person_id=?";
        PreparedStatement ps = prepareQuery(sql_text);
        ps.setInt(1, person_id);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            ans = true;
        }
        cleanup(rs, ps);
        return ans;
    }

    public void migrateObs(String xmlFileName, int locationID) {
        File file = new File(xmlFileName);
        int count = 0;
        List<Obs> obsList = new ArrayList<Obs>();
        Integer[] allowedForms = {1, 56, 65, 67, 18,46,53,20,29,71};
        List<Integer> allowedFormList=new ArrayList<Integer>();
        allowedFormList.addAll(Arrays.asList(allowedForms));
        Obs obs = null;
        dictionary.initializeObsErrorLog();
        //All dictionaries needed for this operation is loaded here
        loadProviderMap();
        loadUserMap();
        loadDateOfBirths();
        try {
            int size = countRecords(xmlFileName, "obs");
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLEventReader eventReader = factory.createXMLEventReader(new FileReader(file));
            //StartElement startElement=null;
            screen.updateMinMaxProgress(0, size);
            XMLEvent xmlEvent2 = null;
            XMLEvent evt = null;
            Characters eventCharacters = null;
            while (eventReader.hasNext()) {
                evt = eventReader.nextEvent();
                if (evt.isStartElement()) {
                    StartElement startElement = evt.asStartElement();
                    if ("obs".equalsIgnoreCase(startElement.getName().getLocalPart())) {
                        obs = new Obs();
                    }
                    switch (startElement.getName().getLocalPart()) {
                        case "OBS_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setObsID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "PATIENT_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setPatientID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "ENCOUNTER_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setEncounterID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "PEPFAR_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setPepfarID(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "HOSP_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setHospID(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "VISIT_DATE":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVisitDate(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "PMM_FORM":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setFormName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "CONCEPT_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setConceptID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VARIABLE_NAME":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVariableName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "VARIABLE_VALUE":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVariableValue(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "ENTERED_BY":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setEnteredBy(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "DATE_CREATED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setDateEntered(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "DATE_CHANGED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setDateChanged(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "PROVIDER":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setProvider(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "UUID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setUuid(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "LOCATION":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setLocationName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "LOCATION_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setLocationID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "CREATOR_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setCreator(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "PROVIDER_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setProviderID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VALUE_NUMERIC":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueNumeric(Converter.convertEventToDouble(xmlEvent2));
                            break;
                        case "VALUE_DATETIME":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueDate(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "VALUE_CODED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueCoded(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VALUE_TEXT":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueText(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "VALUE_BOOL":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueBoolean(Converter.convertEventToBoolean(xmlEvent2));
                            break;
                        case "OBS_GROUP_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setObsGroupID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VOIDED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVoided(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "DATE_VOIDED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setDateVoided(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "VOIDED_BY":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVoidedBy(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "CHANGED_BY":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setChangedBy(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "FORM_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setFormID(Converter.convertEventToInt(xmlEvent2));
                            break;
                    }

                }
                if (evt.isEndElement()) {
                    EndElement endElement = evt.asEndElement();
                    if ("obs".equalsIgnoreCase(endElement.getName().getLocalPart())) {
                        if (obs != null) {
                            screen.updateProgress(count);
                            screen.updateStatus(count + " of " + size + " obs records migrated");
                            if (allowedFormList.contains(obs.getFormID()) && obs.getVoided() == 0) {
                               obsList.add(obs);
                            }
                            count++;
                        }
                        if (count % 1000 == 0) {
                            migrateMigrateForms(obsList, locationID, dateOfBirthMap);
                            obsList.clear();
                        }
                    }
                }
            }
            if (!obsList.isEmpty()) {
                migrateMigrateForms(obsList, locationID, dateOfBirthMap);
                obsList.clear();
            }
            commitConnection();
            dictionary.closeAllResources();
            
        } catch (XMLStreamException xstr) {
            handleException(xstr);
        } catch (FileNotFoundException fne) {
            handleException(fne);
        } catch (SQLException ex) {
            handleException(ex);
        }

    }
    public void trackObsGroupingConcepts(List<Obs> obsList){
        Record record=null;
        for(Obs obs: obsList){
            if(obs.getConceptID()==7778364){
                record=dictionary.getRecordObject(obs.getValueCoded());
                trackMap.put(obs.getObsID(), record);
            }
        }
    }
   
    
    
    public void savePersonName(List<User> userList) {
        String sql_text = "insert into person_name (preferred,person_id,given_name,family_name,creator,date_created,voided,uuid) values (?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (User usr : userList) {
                ps.setInt(1, PREFERRED);
                ps.setInt(2, usr.getPerson_id());
                ps.setString(3, usr.getFirstName());
                ps.setString(4, usr.getLastName());
                ps.setInt(5, ADMIN_USER_ID);
                ps.setDate(6, Converter.convertToSQLDate(new Date()));
                ps.setInt(7, VOIDED);
                ps.setString(8, Converter.generateUUID());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }

    }

    public void savePersonUsers(List<User> prsList) {
        String sql_text = "insert into person (person_id,gender,birthdate_estimated,dead,creator,date_created,voided,uuid) values (?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (User usr : prsList) {
                ps.setInt(1, usr.getPerson_id());
                ps.setString(2, usr.getGender());
                ps.setInt(3, BIRTHDATEESTIMATED);
                ps.setInt(4, DEAD);
                ps.setInt(5, ADMIN_USER_ID);
                ps.setDate(6, Converter.convertToSQLDate(new Date()));
                ps.setInt(7, VOIDED);
                ps.setString(8, Converter.generateUUID());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void saveUsers(List<User> usrList) {
        String sql_text = "insert into users (user_id,system_id,username,password,salt,creator,date_created,person_id,retired,uuid) values (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (User usr : usrList) {
                ps.setInt(1, usr.getUser_id());
                ps.setString(2, Converter.generateUserSystemID(usr));
                ps.setString(3, usr.getUserName());
                ps.setString(4, USERENCRYPTPASSWORD);
                ps.setString(5, USERENCRYPTSALT);
                ps.setInt(6, ADMIN_USER_ID);
                ps.setDate(7, Converter.convertToSQLDate(new Date()));
                ps.setInt(8, usr.getPerson_id());
                ps.setInt(9, USER_RETIRED);
                ps.setString(10, Converter.generateUUID());
                ps.addBatch();

            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void migrateUsers(String xmlFileName) {
        File file = new File(xmlFileName);
        int count = 0;
        int batch_count = 1;
        List<User> userList = new ArrayList<User>();
        User usr = null;
        dictionary.initializeUserErrorLog();
        try {
            int size = countRecords(xmlFileName, "user");
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLEventReader eventReader = factory.createXMLEventReader(new FileReader(file));
            //StartElement startElement=null;
            screen.updateMinMaxProgress(0, size);
            XMLEvent xmlEvent2 = null;
            XMLEvent evt = null;

            while (eventReader.hasNext()) {
                evt = eventReader.nextEvent();
                if (evt.isStartElement()) {
                    StartElement startElement = evt.asStartElement();
                    if ("user".equalsIgnoreCase(startElement.getName().getLocalPart())) {
                        usr = new User();
                    }
                    switch (startElement.getName().getLocalPart()) {
                        case "user_id":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setUser_id(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "person_id":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setPerson_id(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "username":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setUserName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "fullname":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setFullName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "given_name":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setFirstName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "family_name":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setLastName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "gender":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setGender(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "date_created":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setDateCreated(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "creator":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setCreator(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "uuid":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setUuid(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "date_changed":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setDateChanged(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "retired":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setRetired(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "retired_by":
                            xmlEvent2 = eventReader.nextEvent();
                            usr.setRetiredBy(Converter.convertEventToInt(xmlEvent2));
                            break;

                    }
                }
                if (evt.isEndElement()) {
                    EndElement endElement = evt.asEndElement();
                    //If employee tag is closed then add the employee object to list;
                    //and be ready to read next employee data
                    if ("user".equalsIgnoreCase(endElement.getName().getLocalPart())) {
                        if (usr != null && (usr.getPerson_id() != 1 && usr.getPerson_id() != 10) && !existsInList(userList, usr.getPerson_id()) && !StringUtils.equalsIgnoreCase("scheduler", usr.getUserName()) && !StringUtils.equalsIgnoreCase("daemon", usr.getUserName())) {
                            userList.add(usr);
                        } else {
                            dictionary.log(usr);
                        }
                        count++;
                        screen.updateProgress(count);
                        screen.updateStatus(count + " of " + size + " users records migrated in batch: " + batch_count);
                        if ((count % 500) == 0) {
                            migrateUsers(userList);
                            batch_count++;
                            userList.clear();
                        }

                    }

                }

            }
            if (!userList.isEmpty()) {
                migrateUsers(userList);
            }
            dictionary.closeAllResources();
            commitConnection();
        } catch (XMLStreamException xstr) {
            handleException(xstr);
        } catch (FileNotFoundException fne) {
            handleException(fne);
        } catch (SQLException ex) {
            handleException(ex);
        }

    }

    public boolean existsInList(List<User> usrList, int person_id) {
        boolean ans = false;
        for (User usr : usrList) {
            if (usr.getPerson_id() == person_id) {
                ans = true;
            }
        }
        return ans;
    }

    public void commitConnection() throws SQLException {
        if (connection != null) {
            connection.commit();
        }
    }

    public Set<Visit> createVisitSet(List<Obs> obsList, int locationID) {
        Set<Visit> visitSet = new HashSet<Visit>();
        Visit vst = null;
        for (Obs obs : obsList) {
            vst = createVisitFromObs(obs, locationID);
            visitSet.add(vst);
        }
        return visitSet;
    }

    public Set<Provider> createProviderList(List<Obs> obsList, int locationID) {
        Set<Provider> providerList = new HashSet<Provider>();
        Provider provider = null;
        User usr = null;
        for (Obs obs : obsList) {
            usr = usersMap.get(obs.getProviderID());
            provider = createProviderFromUser(usr);
            providerList.add(provider);
        }
        return providerList;
    }

    public Set<Encounter> createEncounterSet(List<Obs> obsList, int locationID) {
        Set<Encounter> encounterSet = new HashSet<Encounter>();
        Encounter enc,enc2 = null;
        for (Obs obs : obsList) {
            enc = createEncounterFromObs(obs, locationID);
            //enc2=createAdditionalFormForInitialEvaluation(obs, locationID);
            encounterSet.add(enc);
            //if(enc2!=null){
                //encounterSet.add(enc2);
            //}
        }
        return encounterSet;
    }

    //Create two EncounterProvider per 
    public EncounterProvider createEncounterProvider(Obs obs) {
        EncounterProvider encPro = new EncounterProvider();
        encPro.setEncounterID(obs.getEncounterID());
        //Go to the provider table with the person_id from obs.getProvider()
        //and retrieve the provider id 
        encPro.setProviderID(obs.getProviderID());
        encPro.setCreator(obs.getCreator());
        encPro.setVoided(VOIDED);
        encPro.setUuid(Converter.generateUUID());
        return encPro;

    }

    public Set<EncounterProvider> createEncounterProvider(List<Obs> obsList) {
        Set<EncounterProvider> encProSet = new HashSet<EncounterProvider>();
        EncounterProvider encPro = null;
        for (Obs obs : obsList) {
            encPro = createEncounterProvider(obs);
            encProSet.add(encPro);
        }
        return encProSet;
    }

    public Set<EncounterProvider> createEncounterProvider(Encounter enc) {
        Set<EncounterProvider> encProSet = new HashSet<EncounterProvider>();
        EncounterProvider dataEntryProviderEP = new EncounterProvider();
        EncounterProvider serviceProviderEP = new EncounterProvider();
        // Creating the serviceProvider EncounterProvider object
        serviceProviderEP.setEncounterID(enc.getEncounterID());
        Provider serviceProvider = providerMap.get(enc.getProviderID());
        int serviceProviderID = 1;
        if (serviceProvider != null) {
            serviceProviderID = serviceProvider.getProviderID();
        }
        serviceProviderEP.setProviderID(serviceProviderID);
        serviceProviderEP.setEncounterRoleID(SERVICE_PROVIDER_ENCOUNTER_ROLE);
        serviceProviderEP.setCreator(enc.getCreator());
        serviceProviderEP.setDateCreated(enc.getDateCreated());
        serviceProviderEP.setVoided(VOIDED);
        serviceProviderEP.setUuid(Converter.generateUUID());
        // Creating the dataEntryProvider EncounterProvider object
        dataEntryProviderEP.setEncounterID(enc.getEncounterID());
        User dataEntryUser = usersMap.get(enc.getCreator());
        int dataEntryUserID = 1;
        int dataEntryProviderID = 1;
        if (dataEntryUser != null) {
            dataEntryUserID = dataEntryUser.getPerson_id();
            Provider dataEntryProvider = providerMap.get(dataEntryUser.getUser_id());
            if (dataEntryProvider != null) {
                dataEntryProviderID = dataEntryProvider.getProviderID();
            }
        }
        dataEntryProviderEP.setProviderID(dataEntryProviderID);
        dataEntryProviderEP.setEncounterRoleID(DATAENTRY_PROVIDER_ENCOUNTER_ROLE);
        dataEntryProviderEP.setCreator(enc.getCreator());
        dataEntryProviderEP.setDateCreated(enc.getDateCreated());
        dataEntryProviderEP.setVoided(VOIDED);
        dataEntryProviderEP.setUuid(Converter.generateUUID());
        //Add the 2 EncounterProvider objects to a set and return set
        encProSet.add(serviceProviderEP);
        encProSet.add(dataEntryProviderEP);
        return encProSet;
    }

    public int getLastVisitID() throws SQLException {
        String sql_text = "select max(visit_id) id from visit where voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = ps.executeQuery();
        int last_id = 0;
        while (rs.next()) {
            last_id = rs.getInt("id");
        }
        return last_id;
    }

    public Set<EncounterProvider> createEncounterProvider(Set<Encounter> encSet) {
        Set<EncounterProvider> encProSet = new HashSet<EncounterProvider>();
        for (Encounter enc : encSet) {
            encProSet.addAll(createEncounterProvider(enc));
        }
        return encProSet;
    }

    public void migrateVisitSet(List<Obs> obsList, int locationID) {
        Set<Visit> visitSet = createVisitSet(obsList, locationID);
        preprocessVisits(visitSet);
        migrateVisits(visitSet);

    }

    public void migrateEncounterSet(List<Obs> obsList, int locationID) {
        Set<Encounter> encounterSet = createEncounterSet(obsList, locationID);
        preprocessEncounters(encounterSet);
        migrateEncounter(encounterSet, locationID);
    }

    public void migrateEncounterProviderSet(List<Obs> obsList, int locationID) {
        Set<Encounter> encounterSet = createEncounterSet(obsList, locationID);
        Set<EncounterProvider> providerSet = createEncounterProvider(encounterSet);
        preprocessEncounterProviders(providerSet);
        migrateEncounterProvider(providerSet);
    }

    public void migrateMigrateForms(List<Obs> obsList, int locationID, Map<Integer, Date> dateMap) {

        Set<Visit> visitSet = createVisitSet(obsList, locationID);

        Set<Encounter> encounterSet = createEncounterSet(obsList, locationID);

        Set<EncounterProvider> providerSet = createEncounterProvider(encounterSet);

        preprocessVisits(visitSet);
        migrateVisits(visitSet);
        preprocessEncounters(encounterSet);
        migrateEncounter(encounterSet, locationID);
        preprocessEncounterProviders(providerSet);
        migrateEncounterProvider(providerSet);
        preprocessObsList(obsList, dateMap);
        migrateObs(obsList, locationID);
        //correctPharmacyObsGroupingConcepts(obsList);
        
    }
    public void correctPharmacyObsGroupingConcepts2(List<Obs> obsList){
        int omrsConceptID=0;
        int omrsValueCoded=0;
        int nmrsConceptID=0;
        int nmrsValueCoded=0;
        int drugNameConceptID=0;
        int multiplyUnit=1;
        for(Obs obs: obsList){
            omrsConceptID=obs.getConceptID();
            if(omrsConceptID==7778364){
                omrsValueCoded=obs.getValueCoded();
                nmrsConceptID=dictionary.getNMRSGroupingConceptID(omrsValueCoded);
                drugNameConceptID=dictionary.getNMRSMedicationNameConceptID(omrsValueCoded);
                updateNMRSDrugGroupingConcepts(obs.getObsGroupID(), nmrsConceptID);
                updateNMRSDrugNameConceptID(obs.getObsID(), drugNameConceptID);
            }
            if(obs.getConceptID()==7778371){
                omrsValueCoded=obs.getValueCoded();
                switch(omrsValueCoded){
                    case 523:
                        multiplyUnit=1;
                        break;
                    case 524:
                        multiplyUnit=30;
                        break;
                    case 520:
                        multiplyUnit=7;
                        break;
                }
                updateDurationCalculation(obs.getObsGroupID(), multiplyUnit);
            }
        }
    }
    public void correctPharmacyObsGroupingConcepts(List<Obs> obsList){
        int valueCoded=0;
        Integer[] targetConcepts={165724,165727,165304};
        List<Integer> targetConceptList=new ArrayList<Integer>();
        targetConceptList.addAll(Arrays.asList(targetConcepts));
        for(Obs obs: obsList){
            if(targetConceptList.contains(obs.getConceptID())){
                valueCoded=obs.getValueCoded();
                int nmrsGroupingConceptID=dictionary.getNMRSGroupingConceptID(valueCoded);
                int medicationNameConceptID=dictionary.getNMRSMedicationNameConceptID(valueCoded);
                updateNMRSDrugGroupingConcepts(obs.getObsGroupID(), nmrsGroupingConceptID);
                updateNMRSDrugNameConceptID(obs.getObsID(), medicationNameConceptID);
                screen.updateStatus("Updating pharmacy obs... ");
            }
        }
        try {
            commitConnection();
            //List<DrugObs> drugNamesObs=getAllObs(OMRS_DRUG_NAME_CONCEPT_ID);//get all drug name concept
            //check the value coded concepts
            //update the obs table based on the value of the 
        } catch (SQLException ex) {
            handleException(ex);
        }
    }
    public void updateDurationCalculation(int obsGroupID, int multiplyUnit){
        String sql_text="update obs set value_numeric=value_numeric*? where obs_group_id=? and concept_id=159368";
        PreparedStatement ps=null;
        ResultSet rs=null;
        try{
           ps=prepareQuery(sql_text);
           ps.setInt(1, multiplyUnit);
           ps.setInt(2, obsGroupID);
           ps.executeUpdate();
           cleanup(rs, ps);
        }catch(SQLException ex){
            handleException(ex);
        }finally{
            cleanup(rs, ps);
        }
    }
     public void secondPass(String xmlFileName, int locationID) {
        File file = new File(xmlFileName);
        int count = 0;
        List<Obs> obsList = new ArrayList<Obs>();
        Integer[] allowedForms = {1, 56, 65, 67, 18,46,53};
        List<Integer> allowedFormList=new ArrayList<Integer>();
        allowedFormList.addAll(Arrays.asList(allowedForms));
        Obs obs = null;
        dictionary.initializeObsErrorLog();
        //All dictionaries needed for this operation is loaded here
        loadProviderMap();
        loadUserMap();
        loadDateOfBirths();
        try {
            int size = countRecords(xmlFileName, "obs");
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLEventReader eventReader = factory.createXMLEventReader(new FileReader(file));
            //StartElement startElement=null;
            screen.updateMinMaxProgress(0, size);
            XMLEvent xmlEvent2 = null;
            XMLEvent evt = null;
            Characters eventCharacters = null;
            while (eventReader.hasNext()) {
                evt = eventReader.nextEvent();
                if (evt.isStartElement()) {
                    StartElement startElement = evt.asStartElement();
                    if ("obs".equalsIgnoreCase(startElement.getName().getLocalPart())) {
                        obs = new Obs();
                    }
                    switch (startElement.getName().getLocalPart()) {
                        case "OBS_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setObsID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "PATIENT_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setPatientID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "ENCOUNTER_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setEncounterID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "PEPFAR_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setPepfarID(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "HOSP_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setHospID(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "VISIT_DATE":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVisitDate(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "PMM_FORM":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setFormName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "CONCEPT_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setConceptID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VARIABLE_NAME":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVariableName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "VARIABLE_VALUE":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVariableValue(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "ENTERED_BY":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setEnteredBy(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "DATE_CREATED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setDateEntered(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "DATE_CHANGED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setDateChanged(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "PROVIDER":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setProvider(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "UUID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setUuid(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "LOCATION":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setLocationName(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "LOCATION_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setLocationID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "CREATOR_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setCreator(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "PROVIDER_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setProviderID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VALUE_NUMERIC":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueNumeric(Converter.convertEventToDouble(xmlEvent2));
                            break;
                        case "VALUE_DATETIME":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueDate(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "VALUE_CODED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueCoded(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VALUE_TEXT":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueText(Converter.convertEventToString(xmlEvent2));
                            break;
                        case "VALUE_BOOL":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setValueBoolean(Converter.convertEventToBoolean(xmlEvent2));
                            break;
                        case "OBS_GROUP_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setObsGroupID(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "VOIDED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVoided(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "DATE_VOIDED":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setDateVoided(Converter.convertEventToDate(xmlEvent2));
                            break;
                        case "VOIDED_BY":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setVoidedBy(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "CHANGED_BY":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setChangedBy(Converter.convertEventToInt(xmlEvent2));
                            break;
                        case "FORM_ID":
                            xmlEvent2 = eventReader.nextEvent();
                            obs.setFormID(Converter.convertEventToInt(xmlEvent2));
                            break;
                    }

                }
                if (evt.isEndElement()) {
                    EndElement endElement = evt.asEndElement();
                    if ("obs".equalsIgnoreCase(endElement.getName().getLocalPart())) {
                        if (obs != null) {
                            screen.updateProgress(count);
                            screen.updateStatus(count + " of " + size + " pharmacy updated");
                            if (allowedFormList.contains(obs.getFormID()) && obs.getVoided() == 0) {
                               obsList.add(obs);
                               
                            }
                            count++;
                        }
                        if (count % 1000 == 0) {
                            correctPharmacyObsGroupingConcepts2(obsList);
                            //migrateMigrateForms(obsList, locationID, dateOfBirthMap);
                            obsList.clear();
                        }
                    }
                }
            }
            if (!obsList.isEmpty()) {
                //migrateMigrateForms(obsList, locationID, dateOfBirthMap);
                correctPharmacyObsGroupingConcepts2(obsList);
                obsList.clear();
            }
            commitConnection();
            dictionary.closeAllResources();
            
        } catch (XMLStreamException xstr) {
            handleException(xstr);
        } catch (FileNotFoundException fne) {
            handleException(fne);
        } catch (SQLException ex) {
            handleException(ex);
        }

    }
     public void updateRemainingGroupingConcepts(){
         String sql_text="update obs set concept_id=162240 where concept_id=7778408";
         PreparedStatement ps=null;
         ResultSet rs=null;
         try{
             ps=prepareQuery(sql_text);
             ps.executeUpdate();
             cleanup(rs, ps);
             commitConnection();
         }catch(SQLException ex){
             handleException(ex);
         }finally{
             cleanup(rs, ps);
         }
     }
    public void updateNMRSDrugGroupingConcepts(int obs_id, int conceptID){
        String sql_text="update obs set concept_id=? where obs_id=? and concept_id=7778408";
        PreparedStatement ps=null;
        ResultSet rs=null;
        try{
            ps=prepareQuery(sql_text);
            ps.setInt(1, conceptID);
            ps.setInt(2, obs_id);
            ps.executeUpdate();
            cleanup(rs, ps);
        }catch(SQLException ex){
            handleException(ex);
        }finally{
            cleanup(rs, ps);
        }
    }
    public void updateNMRSDrugNameConceptID(int obs_id, int conceptID){
        String sql_text="update obs set concept_id=? where obs_id=?";
        PreparedStatement ps=null;
        ResultSet rs=null;
        try{
            ps=prepareQuery(sql_text);
            ps.setInt(1, conceptID);
            ps.setInt(2, obs_id);
            ps.executeUpdate();
            cleanup(rs, ps);
        }catch(SQLException ex){
            handleException(ex);
        }finally{
            cleanup(rs, ps);
        }
    }
    
    public List<DrugObs> getAllObs(int conceptID){
        String sql_text="select obs_id,concept_id,value_coded,obs_group_id from obs where concept_id=?";
        PreparedStatement ps=null;
        ResultSet rs=null;
        DrugObs obsDrug=null;
        List<DrugObs> drugObsList=new ArrayList<DrugObs>();
        try{
            ps=prepareQuery(sql_text);
            ps.setInt(1, conceptID);
            rs=ps.executeQuery();
            while(rs.next()){
                obsDrug=new DrugObs();
                obsDrug.setObsID(rs.getInt("obs_id"));
                obsDrug.setConceptID(rs.getInt("concept_id"));
                obsDrug.setValueCoded(rs.getInt("value_coded"));
                obsDrug.setObsGroupID(rs.getInt("obs_group_id"));
                drugObsList.add(obsDrug);
            }
            cleanup(rs, ps);
        }catch(SQLException ex){
            handleException(ex);
        }finally{
            cleanup(rs, ps);
        }
        return drugObsList;
    }
    public boolean isExistingVisit(Date startDate, int patientID) {
        boolean ans = false;
        String sql_text = "select visit_id from visit where patient_id=? and date_started=? and voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        try {
            ps.setInt(1, patientID);
            ps.setDate(2, Converter.convertToSQLDate(startDate));
            rs = ps.executeQuery();
            while (rs.next()) {
                ans = true;
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
        return ans;
    }

    public boolean isExisting(Visit visit) {
        String sql_text = "select visit_id from visit where patient_id=? and date_started=? and voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        boolean ans = false;
        try {
            ps.setInt(1, visit.getPatientID());
            ps.setDate(2, Converter.convertToSQLDate(visit.getDateStarted()));
            rs = ps.executeQuery();
            while (rs.next()) {
                ans = true;
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
        return ans;
    }

    public void preprocessVisits(Set<Visit> visitSet) {
        System.out.println("Processing visits...");
        if (!visitSet.isEmpty()) {
            for (Visit visit : visitSet) {
                if (isExisting(visit)) {
                    visit.setExists(true);
                }
            }
        }
        //return visitSet;
    }

    public boolean isExisting(Encounter encounter) {
        String sql_text = "select encounter_id from encounter where encounter_id=? and voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        boolean ans = false;
        try {
            ps.setInt(1, encounter.getEncounterID());
            rs = ps.executeQuery();
            while (rs.next()) {
                ans = true;
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
        return ans;
    }

    public int getVisitID(Date visitDate, int patientID) {
        int visitID = 0;
        String sql_text = "select visit_id from visit where patient_id=? and date_started=? and voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        try {
            ps.setInt(1, patientID);
            ps.setDate(2, Converter.convertToSQLDate(visitDate));
            rs = ps.executeQuery();
            while (rs.next()) {
                visitID = rs.getInt("visit_id");
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
        return visitID;
    }

    public void preprocessEncounters(Set<Encounter> encounterSet) {
        int visitID = 0;
        System.out.println("Preprocess Encounters...");
        for (Encounter encounter : encounterSet) {
            dictionary.mapToNMRS(encounter);
            if (isExisting(encounter)) {
                encounter.setExists(true);
                System.out.println("Encounter existing");
            } else {
                System.out.println("Encounter not existing");
                encounter.setExists(false);
            }
            if (isExistingVisit(encounter.getEncounterDatetime(), encounter.getPatientID())) {
                visitID = getVisitID(encounter.getEncounterDatetime(), encounter.getPatientID());
                encounter.setVisitID(visitID);
                System.out.println("Encounter id is: " + encounter.getEncounterID() + " Visit id is " + visitID);
            } else {
                System.out.println("Visit not existing");
            }

            //check if encounter exists
            //if encounter exists update the exists flag
            //Map Encounter to NMRS equivalent (Change FormID and EncounterType) 
            //Update the VisitID for encounter
        }
        //return encounterSet;
    }

    public void preprocessEncounterProviders(Set<EncounterProvider> encounterProviders) {
        System.out.println("Preprocess EncounterProvider...");
        for (EncounterProvider encounterProvider : encounterProviders) {
            if (isExisting(encounterProvider)) {
                encounterProvider.setExists(true);
            } else {
                encounterProvider.setExists(false);
            }
        }
        // return encounterProviders;
    }

    public boolean isExisting(EncounterProvider encounterProvider) {
        boolean ans = false;
        String sql_text = "select encounter_id from encounter_provider where encounter_id=? and provider_id=? and encounter_role_id=? and voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        try {
            ps.setInt(1, encounterProvider.getEncounterID());
            ps.setInt(2, encounterProvider.getProviderID());
            ps.setInt(3, encounterProvider.getEncounterRoleID());
            rs = ps.executeQuery();
            while (rs.next()) {
                ans = true;
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
        return ans;
    }
    
   
    public void preprocessObsList(List<Obs> obsList, Map<Integer, Date> dateMap) {
        //List<Obs> mappedObs = new ArrayList<Obs>();
        
        for (Obs obs : obsList) {
            if (dictionary.isMapped(obs) != null || dictionary.isSpecialConcept(obs)) {
                dictionary.mapToNMRS(obs, dateMap);
                System.out.println("CMap was found");
                obs.setAllowed(true);
            } else {
                obs.setAllowed(false);
                dictionary.log(obs);
            }
            if (isExisting(obs)) {
                obs.setExist(true);
            } else {
                obs.setExist(false);
            }
        }
        System.out.println("Preprocess Obs...");
        //return mappedObs;
    }

    public boolean isExisting(Obs obs) {
        boolean ans = false;
        String sql_text = "select obs_id from obs where obs_id=? and voided=0";
        PreparedStatement ps = prepareQuery(sql_text);
        ResultSet rs = null;
        try {
            ps.setInt(1, obs.getObsID());
            rs = ps.executeQuery();
            while (rs.next()) {
                ans = true;
            }
            cleanup(rs, ps);
        } catch (SQLException ex) {
            handleException(ex);
        } finally {
            cleanup(rs, ps);
        }
        return ans;
    }

    public void handlePS(int pos, int val, PreparedStatement ps) throws SQLException {
        if (val == 0) {
            ps.setNull(pos, java.sql.Types.INTEGER);
        } else {
            ps.setInt(pos, val);
        }
    }

    public void handlePS(int pos, double val, PreparedStatement ps) throws SQLException {
        if (val == 0.0) {
            ps.setNull(pos, java.sql.Types.DOUBLE);
        } else {
            ps.setDouble(pos, val);
        }
    }

    public void migrateObs(List<Obs> obsList, int locationID) {
        String sql_text = "insert into obs (obs_id,person_id,concept_id,encounter_id,"
                + "obs_datetime,location_id,obs_group_id,"
                + "value_coded,value_datetime,"
                + "value_numeric,value_text,creator,"
                + "date_created,voided,uuid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        System.out.println("Migrate Obs...");
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Obs obs : obsList) {
                if (!obs.isExist() && obs.isAllowed()) {
                    ps.setInt(1, obs.getObsID());
                    ps.setInt(2, obs.getPatientID());
                    ps.setInt(3, obs.getConceptID());
                    ps.setInt(4, obs.getEncounterID());
                    ps.setDate(5, Converter.convertToSQLDate(obs.getVisitDate()));
                    ps.setInt(6, locationID);
                    //handlePS(6, obs.getObsGroupID(), ps);
                    handlePS(7, obs.getObsGroupID(), ps);
                    handlePS(8, obs.getValueCoded(), ps);
                    ps.setDate(9, Converter.convertToSQLDate(obs.getValueDate()));
                    handlePS(10, obs.getValueNumeric(), ps);
                    ps.setString(11, obs.getValueText());
                    ps.setInt(12, obs.getCreator());
                    ps.setDate(13, Converter.convertToSQLDate(obs.getDateEntered()));
                    ps.setInt(14, obs.getVoided());
                    ps.setString(15, obs.getUuid());
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            ps.close();
            commitConnection();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public Encounter createEncounterFromObs(Obs obs, int locationID) {
        Encounter enc = new Encounter();
        enc.setEncounterID(obs.getEncounterID());
        //enc.setEncounterType(dictionary.convertEncounterType(obs.getFormID()));
        enc.setPatientID(obs.getPatientID());
        enc.setLocationID(locationID);
        enc.setFormID(obs.getFormID());

        enc.setEncounterDatetime(obs.getVisitDate());
        enc.setCreator(obs.getCreator());
        enc.setDateCreated(obs.getDateEntered());
        enc.setVoided(obs.getVoided());
        enc.setProviderID((obs.getProviderID()));
        // Visit ID not set
        enc.setUuid(Converter.generateUUID());

        return enc;
    }

    public Encounter createAdditionalFormForInitialEvaluation(Obs obs, int locationID) {
        Encounter enc = null;
        int OMRS_ADULT_INITIAL_FORM_ID = 18;
        int OMRS_CARE_CARD_INITIAL_ID=28;
        if (obs.getFormID() == OMRS_ADULT_INITIAL_FORM_ID) {
            enc = new Encounter();
            enc.setEncounterID(obs.getEncounterID());
            //enc.setEncounterType(dictionary.convertEncounterType(obs.getFormID()));
            enc.setPatientID(obs.getPatientID());
            enc.setLocationID(locationID);
            enc.setFormID(OMRS_CARE_CARD_INITIAL_ID);

            enc.setEncounterDatetime(obs.getVisitDate());
            enc.setCreator(obs.getCreator());
            enc.setDateCreated(obs.getDateEntered());
            enc.setVoided(obs.getVoided());
            enc.setProviderID((obs.getProviderID()));
            // Visit ID not set
            enc.setUuid(Converter.generateUUID());
        }
        return enc;
    }

    public Visit createVisitFromObs(Obs obs, int locationID) {
        Visit vst = new Visit();
        vst.setPatientID(obs.getPatientID());
        // Set Visit Type ID
        vst.setDateStarted(obs.getVisitDate());
        vst.setVisitTypeID(VISIT_TYPE_ID);
        vst.setDateStopped(obs.getVisitDate());
        vst.setLocationID(locationID);
        vst.setCreator(obs.getCreator());
        vst.setDateCreated(obs.getDateEntered());
        vst.setVoided(obs.getVoided());
        vst.setUuid(Converter.generateUUID());
        return vst;
    }

    public List<Provider> createProvidersFromUsers(List<User> usrList) {
        List<Provider> providerList = new ArrayList<Provider>();
        Provider provider = null;
        for (User usr : usrList) {
            provider = createProviderFromUser(usr);
            providerList.add(provider);
        }
        return providerList;
    }

    public Provider createProviderFromUser(User usr) {
        Provider provider = new Provider();
        provider.setPersonID(usr.getPerson_id());
        provider.setIdentifier(usr.getUserName());
        provider.setCreator(usr.getCreator());
        provider.setDateCreated(usr.getDateCreated());
        provider.setRetired(usr.getRetired());
        //provider.setProviderRoleID(PREFERRED);
        provider.setUuid(Converter.generateUUID());
        return provider;
    }

    public void migrateVisits(Set<Visit> visitList) {
        System.out.println("Migrating visits...");
        String sql_text = "insert into visit (patient_id,visit_type_id,"
                + "      date_started,date_stopped,location_id,creator,"
                + "      date_created,voided,uuid) values (?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            if (!visitList.isEmpty()) {
                for (Visit vst : visitList) {
                    if (!vst.isExists()) {
                        ps.setInt(1, vst.getPatientID());
                        ps.setInt(2, vst.getVisitTypeID());
                        ps.setDate(3, Converter.convertToSQLDate(vst.getDateStarted()));
                        ps.setDate(4, Converter.convertToSQLDate(vst.getDateStopped()));
                        ps.setInt(5, vst.getLocationID());
                        ps.setInt(6, vst.getCreator());
                        ps.setDate(7, Converter.convertToSQLDate(vst.getDateCreated()));
                        ps.setInt(8, vst.getVoided());
                        ps.setString(9, vst.getUuid());
                        ps.addBatch();
                    }
                }
            }
            ps.executeBatch();
            ps.close();
            //commitConnection();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void migrateEncounterProvider(Set<EncounterProvider> encProviderList) {
        String sql_text = "insert into encounter_provider "
                + "(encounter_id,provider_id,encounter_role_id,creator,date_created,voided,uuid) values (?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        System.out.println("Migrating EncounterProvider...");
        try {
            for (EncounterProvider encounterProvider : encProviderList) {
                if (!encounterProvider.isExists()) {
                    ps.setInt(1, encounterProvider.getEncounterID());
                    ps.setInt(2, encounterProvider.getProviderID());
                    ps.setInt(3, encounterProvider.getEncounterRoleID());
                    ps.setInt(4, encounterProvider.getCreator());
                    ps.setDate(5, Converter.convertToSQLDate(encounterProvider.getDateCreated()));
                    ps.setInt(6, encounterProvider.getVoided());
                    ps.setString(7, encounterProvider.getUuid());
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            ps.close();
            commitConnection();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void migrateEncounter(Set<Encounter> encList, int locationID) {
        String sql_text = "insert into encounter (encounter_id,encounter_type,patient_id,"
                + "location_id,form_id,encounter_datetime,"
                + "creator,date_created,voided,visit_id,uuid) values (?,?,?,?,?,?,?,?,?,?,?)";
        System.out.println("Migrating Encounters...");
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Encounter encounter : encList) {
                if (!encounter.isExists()) {
                    ps.setInt(1, encounter.getEncounterID());
                    ps.setInt(2, encounter.getEncounterType());
                    ps.setInt(3, encounter.getPatientID());
                    ps.setInt(4, locationID);
                    ps.setInt(5, encounter.getFormID());
                    ps.setDate(6, Converter.convertToSQLDate(encounter.getEncounterDatetime()));
                    ps.setInt(7, encounter.getCreator());
                    ps.setDate(8, Converter.convertToSQLDate(encounter.getDateCreated()));
                    ps.setInt(9, VOIDED);
                    ps.setInt(10, encounter.getVisitID());
                    ps.setString(11, encounter.getUuid());
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            ps.close();
            //commitConnection();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void migrateProviders(List<Provider> providerList) {
        String sql_text = "insert into provider (person_id,identifier,"
                + "creator,date_created,retired,uuid,provider_role_id) values (?,?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (Provider prv : providerList) {
                ps.setInt(1, prv.getPersonID());
                ps.setString(2, prv.getIdentifier());
                ps.setInt(3, prv.getCreator());
                ps.setDate(4, Converter.convertToSQLDate(prv.getDateCreated()));
                ps.setInt(5, prv.getRetired());
                ps.setString(6, prv.getUuid());
                ps.setInt(7, prv.getProviderRoleID());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public void migrateUsers(List<User> usrList) {

        savePersonUsers(usrList);
        savePersonName(usrList);
        saveUsers(usrList);
        saveProvider(usrList);
    }

    public boolean isExisting(User usr) {
        String sql_text = "";
        return false;
    }

    public void preprocessUsers(List<User> usrList) {

    }

    public void saveProvider(List<User> usrList) {
        String sql_text = "insert into provider (person_id,identifier,creator,date_created,retired,uuid) values (?,?,?,?,?,?)";
        PreparedStatement ps = prepareQuery(sql_text);
        try {
            for (User usr : usrList) {
                ps.setInt(1, usr.getPerson_id());
                ps.setString(2, Converter.generateUserSystemID(usr));
                ps.setInt(3, usr.getCreator());
                ps.setDate(4, Converter.convertToSQLDate(usr.getDateCreated()));
                ps.setInt(5, 0);
                ps.setString(6, Converter.generateUUID());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } catch (SQLException ex) {
            handleException(ex);
        }
    }

    public List<Location> getLocations() {
        List<Location> locationList = new ArrayList<Location>();
        try {
            String query = "select location_id, TRIM(UPPER(name)) location_name from location where parent_location is null order by location_name desc";
            PreparedStatement ps = prepareQuery(query);
            ResultSet result = ps.executeQuery();
            Location loc = null;
            String locationName = null;
            int location_id = 0;
            while (result.next()) {
                location_id = result.getInt("location_id");
                locationName = result.getString("location_name");
                loc = new Location();
                loc.setLocationID(location_id);
                loc.setLocationName(locationName);
                locationList.add(loc);
            }
            result.close();
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return locationList;
    }
    
}
