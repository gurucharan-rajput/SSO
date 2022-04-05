/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.io;

import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import com.erwin.cfx.connectors.ssis.generic.util.BoxingAndUnBoxingWrapper;
import com.erwin.cfx.connectors.ssis.generic.util.ConnectionStringUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/harika
 * @Date : 16-08-2021
 */
public class XMLUtil {

    XMLUtil() {
    }

    /**
     * this method will take XML file as an input and return the Document object
     * which can used to parse the XML file later
     *
     * @author : Dinesh Arasankala
     * @param inputFile
     * @return Document
     */
    public static Document returnTheDocument(File inputFile) {
        Document xmlDocument = null;

        try (FileInputStream fileInputStream = new FileInputStream(inputFile)) {
            DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();

            DocumentBuilder builder = builderFactory.newDocumentBuilder();

            xmlDocument = builder.parse(fileInputStream);
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--returnTheDocument(-,-)");
            SSISController.logger.error(e.getMessage());
        }

        return xmlDocument;
    }

    /**
     * this method will take XML document of DTSX file as input and returns the
     * package format version number back to the code
     *
     * @author : Dinesh Arasankala
     * @param xmlDocument
     * @return SSIS packageFormat version number
     */
    public static int getPackageFormatVersionFromDtsxfile(Document xmlDocument) {
        int packageFormatVersionNo = 0;
        try {

            String packageFormatVersion = "";
            String pcgFormatNodeValue = "";
            XPath xPath = XPathFactory.newInstance().newXPath();
            String packageFormatPath = "//Executable/Property";
            NodeList packageFormatNodeList = (NodeList) xPath.compile(packageFormatPath).evaluate(xmlDocument, XPathConstants.NODESET);
            for (int i = 0; i < packageFormatNodeList.getLength(); i++) {
                Node packageFormatNode = packageFormatNodeList.item(i);
                if (packageFormatNode != null && "DTS:Property".equalsIgnoreCase(packageFormatNode.getNodeName())) {

                    if (packageFormatNode.getAttributes().getNamedItem("DTS:Name") != null) {
                        pcgFormatNodeValue = packageFormatNode.getAttributes().getNamedItem("DTS:Name").getTextContent();
                    }

                    if ("PackageFormatVersion".equalsIgnoreCase(pcgFormatNodeValue)) {
                        packageFormatVersion = packageFormatNode.getTextContent();

                        packageFormatVersionNo = BoxingAndUnBoxingWrapper.convertStringToInteger(packageFormatVersion);
                        break;
                    }

                }

            }
        } catch (Exception e) {
            packageFormatVersionNo = 0;
            SSISController.logger.info("methodName :--getPackageFormatVersionFromDtsxfile(-,-)");
            SSISController.logger.error(e.getMessage());
        }

        return packageFormatVersionNo;
    }

    /**
     * this method will take XML document of DTSX file(2014) as input and
     * returns the package build number back to the code
     *
     * @param xmlDocument
     * @return packageBuildNumber
     */
    public static int getPackageBuildNumberFromDTSXFile_2014(Document xmlDocument) {
        int versionBuildNumber = 0;
        try {

            String versionBuildNodeValue = "";
            XPath xPath = XPathFactory.newInstance().newXPath();
            String packageFormatPath = "Executable";
            NodeList packageFormatNodeList = (NodeList) xPath.compile(packageFormatPath).evaluate(xmlDocument, XPathConstants.NODESET);
            for (int i = 0; i < packageFormatNodeList.getLength(); i++) {
                Node packageFormatNode = packageFormatNodeList.item(i);

                if (packageFormatNode != null) {
                    versionBuildNodeValue = packageFormatNode.getAttributes().getNamedItem("DTS:VersionBuild").getTextContent();
                    versionBuildNumber = BoxingAndUnBoxingWrapper.convertStringToInteger(versionBuildNodeValue);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            versionBuildNumber = 0;
            SSISController.logger.info("methodName :--getPackageBuildNumberFromDTSXFile_2014(-,-)");
            SSISController.logger.error(e.getMessage());
        }

        return versionBuildNumber;
    }

    /**
     * this method will take XML document of DTSX file(2008) as input and
     * returns the package build number back to the code
     *
     * @param xmlDocument
     * @return packageBuildNumber
     */
    public static int getPackageBuildNumberFromDTSXFile_2008(Document xmlDocument) {
        int versionBuildNumber = 0;
        try {

            String versionBuildNumberString = "";
            String pcgFormatNodeValue = "";
            XPath xPath = XPathFactory.newInstance().newXPath();
            String packageFormatPath = "//Executable/Property";
            NodeList packageFormatNodeList = (NodeList) xPath.compile(packageFormatPath).evaluate(xmlDocument, XPathConstants.NODESET);
            for (int i = 0; i < packageFormatNodeList.getLength(); i++) {
                if ("DTS:Property".equalsIgnoreCase(packageFormatNodeList.item(i).getNodeName())) {
                    try {
                        pcgFormatNodeValue = packageFormatNodeList.item(i).getAttributes().getNamedItem("DTS:Name").getTextContent();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if ("VersionBuild".equalsIgnoreCase(pcgFormatNodeValue)) {
                        versionBuildNumberString = packageFormatNodeList.item(i).getTextContent();

                        versionBuildNumber = Integer.parseInt(versionBuildNumberString);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--getPackageBuildNumberFromDTSXFile_2008(-,-)");
            SSISController.logger.error(e.getMessage());
        }

        return versionBuildNumber;
    }

    /**
     * this method creates a hash map containing connection keys with
     * corresponding values present in data source PARAM file
     *
     * @param filePath
     */
    public static void prepareConnectionPropertiesMapFromFile(String filePath, ACPInputParameterBean acpInputParameterBean) {
        Map<String, String> connectionPropertiesHashMap = new HashMap<>();
        HashMap<String, String> openQueryHashMap = new HashMap<>();
        File filePathFile = new File(filePath);
        String propertyFileContent = "";
        try {
            if (filePathFile.exists() && filePathFile.isFile()) {
                propertyFileContent = FileUtils.readFileToString(filePathFile, "utf-8");

                String[] splitPropertyFileContent = propertyFileContent.split("\n");

                for (String eachConnectionDetails : splitPropertyFileContent) {

                    if (!StringUtils.isBlank(eachConnectionDetails)) {

                        int length = 0;
                        length = eachConnectionDetails.split(Pattern.quote("||")).length;
                        String connectionName = "";
                        String connectionString = "";
                        if (length <= 1) {
                            connectionName = eachConnectionDetails.split(Pattern.quote("||"))[0];
                        } else if (length >= 2) {
                            connectionName = eachConnectionDetails.split(Pattern.quote("||"))[0];
                            connectionString = eachConnectionDetails.split(Pattern.quote("||"))[1];
                            if (!connectionName.toLowerCase().contains("openquery")) {
                                connectionString = ConnectionStringUtil.getDatabaseAndServerNameFromConnectionString(connectionString);
                            }

                        }
                        if (connectionName.toLowerCase().contains("openquery")) {

                            String updatedConnectionName = "";
                            String dbType = "";
                            String databaseAndServerConnetionString = "";
                            String databaseName = "";
                            String serverName = "";
                            try {
                                if (connectionName.contains("%%") && connectionName.split("%%").length >= 1) {
                                    updatedConnectionName = connectionName.split("%%")[0];
                                }

                                if (connectionString.contains("%%") && connectionString.split("%%").length >= 2) {

                                    if (connectionString.split("%%")[1].split("=").length >= 2) {
                                        dbType = connectionString.split("%%")[1].split("=")[1];
                                    }
                                    databaseAndServerConnetionString = connectionString.split("%%")[0];
                                    databaseAndServerConnetionString = ConnectionStringUtil.getDatabaseAndServerNameFromConnectionStringForOpenQuery(databaseAndServerConnetionString);

                                }
                                try {
                                    if (StringUtils.isNotBlank(databaseAndServerConnetionString)) {
                                        serverName = databaseAndServerConnetionString.split(Delimiter.delimiter)[0];
                                        databaseName = databaseAndServerConnetionString.split(Delimiter.delimiter)[1];
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                                if (StringUtils.isNotBlank(updatedConnectionName) && StringUtils.isNotBlank(dbType)) {
                                    updatedConnectionName = updatedConnectionName.trim();
                                    dbType = dbType.trim();
                                    openQueryHashMap.put(updatedConnectionName, serverName + Delimiter.delimiter + databaseName + Delimiter.delimiter + dbType);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        } else if (!StringUtils.isBlank(connectionName) && !StringUtils.isBlank(connectionString)) {

                            connectionPropertiesHashMap.put(connectionName, connectionString);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--prepareConnectionPropertiesMapFromFile(-,-)");
            SSISController.logger.error(e.getMessage());
        }
        acpInputParameterBean.setOpenQueryHashMap(openQueryHashMap);
        acpInputParameterBean.setConnectionPropertiesHashMap(connectionPropertiesHashMap);
    }

    /**
     * this method prepares a hash map containing project level($Project)
     * reference connections present in XML file with .CONMGR extension
     *
     * @param inputFile
     *
     */
    public static void getProjectLevelConnectionInfo(File inputFile, ACPInputParameterBean acpInputParameterBean, Map<String, String> projectLevelConnectionInfo) {
        try {
            Map<String, String> connectionPropertiesHashMap = acpInputParameterBean.getConnectionPropertiesHashMap();
            String ObjectName = "";
            String dtsId = "";
            String connectionInfo = "";
            Document doc = XMLUtil.returnTheDocument(inputFile);
            NodeList listOfConnectionManager = doc.getElementsByTagName("DTS:ConnectionManager");
            for (int eg = 0; eg < listOfConnectionManager.getLength(); eg++) {
                Node nodeConnectionManager = listOfConnectionManager.item(eg);
                if (nodeConnectionManager.getNodeName().equals("DTS:ConnectionManager")) {
                    if (nodeConnectionManager.getAttributes().getNamedItem("DTS:ObjectName") != null) {
                        ObjectName = nodeConnectionManager.getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                    }
                    if (nodeConnectionManager.getAttributes().getNamedItem("DTS:DTSID") != null) {
                        dtsId = nodeConnectionManager.getAttributes().getNamedItem("DTS:DTSID").getTextContent();
                    }
                    if (nodeConnectionManager.getAttributes().getNamedItem("DTS:ConnectionString") != null) {
                        String connectionString = nodeConnectionManager.getAttributes().getNamedItem("DTS:ConnectionString").getTextContent();

                        boolean flag = true;
                        try {
                            String dsnName = ConnectionStringUtil.getDataSourceOrDSNFromConnectionString(connectionString);
                            if (connectionPropertiesHashMap.get(dsnName) != null) {
                                connectionString = connectionPropertiesHashMap.get(dsnName).toString();
                                connectionInfo = connectionString;
                                flag = false;
                            } else if (connectionPropertiesHashMap.get(ObjectName) != null) {
                                connectionString = connectionPropertiesHashMap.get(ObjectName).toString();
                                connectionInfo = connectionString;
                                flag = false;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (flag) {
                            connectionInfo = ConnectionStringUtil.getDatabaseAndServerNameFromConnectionString(connectionString);
                        }
                    }
                }
            }
            projectLevelConnectionInfo.put(dtsId, connectionInfo);
            if (!StringUtils.isBlank(ObjectName)) {
                projectLevelConnectionInfo.put(ObjectName, connectionInfo);
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--getProjectLevelConnectionInfo(-,-)");
            SSISController.logger.error(e.getMessage());
        }
        acpInputParameterBean.setProjectLevelConnectionInfo(projectLevelConnectionInfo);
    }

    /**
     * this method creates a hash map containing project level variables present
     * in XML file with .PARAM extension
     *
     * @param inputFile
     * @return projectParamHashMap
     */
    public static void getProjectLevelParamFileData(File inputFile, ACPInputParameterBean acpInputParameterBean, Map<String, String> projectLevelVariableInfo) {

        try {
            Document doc = XMLUtil.returnTheDocument(inputFile);
            NodeList listOfProjectParam = doc.getElementsByTagName("SSIS:Parameters");
            for (int i = 0; i < listOfProjectParam.getLength(); i++) {
                NodeList parameterList = listOfProjectParam.item(i).getChildNodes();
                String parmeterName = "";
                String parmeterValue = "";

                for (int j = 0; j < parameterList.getLength(); j++) {
                    if ("SSIS:Parameter".equalsIgnoreCase(parameterList.item(j).getNodeName())) {
                        parmeterName = parameterList.item(j).getAttributes().getNamedItem("SSIS:Name").getNodeValue();
                        NodeList propertiesList = parameterList.item(j).getChildNodes();
                        for (int k = 0; k < propertiesList.getLength(); k++) {
                            if ("SSIS:Properties".equalsIgnoreCase(propertiesList.item(k).getNodeName())) {
                                NodeList propertyList = propertiesList.item(k).getChildNodes();
                                for (int l = 0; l < propertyList.getLength(); l++) {
                                    if ("SSIS:Property".equalsIgnoreCase(propertyList.item(l).getNodeName())) {
                                        if ("Value".equalsIgnoreCase(propertyList.item(l).getAttributes().getNamedItem("SSIS:Name").getNodeValue())) {
                                            parmeterValue = propertyList.item(l).getTextContent();
                                            projectLevelVariableInfo.put(parmeterName, parmeterValue);
                                            parmeterName = "";
                                            parmeterValue = "";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            SSISController.logger.info("methodName :--getProjectLevelParamFileData(-,-)");
            SSISController.logger.error(e.getMessage());
        }
        acpInputParameterBean.setProjectLevelVaraiblesMap(projectLevelVariableInfo);
    }

}
