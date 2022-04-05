/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2014;

import com.erwin.cfx.connectors.ssis.generic.util.ConnectionStringUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 18-08-2021
 */
public class Parser2014XMLFile {

    List<String> controlFlowObjectNameList = new ArrayList<>();

    HashSet<String> controlFlowComponentNameSet = new HashSet<>();

    /**
     * this method return a hash map containing key as component name and value
     * as query for a given node list
     *
     * @param siblingsList
     * @param connections
     * @return
     */
    public HashMap<String, String> readQueriesFromControlFlowComponents(NodeList siblingsList, Map<String, String> connections) {
        HashMap<String, String> extendedProperties = new HashMap<>();
        String objectName = "";
        String sequenceContainerName = "";

        for (int j = 0; j < siblingsList.getLength(); j++) {
            if ("DTS:Executables".equalsIgnoreCase(siblingsList.item(j).getNodeName())) {
                NodeList executableList = siblingsList.item(j).getChildNodes();

                for (int k = 0; k < executableList.getLength(); k++) {
                    Node executableListNode = executableList.item(k);

                    if (executableListNode != null && Constants.executableLiteral.equalsIgnoreCase(executableList.item(k).getNodeName())) {

                        objectName = executableList.item(k).getAttributes().getNamedItem(Constants.objectNameLiteral).getTextContent();
                        String disabledProprety = "";
                        try {
                            if (executableList.item(k).getAttributes().getNamedItem("DTS:Disabled") != null) {
                                disabledProprety = executableList.item(k).getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                            }
                            if (disabledProprety.toUpperCase().contains("TRUE")) {
                                continue;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        sequenceContainerName = "";
                        try {
                            Node rootparentNode = executableList.item(k).getParentNode().getParentNode();
                            sequenceContainerName = rootparentNode.getAttributes().getNamedItem(Constants.objectNameLiteral).getTextContent();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        String creationName = executableList.item(k).getAttributes().getNamedItem("DTS:CreationName").getTextContent();
                        if ("Microsoft.Pipeline".equalsIgnoreCase(creationName) || "#text".equalsIgnoreCase(executableList.item(k).getNodeName()) || "STOCK:SEQUENCE".equalsIgnoreCase(creationName) || "STOCK:FOREACHLOOP".equalsIgnoreCase(creationName)) {
                            continue;
                        } else {
                            NodeList objectDataList = executableList.item(k).getChildNodes();
                            for (int i = 0; i < objectDataList.getLength(); i++) {
                                if ("DTS:ObjectData".equalsIgnoreCase(objectDataList.item(i).getNodeName())) {
                                    NodeList sQLTaskList = objectDataList.item(i).getChildNodes();
                                    if (objectDataList.item(i) == null) {
                                        continue;
                                    }
                                    for (int l = 0; l < sQLTaskList.getLength(); l++) {
                                        if (!"SQLTask:SqlTaskData".equalsIgnoreCase(sQLTaskList.item(l).getNodeName()) || sQLTaskList.item(l).getAttributes().getNamedItem("SQLTask:SqlStatementSource") == null) {
                                            continue;
                                        }
                                        String query = sQLTaskList.item(l).getAttributes().getNamedItem("SQLTask:SqlStatementSource").getTextContent();
                                        String sqlConnection = sQLTaskList.item(l).getAttributes().getNamedItem("SQLTask:Connection").getTextContent();

                                        String serverName = "";
                                        String databaseName = "";
                                        try {
                                            if (!StringUtils.isBlank(sqlConnection)) {
                                                String connectionInfo = connections.get(sqlConnection);
                                                connectionInfo = updatedConnectionInfo(connectionInfo);
                                                if (connectionInfo != null) {
                                                    String[] connectionInfoArray = connectionInfo.split(Delimiter.delimiter);
                                                    int length = connectionInfoArray.length;
                                                    switch (length) {
                                                        case 2:
                                                            serverName = connectionInfoArray[0];
                                                            databaseName = connectionInfoArray[1];
                                                            break;
                                                        case 1:
                                                            serverName = connectionInfoArray[0];
                                                            break;
                                                        default:
                                                            serverName = "";
                                                            databaseName = "";
                                                            break;

                                                    }
                                                }

                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        String sequnceContainerNameWithObjectName = objectName + "_" + sequenceContainerName;
                                        if (controlFlowObjectNameList.contains(objectName) && !StringUtils.isBlank(sequenceContainerName) && !controlFlowObjectNameList.contains(objectName + "_" + sequenceContainerName)) {
                                            objectName = sequnceContainerNameWithObjectName;
                                        }
                                        controlFlowObjectNameList.add(objectName);
                                        controlFlowObjectNameList.add(sequnceContainerNameWithObjectName);
                                        String key = objectName + "$queryName";
                                        if ((!controlFlowComponentNameSet.contains(key) && SSIS2014.missingControlFlowComponentExtendedPropertiesFlag) || SSIS2014.normalControlFlowComponentExtendedPropertiesFlag) {
                                            extendedProperties.put(key, query + Delimiter.delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName);
                                        }

                                    }
                                } else if ("DTS:Executables".equalsIgnoreCase(objectDataList.item(i).getNodeName())) {
                                    NodeList childExecutables = objectDataList.item(i).getChildNodes();
                                    for (int l = 0; l < childExecutables.getLength(); l++) {
                                        if ("DTS:Executable".equalsIgnoreCase(childExecutables.item(l).getNodeName())) {
                                            objectName = childExecutables.item(l).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                                            try {
                                                disabledProprety = childExecutables.item(l).getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                                                if (disabledProprety.toUpperCase().contains("TRUE")) {
                                                    continue;
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }
                                            sequenceContainerName = "";
                                            try {
                                                Node rootparentNode = childExecutables.item(l).getParentNode().getParentNode();
                                                sequenceContainerName = rootparentNode.getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }
                                            String childExeCreationName = childExecutables.item(l).getAttributes().getNamedItem("DTS:CreationName").getTextContent();

                                            if ("Microsoft.Pipeline".equalsIgnoreCase(childExeCreationName) || "#text".equalsIgnoreCase(childExecutables.item(l).getNodeName())) {
                                                continue;
                                            } else {
                                                NodeList childObjectDataList = childExecutables.item(l).getChildNodes();
                                                for (int m = 0; m < childObjectDataList.getLength(); m++) {
                                                    if ("DTS:ObjectData".equalsIgnoreCase(childObjectDataList.item(m).getNodeName())) {
                                                        NodeList sQLTaskList = childObjectDataList.item(m).getChildNodes();
                                                        if (childObjectDataList.item(m) == null) {
                                                            continue;
                                                        }
                                                        for (int n = 0; n < sQLTaskList.getLength(); n++) {
                                                            if (!"SQLTask:SqlTaskData".equalsIgnoreCase(sQLTaskList.item(n).getNodeName()) || sQLTaskList.item(n).getAttributes().getNamedItem("SQLTask:SqlStatementSource") == null) {
                                                                continue;
                                                            }
                                                            String queryconnection = sQLTaskList.item(n).getAttributes().getNamedItem("SQLTask:Connection").getTextContent();
                                                            String query = sQLTaskList.item(n).getAttributes().getNamedItem("SQLTask:SqlStatementSource").getTextContent();

                                                            String serverName = "";
                                                            String databaseName = "";
                                                            try {
                                                                if (!StringUtils.isBlank(queryconnection)) {
                                                                    String connectionInfo = connections.get(queryconnection);

                                                                    connectionInfo = updatedConnectionInfo(connectionInfo);
                                                                    if (connectionInfo != null) {
                                                                        String[] connectionInfoArray = connectionInfo.split(Delimiter.delimiter);
                                                                        int length = connectionInfoArray.length;
                                                                        switch (length) {
                                                                            case 2:
                                                                                serverName = connectionInfoArray[0];
                                                                                databaseName = connectionInfoArray[1];
                                                                                break;
                                                                            case 1:
                                                                                serverName = connectionInfoArray[0];
                                                                                break;
                                                                            default:
                                                                                serverName = "";
                                                                                databaseName = "";
                                                                                break;

                                                                        }
                                                                    }
                                                                }
                                                            } catch (Exception e) {
                                                                e.printStackTrace();
                                                            }
                                                            String sequnceContainerNameWithObjectName = objectName + "_" + sequenceContainerName;
                                                            if (controlFlowObjectNameList.contains(objectName) && !StringUtils.isBlank(sequenceContainerName) && !controlFlowObjectNameList.contains(sequnceContainerNameWithObjectName)) {
                                                                objectName = sequnceContainerNameWithObjectName;
                                                            }
                                                            controlFlowObjectNameList.add(objectName);
                                                            controlFlowObjectNameList.add(sequnceContainerNameWithObjectName);

                                                            String key = objectName + "$queryName";
                                                            if ((!controlFlowComponentNameSet.contains(key) && SSIS2014.missingControlFlowComponentExtendedPropertiesFlag) || SSIS2014.normalControlFlowComponentExtendedPropertiesFlag) {
                                                                extendedProperties.put(key, query + Delimiter.delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName);
                                                            }

                                                        }
                                                    }
                                                }

                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

            }
        }
        return extendedProperties;
    }

    /**
     * this method updates the given connection string with the actual value if
     * it contains any user,project or package reference variable
     *
     * @param connectionString
     * @return
     */
    public String updatedConnectionInfo(String connectionString) {
        String defaultConnectionString = "";
        try {
            if (!StringUtils.isBlank(connectionString) && connectionString.contains(Delimiter.connectionStringDelimiter)) {
                defaultConnectionString = connectionString.split(Delimiter.connectionStringDelimiter)[1];
                connectionString = connectionString.split(Delimiter.connectionStringDelimiter)[0];
                if (!StringUtils.isBlank(connectionString)) {
                    if (connectionString.toLowerCase().contains("user")) {
                        connectionString = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbols(connectionString, "user", "");
                    } else if (connectionString.toLowerCase().contains("project")) {
                        connectionString = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbols(connectionString, "$Project", "");
                    } else if (connectionString.toLowerCase().contains("package")) {
                        connectionString = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbols(connectionString, "$Package", "");
                    }

                    if (connectionString.contains(Delimiter.variableReplacementDelimiter)) {
                        connectionString = connectionString.split(Delimiter.variableReplacementDelimiter)[0];

                    }
                    if (connectionString.toLowerCase().contains("data source") || connectionString.toLowerCase().contains("initial catalog")) {
                        connectionString = ConnectionStringUtil.getDatabaseAndServerNameFromConnectionString(connectionString);
                    } else {
                        connectionString = defaultConnectionString;
                    }

                } else {
                    connectionString = defaultConnectionString;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionString;
    }

    /**
     * this method will take XML document as input and read the variables tag
     * inside the DTSX file to prepare the variable hash map
     *
     * @author Dinesh Arasankala
     * @param xmlDocument
     * @return variableMap
     */
    public Map<String, String> prepareVariablesMap(Document xmlDocument) {
        Map<String, String> variableMap = null;
        try {
            variableMap = new LinkedHashMap<>();
            XPath xPath = XPathFactory.newInstance().newXPath();
            String variablepath = "//Executable/Variables/Variable";
            NodeList controlFlowLevelNodeList = (NodeList) xPath.compile(variablepath).evaluate(xmlDocument, XPathConstants.NODESET);
            String variableName = "";
            String expression = "";
            String variableValue = "";
            for (int i = 0; i < controlFlowLevelNodeList.getLength(); i++) {

                if ("DTS:Variable".equalsIgnoreCase(controlFlowLevelNodeList.item(i).getNodeName())) {
                    variableName = controlFlowLevelNodeList.item(i).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                    if (controlFlowLevelNodeList.item(i).getAttributes().getNamedItem(Constants.expressionLiteral) != null) {
                        expression = controlFlowLevelNodeList.item(i).getAttributes().getNamedItem(Constants.expressionLiteral).getTextContent();
                        expression = StringEscapeUtils.unescapeXml(expression);

                        expression = StringUtils.removeStart(expression, "\"");
                        expression = StringUtils.removeEnd(expression, "\"");

                    } else if (controlFlowLevelNodeList.item(i).getAttributes().getNamedItem("DTS:Expression") == null) {
                        NodeList childelevel = controlFlowLevelNodeList.item(i).getChildNodes();
                        for (int ch = 0; ch < childelevel.getLength(); ch++) {
                            String nodeValue = childelevel.item(ch).getNodeName();
                            if ("DTS:VariableValue".equalsIgnoreCase(nodeValue)) {
                                variableValue = childelevel.item(ch).getTextContent();
                            }
                        }

                    }

                }
                if (!StringUtils.isBlank(variableName) && !StringUtils.isBlank(expression)) {

                    variableMap.put(variableName, expression);
                    variableName = "";
                    expression = "";
                } else if (!StringUtils.isBlank(variableName) && !StringUtils.isBlank(variableValue)) {

                    variableMap.put(variableName, variableValue);
                    variableName = "";
                    variableValue = "";
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return variableMap;
    }

    /**
     * this method will take XML document as input and read the
     * PackageParameters tag inside the DTSX file to prepare the
     * PackageParameters hash map
     *
     * @author Dinesh Arasankala
     * @param xmlDocument
     * @return PackageParametersHashMap
     */
    public Map<String, String> preparePackageParametersVaraibles(Document xmlDocument) {
        HashMap<String, String> packageParameterHashmap = null;
        try {
            packageParameterHashmap = new HashMap<>();
            XPath xPath = XPathFactory.newInstance().newXPath();
            String packageParametersVariablepath = "//Executable/PackageParameters/PackageParameter";
            NodeList controlFlowLevelNodeList = (NodeList) xPath.compile(packageParametersVariablepath).evaluate(xmlDocument, XPathConstants.NODESET);
            String variableName = "";
            String variableValue = "";
            for (int i = 0; i < controlFlowLevelNodeList.getLength(); i++) {

                if ("DTS:PackageParameter".equalsIgnoreCase(controlFlowLevelNodeList.item(i).getNodeName())) {
                    variableName = controlFlowLevelNodeList.item(i).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();

                    NodeList childelevel = controlFlowLevelNodeList.item(i).getChildNodes();
                    for (int ch = 0; ch < childelevel.getLength(); ch++) {
                        String nodeValue = childelevel.item(ch).getNodeName();

                        if ("DTS:Property".equalsIgnoreCase(nodeValue)) {
                            variableValue = childelevel.item(ch).getTextContent();
                        }
                    }

                }
                if (!StringUtils.isBlank(variableName) && !StringUtils.isBlank(variableValue)) {

                    packageParameterHashmap.put(variableName, variableValue);
                    variableName = "";
                    variableValue = "";
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return packageParameterHashmap;
    }

    /**
     * this method will take input as XML document read the ConnectionManagers
     * tag inside the DTSX file to prepare the Connections hash map
     *
     * @param xmlDocument
     * @param xPath
     * @return Connections HashMap
     */
    @SuppressWarnings("all")
    public Map<String, String> prepareConnectionsMap(Document xmlDocument, XPath xPath, Map<String, String> connectionPropertiesHashMap) {
        Map<String, String> connections = new HashMap<>();
        String connectionManagerPath = "//Executable/ConnectionManagers";
        String connectionInfo = "";

        try {
            NodeList connectionManagerNodeList = (NodeList) xPath.compile(connectionManagerPath).evaluate(xmlDocument, XPathConstants.NODESET);
            for (int i = 0; i < connectionManagerNodeList.getLength(); i++) {
                Node rootparentNode = connectionManagerNodeList.item(i).getParentNode();
                if (rootparentNode.getParentNode() == null) {
                    continue;
                }

                NodeList rootChildNodeList = connectionManagerNodeList.item(i).getChildNodes();
                for (int j = 0; j < rootChildNodeList.getLength(); j++) {
                    if ("DTS:ConnectionManager".equalsIgnoreCase(rootChildNodeList.item((j)).getNodeName())) {
                        String connectionRefId = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:refId").getTextContent();

                        String connectionName = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                        String DTSId = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:DTSID").getTextContent();
                        NodeList conManObjectDataNodeList = rootChildNodeList.item(j).getChildNodes();
                        String propertyExpressionConnectionString = "";
                        boolean propertExpressionFlag = true;
                        for (int k = 0; k < conManObjectDataNodeList.getLength(); k++) {

                            if (connectionPropertiesHashMap.get(connectionName) != null) {
                                propertyExpressionConnectionString = connectionPropertiesHashMap.get(connectionName);
                                propertExpressionFlag = false;
                            }
                            if ("DTS:PropertyExpression".equalsIgnoreCase(conManObjectDataNodeList.item((k)).getNodeName())) {
                                if (("ConnectionString".equalsIgnoreCase(conManObjectDataNodeList.item((k)).getAttributes().getNamedItem("DTS:Name").getNodeValue())) && propertExpressionFlag) {
                                    propertyExpressionConnectionString = conManObjectDataNodeList.item((k)).getTextContent();

                                    if (StringUtils.isNotBlank(propertyExpressionConnectionString)) {
                                        propertyExpressionConnectionString = propertyExpressionConnectionString.replaceAll("\\s*\\+\\s*\\@", "@");
                                    }
                                }
                            }

                            if (("DTS:ObjectData".equalsIgnoreCase(conManObjectDataNodeList.item((k)).getNodeName()))) {

                                NodeList conManInObjectDataNodeList = conManObjectDataNodeList.item(k).getChildNodes();
                                for (int l = 0; l < conManInObjectDataNodeList.getLength(); l++) {
                                    if ("DTS:ConnectionManager".equalsIgnoreCase(conManInObjectDataNodeList.item((l)).getNodeName())) {
                                        try {
                                            String connectionString = "";
                                            if (conManInObjectDataNodeList.item((l)).getAttributes().getNamedItem("DTS:ConnectionString") != null) {
                                                connectionString = conManInObjectDataNodeList.item((l)).getAttributes().getNamedItem("DTS:ConnectionString").getTextContent();
                                            } else {
                                                NodeList conManNodeListForPropExp = conManInObjectDataNodeList.item((l)).getParentNode().getParentNode().getChildNodes();
                                                for (int m = 0; m < conManNodeListForPropExp.getLength(); m++) {
                                                    if ("DTS:PropertyExpression".equals(conManNodeListForPropExp.item(m).getNodeName().trim())) {
                                                        connectionString = conManNodeListForPropExp.item(m).getTextContent();
                                                        if (StringUtils.isNotBlank(connectionString)) {
                                                            connectionString = connectionString.replaceAll("\\s*\\+\\s*\\@", "@");
                                                        }

                                                        break;
                                                    }
                                                }
                                            }
                                            boolean flag = true;
                                            try {
                                                String dsnName = ConnectionStringUtil.getDataSourceOrDSNFromConnectionString(connectionString);
                                                if (connectionPropertiesHashMap.get(dsnName) != null) {
                                                    connectionString = connectionPropertiesHashMap.get(dsnName);
                                                    connectionInfo = connectionString;
                                                    flag = false;
                                                } else if (connectionPropertiesHashMap.get(connectionName) != null) {
                                                    connectionString = connectionPropertiesHashMap.get(connectionName);
                                                    connectionInfo = connectionString;
                                                    flag = false;
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            if (flag) {

                                                connectionInfo = ConnectionStringUtil.getDatabaseAndServerNameFromConnectionString(connectionString);
                                            }
                                            if (StringUtils.isNotBlank(connectionInfo) && StringUtils.isBlank(propertyExpressionConnectionString)) {
                                                connections.put(connectionRefId, connectionInfo);
                                                connections.put(DTSId, connectionInfo);
                                                connections.put(connectionName, connectionInfo);
                                            } else if (StringUtils.isNotBlank(connectionInfo) && StringUtils.isNotBlank(propertyExpressionConnectionString)) {
                                                String defaultAndPropertyExpressionConnectioString = propertyExpressionConnectionString + Delimiter.connectionStringDelimiter + connectionInfo;
                                                connections.put(connectionRefId, defaultAndPropertyExpressionConnectioString);
                                                connections.put(DTSId, defaultAndPropertyExpressionConnectioString);
                                                connections.put(connectionName, defaultAndPropertyExpressionConnectioString);
                                            }

                                            connectionInfo = "";
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    if ("ODataConnectionManager".equalsIgnoreCase(conManInObjectDataNodeList.item((l)).getNodeName())) {
                                        String url = "";
                                        if (conManInObjectDataNodeList.item((l)).getAttributes().getNamedItem("Url") != null) {
                                            url = conManInObjectDataNodeList.item((l)).getAttributes().getNamedItem("Url").getTextContent();
                                        }
                                        connections.put(connectionRefId, url);
                                        connections.put(DTSId, url);
                                        connections.put(connectionName, url);
                                    }
                                }
                            }
                        }
                    }
                }
            }

        } catch (XPathExpressionException ex) {
            ex.printStackTrace();
//            exceptionBuilder.append("\n" + ex + "\n");
        }
        return connections;
    }

    /**
     * this method will take executable tag as node list and return the disabled
     * components as set back to code
     *
     * @param allExecutablesNodeList
     * @return disabledComponentSet
     */
    public Set<String> getDeactiveComponentList(NodeList allExecutablesNodeList) {
        Set<String> disabledComponentSet = new HashSet<>();
        if (allExecutablesNodeList != null) {
            try {
                for (int i = 0; i < allExecutablesNodeList.getLength(); i++) {
                    Node rootparentNode = allExecutablesNodeList.item(i);
                    String executableRefId = rootparentNode.getAttributes().getNamedItem("DTS:refId").getTextContent();
                    if (rootparentNode.getAttributes().getNamedItem("DTS:Disabled") != null) {
                        String disableProperties = rootparentNode.getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                        if ("TRUE".equalsIgnoreCase(disableProperties)) {

                            disabledComponentSet.add(executableRefId);
                        }
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return disabledComponentSet;
    }

    /**
     * this method will return the for each loop container executableNode back
     * to the code if it is available in the XML
     *
     * @author :Dinesh Arasankala
     * @param fileExecutableNode
     * @return ForEachLoop Executable Node
     */
    public Node getTheForEachLoopExecutableNodeIfAvaliable(Node fileExecutableNode) {
        boolean forEachLoopFlag = true;

        while (forEachLoopFlag) {
            String creationName = "";
            if (fileExecutableNode != null) {
                try {
                    if (fileExecutableNode.getAttributes().getNamedItem("DTS:CreationName") != null) {
                        creationName = fileExecutableNode.getAttributes().getNamedItem("DTS:CreationName").getTextContent();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if ("STOCK:FOREACHLOOP".equalsIgnoreCase(creationName)) {

                    forEachLoopFlag = false;

                } else if ("Microsoft.Package".equalsIgnoreCase(creationName)) {
                    forEachLoopFlag = false;
                } else {
                    fileExecutableNode = fileExecutableNode.getParentNode();
                }
            } else {
                forEachLoopFlag = false;
            }
        }

        return fileExecutableNode;
    }

    /**
     * this method will return the fileName that is available in the for each
     * loop container
     *
     * @author : Dinesh Arasankala
     * @param fileExecutableNode
     * @return fileName
     */
    public String getFileNameFromTheForEachLoopContainer(Node fileExecutableNode) {
        String filePath = "";
        String userDirectoryName = "";
        try {

            NodeList childNodesList = fileExecutableNode.getChildNodes();
            for (int j = 0; j < childNodesList.getLength(); j++) {

                if ("DTS:ForEachEnumerator".equalsIgnoreCase(childNodesList.item(j).getNodeName())) {
                    NodeList forEachEnumeratorList = childNodesList.item(j).getChildNodes();
                    userDirectoryName = "";
                    for (int k = 0; k < forEachEnumeratorList.getLength(); k++) {
                        if ("DTS:ObjectData".equalsIgnoreCase(forEachEnumeratorList.item(k).getNodeName())) {
                            NodeList objectDataList = forEachEnumeratorList.item(k).getChildNodes();
                            for (int l = 0; l < objectDataList.getLength(); l++) {
                                if ("ForEachFileEnumeratorProperties".equals(objectDataList.item(l).getNodeName())) {
                                    NodeList forEachFileEnumeratorPropertiesList = objectDataList.item(l).getChildNodes();
                                    String folderName = "";
                                    String fileName = "";
                                    String fileNameRetrievalType = "";
                                    for (int m = 0; m < forEachFileEnumeratorPropertiesList.getLength(); m++) {
                                        if ("FEFEProperty".equals(forEachFileEnumeratorPropertiesList.item(m).getNodeName())) {

                                            try {

                                                Node folderNode = forEachFileEnumeratorPropertiesList.item(m).getAttributes().getNamedItem("Folder");
                                                Node fileNameNode = forEachFileEnumeratorPropertiesList.item(m).getAttributes().getNamedItem("FileSpec");
                                                Node fileNameRetrievalTypeNode = forEachFileEnumeratorPropertiesList.item(m).getAttributes().getNamedItem("FileNameRetrievalType");

                                                if (folderNode != null) {
                                                    folderName = folderNode.getNodeValue();
                                                } else if (fileNameNode != null) {
                                                    fileName = fileNameNode.getNodeValue();
                                                } else if (fileNameRetrievalTypeNode != null) {
                                                    fileNameRetrievalType = fileNameRetrievalTypeNode.getNodeValue();
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    }

                                    filePath = fileName;

                                }
                            }
                        }
                    }

                } else if ("DTS:ForEachVariableMappings".equalsIgnoreCase(childNodesList.item(j).getNodeName())) {
                    NodeList forEachVariableMappingsList = childNodesList.item(j).getChildNodes();
                    for (int i = 0; i < forEachVariableMappingsList.getLength(); i++) {
                        if ("DTS:ForEachVariableMapping".equals(forEachVariableMappingsList.item(i).getNodeName())) {
                            Node VariableNameNode = forEachVariableMappingsList.item(i).getAttributes().getNamedItem("DTS:VariableName");
                            if (VariableNameNode != null) {
                                userDirectoryName = VariableNameNode.getNodeValue();
                            }
                        }

                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();

        }
        return userDirectoryName + Delimiter.delimiter + filePath;
    }

    /**
     * this method will prepare the DataFlow components level lineage
     *
     * @author : Sadar
     * @param componentConnectionNodeList
     * @return ComponentsLevel Lineage
     */
    public Map<String, String> prepareComponentsLineage(NodeList componentConnectionNodeList) {

        Map<String, String> componentConnections = new LinkedHashMap<>();
        for (int i = 0; i < componentConnectionNodeList.getLength(); i++) {
            if (componentConnectionNodeList.item(i).getNodeName().equals("#text")) {
                continue;
            }
            String startId = componentConnectionNodeList.item(i).getAttributes().getNamedItem("startId").getTextContent();
            String endId = componentConnectionNodeList.item(i).getAttributes().getNamedItem("endId").getTextContent();
            componentConnections.put(endId, startId);
        }
        return componentConnections;
    }

}
