/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2008;

import com.erwin.cfx.connectors.ssis.generic.service.ssis2014.Parser2014XMLFile;
import com.erwin.cfx.connectors.ssis.generic.util.ConnectionStringUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 */
public class Parser2008XMLFile {

    HashMap<String, String> connections = new HashMap<>();
    Set<String> eventHandlerExecutables = new HashSet<>();
    String variablepath = "//Executable/Variable";

    /**
     * creates a variable map extracting expression or variable value for the
     * respective object name from variable tag in XML
     *
     * @param xmlDocument
     * @return variables map
     */
    public Map<String, String> prepareUserVariablesMap(Document xmlDocument) {
        Map<String, String> userVariableMap = null;
        try {
            userVariableMap = new LinkedHashMap<>();
            String variableName = "";
            String variableValue = "";
            String expressionValue = "";

            NodeList controlFlowLevelNodeList = returnNodeList(xmlDocument, variablepath);
            for (int i = 0; i < controlFlowLevelNodeList.getLength(); i++) {                                //+1
                NodeList rootChildNodeList = controlFlowLevelNodeList.item(i).getChildNodes();
                variableName = "";
                variableValue = "";
                expressionValue = "";
                for (int j = 0; j < rootChildNodeList.getLength(); j++) {                                   //+2    
                    Node rootChildNode = rootChildNodeList.item(j);
                    if (Constants.propertyLiteral.equalsIgnoreCase(rootChildNode.getNodeName())) {          //+3
                        String objectName = "ObjectName";
                        String expression = "Expression";
                        if (StringUtils.isBlank(variableName)) {
                            variableName = getTextContentFromXml(rootChildNode, objectName);
                        }
                        if (StringUtils.isBlank(expressionValue)) {
                            expressionValue = getTextContentFromXml(rootChildNode, expression);
                        }
                    }
                    if ("DTS:VariableValue".equalsIgnoreCase(rootChildNode.getNodeName())) {                //+3
                        variableValue = rootChildNodeList.item((j)).getTextContent();
                    }
                    if (StringUtils.isNotBlank(variableName) && (StringUtils.isNotBlank(expressionValue) || StringUtils.isNotBlank(variableValue))) {                                             //+3
                        String value = StringUtils.isNotBlank(expressionValue) ? expressionValue : variableValue; //+1
                        userVariableMap.put(variableName, value);
                        variableName = "";
                        expressionValue = "";
                        variableValue = "";
                    }
                }
            }
        } catch (Exception e) {                                                                         //+1
            e.printStackTrace();
        }
        return userVariableMap;
    }

    /**
     * creates a Hash Map containing key as executable name and value as query
     * with database and server name
     *
     * @param executableNode
     * @param objectName
     * @return objectNameAndQueryMap hashmap
     */
    public void prepareObjectNameAndSqlQueryForExtendedProperties(Node executableNode, String objectName) {

        String query = "";
        String sqlConnection = "";
        Parser2014XMLFile parser2014XMLFile = new Parser2014XMLFile();
        try {
            NodeList executableChildNodesList = executableNode.getChildNodes();
            for (int i = 0; i < executableChildNodesList.getLength(); i++) {
                if (executableChildNodesList.item(i).getNodeName().equals(Constants.objectDataLiteral)) {
                    Node objectNode = executableChildNodesList.item(i);
                    NodeList objectDataChildNodesList = objectNode.getChildNodes();
                    for (int j = 0; j < objectDataChildNodesList.getLength(); j++) {
                        Node objectDataChildNode = objectDataChildNodesList.item(j);
                        if (objectDataChildNode.getNodeName().equalsIgnoreCase(Constants.sqlTaskDataLiteral)) {
                            try {
                                query = objectDataChildNodesList.item(j).getAttributes().getNamedItem("SQLTask:SqlStatementSource").getTextContent();
                                sqlConnection = objectDataChildNodesList.item(j).getAttributes().getNamedItem("SQLTask:Connection").getTextContent();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            String serverName = "";
                            String databaseName = "";
                            try {
                                if (StringUtils.isNotBlank(sqlConnection)) {
                                    String connectionInfo = connections.get(sqlConnection);
                                    connectionInfo = parser2014XMLFile.updatedConnectionInfo(connectionInfo);
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
//                                    if (connectionInfo.split(Delimiter.delimiter).length >= 2) {
//                                        databaseName = connectionInfo.split(Delimiter.delimiter)[1];
//                                        serverName = connectionInfo.split(Delimiter.delimiter)[0];
//                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            String key = objectName + "$queryName";
                            SSIS2008.objectNameAndQueryMap.put(key, query + Delimiter.delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * creates a connections map extracting ConnectionString for the respective
     * executable from connection manager tag in XML
     *
     * @param xmlDocument
     * @param xPath
     * @return connections
     */
    public HashMap<String, String> prepareConnectionsMap(Document xmlDocument) {
        String connectionManagerPath = "//Executable/ConnectionManager";
        try {
            NodeList connectionManagerNodeList = returnNodeList(xmlDocument, connectionManagerPath);
            for (int i = 0; i < connectionManagerNodeList.getLength(); i++) {
                Node rootparentNode = connectionManagerNodeList.item(i).getParentNode();
                if (rootparentNode.getParentNode() == null) {
                    continue;
                }
                NodeList rootChildNodeList = connectionManagerNodeList.item(i).getChildNodes();
                String connectionRefId = "";
                String creationName = "";
                String connectionName = "";
                String DTSId = "";
                String connectionString = "";
                String propertyExpressionConnectionString = "";
                for (int j = 0; j < rootChildNodeList.getLength(); j++) {
                    Node rootChildNode = rootChildNodeList.item((j));
                    if (Constants.propertyLiteral.equalsIgnoreCase(rootChildNode.getNodeName())) {
                        try {
                            String objectName = "ObjectName";
                            String dtsId = "DTSID";
                            String refId = "refId";
                            String creationNamee = "CreationName";

                            if (StringUtils.isBlank(connectionName)) {
                                connectionName = getTextContentFromXml(rootChildNode, objectName);
                            }
                            if (StringUtils.isBlank(DTSId)) {
                                DTSId = getTextContentFromXml(rootChildNode, dtsId);
                            }
                            if (StringUtils.isBlank(connectionRefId)) {
                                connectionRefId = getTextContentFromXml(rootChildNode, refId);
                            }
                            if (StringUtils.isBlank(creationName)) {
                                creationName = getTextContentFromXml(rootChildNode, creationNamee);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    try {
                        if ("DTS:PropertyExpression".equalsIgnoreCase(rootChildNode.getNodeName())) {
                            if (StringUtils.isBlank(propertyExpressionConnectionString)) {
                                propertyExpressionConnectionString = getTextContentFromXml(rootChildNode, "ConnectionString");
                                if (StringUtils.isNotBlank(propertyExpressionConnectionString)) {
                                    propertyExpressionConnectionString = propertyExpressionConnectionString.replaceAll("\\s*\\+\\s*\\@", "@");
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (Constants.objectDataLiteral.equalsIgnoreCase(rootChildNodeList.item((j)).getNodeName())) {

                        NodeList connectionManagerChildList = rootChildNodeList.item((j)).getChildNodes();

                        for (int pcl = 0; pcl < connectionManagerChildList.getLength(); pcl++) {
                            try {
                                if (connectionManagerChildList.item((pcl)).getNodeName().equalsIgnoreCase("DTS:ConnectionManager")) {

                                    NodeList propertyNodeList = connectionManagerChildList.item((pcl)).getChildNodes();

                                    for (int k = 0; k < propertyNodeList.getLength(); k++) {
                                        if (propertyNodeList.item((k)).getNodeName().equalsIgnoreCase(Constants.propertyLiteral)) {
                                            if (StringUtils.isBlank(connectionString)) {
                                                connectionString = getTextContentFromXml(propertyNodeList.item((k)), "ConnectionString");
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    if (!StringUtils.isBlank(connectionString)) {
                        String value = "";

                        value = ConnectionStringUtil.getDatabaseAndServerNameFromConnectionString(connectionString);
                        if (StringUtils.isNotBlank(propertyExpressionConnectionString) && StringUtils.isNotBlank(value)) {
                            value = propertyExpressionConnectionString + Delimiter.connectionStringDelimiter + value;
                        }
                        connections.put(connectionString, value);
                        connections.put(connectionRefId, value);
                        connections.put(DTSId, value);
                        connections.put(connectionName, value);

                        connectionRefId = "";
                        creationName = "";
                        connectionName = "";
                        DTSId = "";
                        connectionString = "";
                        propertyExpressionConnectionString = "";
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return connections;
    }

    /**
     * creates a set containing all the disabled executables by extracting
     * information from the xml
     *
     * @param allExecutablesNodeList
     * @return disabledComponentSet
     */
    public HashSet<String> getDeactiveComponentList(NodeList allExecutablesNodeList) {
        HashSet<String> disabledComponentSet = new HashSet<>();
        String value = "";
        if (allExecutablesNodeList != null) {
            try {
                for (int i = 0; i < allExecutablesNodeList.getLength(); i++) {
                    Node rootparentNode = allExecutablesNodeList.item(i);
                    if (!rootparentNode.getNodeName().equals("#text")) {
                        if ("Disabled".equalsIgnoreCase(rootparentNode.getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                            value = rootparentNode.getTextContent();
                        }
                        if (value.equalsIgnoreCase("1") || value.equalsIgnoreCase("-1")) {
                            if ("DTSID".equalsIgnoreCase(rootparentNode.getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                                String executableRefId = rootparentNode.getTextContent();
                                disabledComponentSet.add(executableRefId);
                            }
                        }
                    }
                }
                value = "";
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return disabledComponentSet;
    }

    /**
     * returns text content of a node by taking node and attribute name as input
     * parameters
     *
     * @param rootChildNode
     * @param attributeName
     * @return variableName
     */
    public String getTextContentFromXml(Node rootChildNode, String attributeName) {

        String variableName = "";
        try {
            Node rootChildNodeItem = null;
            try {
                if (rootChildNode.getAttributes() != null) {
                    rootChildNodeItem = rootChildNode.getAttributes().getNamedItem(Constants.nameLiteral);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (rootChildNodeItem != null && rootChildNodeItem.getTextContent().equalsIgnoreCase(attributeName)) {
                variableName = rootChildNode.getTextContent();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return variableName;
    }

    /**
     * Assigns values to a global map containing details of all the executables
     * present in event handler tag from XML
     *
     * @param eventHandlerNodes
     */
    public void eventHandler(NodeList eventHandlerNodes) {

        try {
            for (int i = 0; i < eventHandlerNodes.getLength(); i++) {
                if (Constants.textLiteral.equalsIgnoreCase(eventHandlerNodes.item(i).getNodeName())) {
                    continue;
                }
                if (Constants.executableLiteral.equalsIgnoreCase(eventHandlerNodes.item(i).getNodeName())) {
                    Node eventHandlerChildNode = eventHandlerNodes.item(i);
                    NodeList childNodes = eventHandlerChildNode.getChildNodes();
                    for (int j = 0; j < childNodes.getLength(); j++) {
                        Node childExecutableNode = childNodes.item(j);
                        try {
                            if (Constants.textLiteral.equalsIgnoreCase(childExecutableNode.getNodeName())) {
                                continue;
                            }
                            if (Constants.executableLiteral.equalsIgnoreCase(childExecutableNode.getNodeName())) {
                                eventHandler(childNodes);
                            } else {
                                String value = "";
                                if (StringUtils.isBlank(value)) {
                                    value = getTextContentFromXml(childExecutableNode, Constants.dtsIdLiteral);
                                }
                                eventHandlerExecutables.add(value);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * returns executable name and its DTSId taking the node value as input
     *
     * @param rootNode
     * @return
     */
    public String getObjectName(Node rootNode) {
        String objectName = "";
        String dummy = "";
        String objectId = "";
        try {
            NodeList rootNodeList = rootNode.getChildNodes();
            for (int i = 0; i < rootNodeList.getLength(); i++) {
                if (!rootNodeList.item(i).getNodeName().equals(Constants.textLiteral)) {

                    if (rootNodeList.item(i).getNodeName().equals(Constants.propertyLiteral)) {
                        try {
                            if (rootNodeList.item(i).getAttributes() == null || rootNodeList.item(i).getAttributes().getNamedItem("DTS:Name") == null) {
                                continue;
                            }
                            if ("ObjectName".equalsIgnoreCase(rootNodeList.item(i).getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                                objectName = rootNodeList.item(i).getTextContent();

                            } else if ("DTSID".equalsIgnoreCase(rootNodeList.item(i).getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                                objectId = rootNodeList.item(i).getTextContent();
                                break;
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return objectName + Delimiter.delimiter + objectId + Delimiter.delimiter + dummy;
    }

    /**
     * creates a hash map containing details of all the executables(key) and
     * queries(value) present in XML file
     *
     * @param siblingsList
     * @return extendedProperties
     */
    public HashMap<String, String> getExtendedProperties(NodeList siblingsList) {
        HashMap<String, String> extendedProperties = new HashMap<>();
        String objectName = "";
        for (int j = 0; j < siblingsList.getLength(); j++) {
            if ("DTS:Executables".equalsIgnoreCase(siblingsList.item(j).getNodeName())) {
                NodeList executableList = siblingsList.item(j).getChildNodes();
                for (int k = 0; k < executableList.getLength(); k++) {

                    if (Constants.executableLiteral.equalsIgnoreCase(executableList.item(k).getNodeName())) {
                        objectName = executableList.item(k).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                        String creationName = executableList.item(k).getAttributes().getNamedItem("DTS:CreationName").getTextContent();
                        if ("Microsoft.Pipeline".equalsIgnoreCase(creationName) || Constants.textLiteral.equalsIgnoreCase(executableList.item(k).getNodeName())) {
                            continue;
                        } else {
                            NodeList objectDataList = executableList.item(k).getChildNodes();
                            for (int i = 0; i < objectDataList.getLength(); i++) {
                                if (Constants.objectDataLiteral.equalsIgnoreCase(objectDataList.item(i).getNodeName())) {
                                    NodeList sQLTaskList = objectDataList.item(i).getChildNodes();
                                    if (objectDataList.item(i) == null) {
                                        continue;
                                    }
                                    for (int l = 0; l < sQLTaskList.getLength(); l++) {
                                        if (!Constants.sqlTaskDataLiteral.equalsIgnoreCase(sQLTaskList.item(l).getNodeName()) || sQLTaskList.item(l).getAttributes().getNamedItem(Constants.sqlStatementLiteral) == null) {
                                            continue;
                                        }
                                        String query = sQLTaskList.item(l).getAttributes().getNamedItem(Constants.sqlStatementLiteral).getTextContent();
                                        String sqlConnection = sQLTaskList.item(l).getAttributes().getNamedItem(Constants.sqlConnectionLiteral).getTextContent();
                                        String connectionInfo = connections.get(sqlConnection);
                                        String serverName = "";
                                        String databaseName = "";
                                        connectionInfo = new Parser2014XMLFile().updatedConnectionInfo(connectionInfo);
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
//                                        extendedProperties.put(objectName + "$queryName", query + "##" + sqlConnection);
                                        String key = objectName + "$queryName";
                                        extendedProperties.put(key, query + Delimiter.delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName);
                                    }
                                } else if ("DTS:Executables".equalsIgnoreCase(objectDataList.item(i).getNodeName())) {
                                    NodeList childExecutables = objectDataList.item(i).getChildNodes();
                                    for (int l = 0; l < childExecutables.getLength(); l++) {
                                        if (Constants.executableLiteral.equalsIgnoreCase(childExecutables.item(l).getNodeName())) {
                                            objectName = childExecutables.item(l).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                                            String childExeCreationName = childExecutables.item(l).getAttributes().getNamedItem("DTS:CreationName").getTextContent();

                                            if ("Microsoft.Pipeline".equalsIgnoreCase(childExeCreationName) || Constants.textLiteral.equalsIgnoreCase(childExecutables.item(l).getNodeName())) {
                                                continue;
                                            } else {
                                                NodeList childObjectDataList = childExecutables.item(l).getChildNodes();
                                                for (int m = 0; m < childObjectDataList.getLength(); m++) {
                                                    if (Constants.objectDataLiteral.equalsIgnoreCase(childObjectDataList.item(m).getNodeName())) {
                                                        NodeList sQLTaskList = childObjectDataList.item(m).getChildNodes();
                                                        if (childObjectDataList.item(m) == null) {
                                                            continue;
                                                        }
                                                        for (int n = 0; n < sQLTaskList.getLength(); n++) {
                                                            if (!Constants.sqlTaskDataLiteral.equalsIgnoreCase(sQLTaskList.item(n).getNodeName()) || sQLTaskList.item(n).getAttributes().getNamedItem(Constants.sqlStatementLiteral) == null) {
                                                                continue;
                                                            }
                                                            String queryconnection = sQLTaskList.item(n).getAttributes().getNamedItem(Constants.sqlConnectionLiteral).getTextContent();
                                                            String query = sQLTaskList.item(n).getAttributes().getNamedItem(Constants.sqlStatementLiteral).getTextContent();

                                                            String connectionInfo = connections.get(queryconnection);
                                                            String serverName = "";
                                                            String databaseName = "";
                                                            connectionInfo = new Parser2014XMLFile().updatedConnectionInfo(connectionInfo);
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
                                                            String key = objectName + "$queryName";
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
        return extendedProperties;
    }

    /**
     * returns node list for respective variable path and xml document
     *
     * @param xmlDocument
     * @param variablepath
     * @return nodeList
     */
    public NodeList returnNodeList(Document xmlDocument, String variablepath) {
        XPath xPath = XPathFactory.newInstance().newXPath();
        NodeList nodeList = null;
        try {
            nodeList = (NodeList) xPath.compile(variablepath).evaluate(xmlDocument, XPathConstants.NODESET);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return nodeList;
    }

    /**
     * this method returns data flow name,DTSId for a given executable node
     *
     * @param dataFlowNode
     * @return
     */
    public static String getDataFlowName(Node dataFlowNode) {

        String dataFlowName = "";
        String RefID = "";
        String DTSID = "";
        String dummyData = "tempData";

        NodeList dataFlowChildNodes = dataFlowNode.getChildNodes();

        for (int i = 0; i < dataFlowChildNodes.getLength(); i++) {

            if (dataFlowChildNodes.item(i).getNodeName().equals(Constants.propertyLiteral)) {
                Node propertyNode = dataFlowChildNodes.item(i);
                if (propertyNode.getAttributes().getNamedItem("DTS:Name").getTextContent().equalsIgnoreCase("ObjectName")) {
                    dataFlowName = propertyNode.getTextContent();
                }
                try {
                    if (propertyNode.getAttributes().getNamedItem("DTS:Name").getTextContent().equalsIgnoreCase("refId")) {
                        RefID = propertyNode.getTextContent();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    if (propertyNode.getAttributes().getNamedItem("DTS:Name").getTextContent().equalsIgnoreCase("DTSID")) {
                        DTSID = propertyNode.getTextContent();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }

        return dataFlowName + Delimiter.delimiter + RefID + Delimiter.delimiter + DTSID + Delimiter.delimiter + dummyData;
    }

    /**
     * this method prepares a hash map containing details of components start Id
     * and end Id
     *
     * @param componentConnectionNodeList
     * @return
     */
    public static LinkedHashMap<String, String> prepareComponentConnections(NodeList componentConnectionNodeList) {

        long startTime = System.currentTimeMillis();
        LinkedHashMap<String, String> componentConnections = new LinkedHashMap<String, String>();
        for (int i = 0; i < componentConnectionNodeList.getLength(); i++) {
            if (componentConnectionNodeList.item(i).getNodeName().equals(Constants.textLiteral)) {
                continue;
            }
            String startId = componentConnectionNodeList.item(i).getAttributes().getNamedItem("startId").getTextContent();
            String endId = componentConnectionNodeList.item(i).getAttributes().getNamedItem("endId").getTextContent();
            componentConnections.put(endId, startId);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("time taken----in prepareComponentConnections method::" + (endTime - startTime));
        return componentConnections;
    }
}
