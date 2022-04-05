/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2014;

import com.erwin.cfx.connectors.ssis.generic.beans.DataFlowBean;
import com.erwin.cfx.connectors.ssis.generic.controller.SSISController;
import com.erwin.cfx.connectors.ssis.generic.util.ConnectionStringUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.DataFlowComponentUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.SqlParserUtil;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 19-08-2021
 */
public class PrepareDataFlowLineage2014 {

    public static Map<String, String> storeProcTableAndItsColumnsMap = new HashMap<>();
    public static Map<String, String> actualQueryWithModifiedQuery = new HashMap<>();
    public static Map<String, String> mergeComponentMapForExtendedProperties = new HashMap<>();
    static Map<String, String> componentNamerefIds = new HashMap<>();
    static Map<String, String> pathInputsOutputsMap = new LinkedHashMap<>();
    static Map<String, String> componentClassIds = new LinkedHashMap<>();

    static Map<String, List<Map<String, String>>> dataflowLineageMap = new HashMap<>();

    public PrepareDataFlowLineage2014() {
    }

    /**
     * this method will iterate all the Data flows in a single DTSX package and
     * prepare the data flows lineage Map
     *
     * @param dataflowComponentNodeList
     * @param dataFlowBean
     * @return dataflowLineageMap
     */
    public static Map<String, List<Map<String, String>>> iterateDataFlowComponentsData(NodeList dataflowComponentNodeList, DataFlowBean dataFlowBean) {
        ArrayList<String> executableNameList = new ArrayList<>();
        Parser2014XMLFile parser2014XMLFile = new Parser2014XMLFile();
        dataflowLineageMap = new HashMap<>();
        actualQueryWithModifiedQuery = new HashMap<>();

        Set<String> disabledComponentSet = null;
        if (dataFlowBean != null) {
            disabledComponentSet = dataFlowBean.getSsisInputParameterBean().getDisabledComponentSet();
        } else {
            dataFlowBean = new DataFlowBean();
        }
        for (int i = 0; i < dataflowComponentNodeList.getLength(); i++) {
            Node executableNode = dataflowComponentNodeList.item(i).getParentNode().getParentNode();
            Node fileExecutableNode = dataflowComponentNodeList.item(i).getParentNode().getParentNode().getParentNode().getParentNode();
            String executableName = executableNode.getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
            String executableReferenceId = executableNode.getAttributes().getNamedItem("DTS:refId").getTextContent();
            String actualExecutableName = executableName;
            String disbaledName = "";

            dataFlowBean.setActualDataFlowName(actualExecutableName);

            if (executableNameList.contains(executableName)) {
                executableName = executableReferenceId.replace("Package\\", "").replace("\\", "_");
            }
            executableName = executableName.replaceAll("[^a-zA-Z0-9 \\p{L}_-]", "_");
            if (executableNode.getAttributes().getNamedItem("DTS:Disabled") != null) {
                disbaledName = executableNode.getAttributes().getNamedItem("DTS:Disabled").getTextContent();
            }
            addTheDisbledDataflowtToTheMap(executableName, disbaledName);
            dataFlowBean.setDataflowName(executableName);
            executableNameList.add(executableName);
            String executableSubStringRefId = executableReferenceId.substring(0, executableReferenceId.lastIndexOf("\\"));
            if (disabledComponentSet != null && (disabledComponentSet.contains(executableReferenceId) || disabledComponentSet.contains(executableSubStringRefId))) {
                continue;
            }
            try {
                fileExecutableNode = parser2014XMLFile.getTheForEachLoopExecutableNodeIfAvaliable(fileExecutableNode);
            } catch (Exception e) {
                e.printStackTrace();
            }

            String forEachLoopFileName = "";
            if (fileExecutableNode != null) {
                forEachLoopFileName = parser2014XMLFile.getFileNameFromTheForEachLoopContainer(fileExecutableNode);
            }
            DataFlowSupport.getFileNameDetailsFromForEachLoop(forEachLoopFileName, dataFlowBean);
            NodeList pipeLinesChilds = dataflowComponentNodeList.item(i).getChildNodes();
            Map<String, String> componentConnections = new LinkedHashMap<>();
            Map<String, String> queryMap = null;

            for (int j = 0; j < pipeLinesChilds.getLength(); j++) {
                if (pipeLinesChilds.item(j).getNodeName().equals("paths")) {
                    Node pathsNode = pipeLinesChilds.item(j);
                    componentConnections = parser2014XMLFile.prepareComponentsLineage(pathsNode.getChildNodes());
                }
            }
            dataFlowBean.setComponentConnections(componentConnections);
            for (int j = 0; j < pipeLinesChilds.getLength(); j++) {
                if (pipeLinesChilds.item(j).getNodeName().equals("components")) {
                    Node componentsNode = pipeLinesChilds.item(j);
                    queryMap = new HashMap<>();
                    try {
                        prepareComponentsAndItsColumnDetails(componentsNode.getChildNodes(), queryMap, dataFlowBean);
                    } catch (Exception e) {
                        e.printStackTrace();

                    }

                }
            }
            dataflowLineageMap = BuildDataFlowLineage.buildMappingLineage(componentConnections, executableName, queryMap, actualExecutableName, dataflowLineageMap);
        }
        return dataflowLineageMap;
    }

    /**
     * this method will prepare the data flow and its components data
     *
     * @param componentList
     * @param queryMap
     * @param dataFlowBean
     */
    public static void prepareComponentsAndItsColumnDetails(NodeList componentList, Map<String, String> queryMap, DataFlowBean dataFlowBean) {
        String dataFlowName = "";

        Map<String, String> componentClassIdsMap = dataFlowBean.getAcpInputParameterBean().getComponentClassIdsMap();
        String executableName = dataFlowBean.getDataflowName();
        if ("Import Data YTD".equalsIgnoreCase(executableName)) {
            int temp = 0;
        }
        IterateInputAndOutPutColumns2014.componentColumnIdsForInput = new LinkedHashMap<>();
        IterateInputAndOutPutColumns2014.componentColumnIdsForOutput = new LinkedHashMap<>();

        Map<String, String> connections = dataFlowBean.getSsisInputParameterBean().getConnectionsMap();
        Map<String, String> packageLevelMap = dataFlowBean.getSsisInputParameterBean().getUserReferenceVariablesMap();
        Map<String, String> componentConnections = dataFlowBean.getComponentConnections();

        for (int j = 0; j < componentList.getLength(); j++) {
            if (componentList.item(j).getNodeName().equals("#text")) {
                continue;
            }
            String componentRefId = componentList.item(j).getAttributes().getNamedItem("refId").getTextContent();
            String componentName = componentList.item(j).getAttributes().getNamedItem("name").getTextContent();
            String actualComponentName = componentName;
            String componentClassId = componentList.item(j).getAttributes().getNamedItem("componentClassID").getTextContent();
            if (componentClassIdsMap.get(componentClassId) != null) {
                componentClassId = componentClassIdsMap.get(componentClassId);
            }
            String componentDescription = "";
            try {
                componentDescription = componentList.item(j).getAttributes().getNamedItem("description").getTextContent();
            } catch (Exception e) {

            }

            String contactInfo = "";
            componentClassIds.put(componentRefId, componentClassId);
            if (componentDescription.toUpperCase().contains("DWS GROUP: MERGE COMPONENT")) {
                contactInfo = "DWS GROUP: MERGE COMPONENT";
            } else if (componentList.item(j).getAttributes().getNamedItem("contactInfo") != null) {
                contactInfo = componentList.item(j).getAttributes().getNamedItem("contactInfo").getTextContent();
            } else {
                contactInfo = componentClassId;
            }

            String query = "";

            String connectionRefId = "";
            String tableName = "";
            String keyTableName = "";

            String xmlDataVariable = "";
            String xmlData = "";

            String fileDataBaseType = "";
            String storeProcName = "";
            String fileExtension = "";
            query = "";
            String accessmode = "";
            boolean isFileComponentFlag = false;
            String joinTypeForMergeComponent = "";
            String joinTypeNameForMergeComponent = "";
            String dfUtilDelimiter = Delimiter.dataFlowUtilDelimiter;
            String rawSourceFileName = "";
            String rawSourceVariableFileName = "";
            String collectionNameForODataSource = "";
            String componentDecision = "";
            String databaseName = "";
            String serverName = "";
            boolean odataFlag = false;
            String odataSourceConnectionUrl = "";
            componentDecision = componentsTypeDecision(componentClassId, contactInfo);
            if (componentDecision.equalsIgnoreCase("Source") || componentDecision.equalsIgnoreCase("Target") || componentDecision.equalsIgnoreCase("Lookup") || componentDecision.equalsIgnoreCase("Multicast") || "Microsoft.SCD".equalsIgnoreCase(componentClassId) || "Microsoft.MergeJoin".equalsIgnoreCase(componentClassId)) {

                NodeList nodeListChildNodes = componentList.item(j).getChildNodes();
                for (int k = 0; k < nodeListChildNodes.getLength(); k++) {
                    if ("Microsoft.FlatFileSource".equalsIgnoreCase(componentClassId)) {
                        if ("connections".equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            NodeList property = nodeListChildNodes.item(k).getChildNodes();
                            for (int l = 0; l < property.getLength(); l++) {
                                if ("connection".equalsIgnoreCase(property.item(l).getNodeName())) {
                                    connectionRefId = property.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();
                                    String tableCombinationData = DataFlowComponentUtil.getTableRelatedInfoFromConnectionString(connectionRefId, connections, dataFlowBean);

                                    try {
                                        tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                        fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                        fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];
                                        isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }

                                }
                            }
                        }
                    } else if ("Microsoft.FlatFileDestination".equalsIgnoreCase(componentClassId)) {
                        if ("connections".equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            NodeList property = nodeListChildNodes.item(k).getChildNodes();
                            for (int l = 0; l < property.getLength(); l++) {
                                if ("connection".equalsIgnoreCase(property.item(l).getNodeName())) {
                                    connectionRefId = property.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();
                                    String tableCombinationData = DataFlowComponentUtil.getTableRelatedInfoFromConnectionString(connectionRefId, connections, dataFlowBean);

                                    try {
                                        tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                        fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                        fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];
                                        isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }

                    } else if ("properties".equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                        NodeList property = nodeListChildNodes.item(k).getChildNodes();

                        String tableOrQuery = DataFlowSupport.returnTableNameOrQueryFromPropertiesFile(property, dataFlowBean, componentDecision, componentClassId);
                        if (StringUtils.isNotBlank(tableOrQuery) && tableOrQuery.endsWith(Constants.queryConstant)) {
                            query = tableOrQuery.replace(Constants.queryConstant, "");
                        } else if (StringUtils.isNotBlank(tableOrQuery) && tableOrQuery.endsWith(Constants.tableConstant)) {
                            tableName = tableOrQuery.replace(Constants.tableConstant, "");
                        } else {
                            tableName = "";
                            query = "";
                        }
                        try {
                            accessmode = dataFlowBean.getAccessMode();
                            rawSourceFileName = dataFlowBean.getRawSourceFileName();
                            rawSourceVariableFileName = dataFlowBean.getRawSourceVariableFileName();
                            collectionNameForODataSource = dataFlowBean.getCollectionNameForODataSource();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if ("0".equalsIgnoreCase(accessmode) && ("Microsoft.RawSource".equalsIgnoreCase(componentClassId) || "Microsoft.RawDestination".equalsIgnoreCase(componentClassId)) && StringUtils.isNotBlank(rawSourceFileName)) {
                            tableName = rawSourceFileName;
                        } else if ("1".equalsIgnoreCase(accessmode) && ("Microsoft.RawSource".equalsIgnoreCase(componentClassId) || "Microsoft.RawDestination".equalsIgnoreCase(componentClassId)) && StringUtils.isNotBlank(rawSourceVariableFileName)) {
                            tableName = rawSourceVariableFileName;
                        }
                        fileExtension = dataFlowBean.getFileExtension();
                        isFileComponentFlag = dataFlowBean.isIsFileComponent();
                        fileDataBaseType = dataFlowBean.getFileDataBaseType();
                        if ("Microsoft.RawSource".equalsIgnoreCase(componentClassId) || "Microsoft.RawDestination".equalsIgnoreCase(componentClassId)) {
                            tableName = DataFlowComponentUtil.returnTableNameFromFilePath(tableName);

                            try {
                                if (tableName.contains(Delimiter.fileExtensionDelimiter)) {
                                    fileExtension = tableName.split(Delimiter.fileExtensionDelimiter)[1];
                                    tableName = tableName.split(Delimiter.fileExtensionDelimiter)[0];
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            try {

                                fileDataBaseType = DataFlowComponentUtil.getTheFileDatabaseTypeBasedonTheFileExtension(fileExtension, dataFlowBean);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            if (!StringUtils.isBlank(fileDataBaseType) && !StringUtils.isBlank(tableName)) {
                                isFileComponentFlag = true;
                            }
                        }

                        if ("Microsoft.ManagedComponentHost".equalsIgnoreCase(componentClassId) && StringUtils.isNotBlank(collectionNameForODataSource)) {
                            tableName = collectionNameForODataSource + Delimiter.oDataSourceDelimiter;
                            isFileComponentFlag = true;
                            fileDataBaseType = "DSV";
                            fileExtension = ".txt";
                            odataFlag = true;
                        }

                    } else if ("Microsoft.ExcelSource".equalsIgnoreCase(componentClassId)) {
                        if ("connections".equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            NodeList property = nodeListChildNodes.item(k).getChildNodes();
                            for (int l = 0; l < property.getLength(); l++) {
                                if ("connection".equalsIgnoreCase(property.item(l).getNodeName())) {
                                    connectionRefId = property.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();

                                    String tableCombinationData = DataFlowComponentUtil.getTableRelatedInfoFromConnectionString(connectionRefId, connections, dataFlowBean);

                                    try {
                                        tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                        fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                        fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];
                                        isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }

                                }

                            }
                        }
                    } else if ("Microsoft.ExcelDestination".equalsIgnoreCase(componentClassId)) {
                        if ("connections".equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            NodeList property = nodeListChildNodes.item(k).getChildNodes();
                            for (int l = 0; l < property.getLength(); l++) {
                                if ("connection".equalsIgnoreCase(property.item(l).getNodeName())) {
                                    connectionRefId = property.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();
                                    String tableCombinationData = DataFlowComponentUtil.getTableRelatedInfoFromConnectionString(connectionRefId, connections, dataFlowBean);

                                    try {
                                        tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                        fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                        fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];
                                        isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }

                                }

                            }
                        }
                    } else if ("connections".equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                        NodeList property = nodeListChildNodes.item(k).getChildNodes();
                        for (int l = 0; l < property.getLength(); l++) {
                            if ("connection".equalsIgnoreCase(property.item(l).getNodeName())) {
                                connectionRefId = property.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();

                                String databaseAndServerName = getDatabaseAndServerNameFromConnectionString(connectionRefId, connections);

                                databaseName = databaseAndServerName.split(Delimiter.delimiter)[0];
                                serverName = databaseAndServerName.split(Delimiter.delimiter)[1];
                                if (odataFlag && StringUtils.isNotBlank(serverName)) {
                                    serverName = FilenameUtils.normalizeNoEndSeparator(serverName, true);

                                    try {
                                        String defaultOdataEnv = dataFlowBean.getAcpInputParameterBean().getoDataEnvironmentName();
                                        if (serverName.contains("/") && serverName.toLowerCase().contains(".svc")) {
                                            odataSourceConnectionUrl = serverName.substring(serverName.lastIndexOf("/") + 1).replace(".svc", "") + defaultOdataEnv;

                                        } else {
                                            odataSourceConnectionUrl = defaultOdataEnv.replace("_", "");
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                    }

                }

                if (StringUtils.isNotBlank(query) && query.toUpperCase().contains("EXECUTE ") || query.toUpperCase().contains("EXEC ")) {

                    String queryAndComponentName = "";
                    queryAndComponentName = SqlParserUtil.makeStorePrcedureNameAsComponentName(query, componentName);
                    if (queryAndComponentName.contains(Delimiter.delimiter)) {
                        int length = queryAndComponentName.split(Delimiter.delimiter).length;
                        if (length == 2) {
                            query = queryAndComponentName.split(Delimiter.delimiter)[0];
                            storeProcName = queryAndComponentName.split(Delimiter.delimiter)[1];
                            componentName = storeProcName;
                        } else if (length == 1) {
                            query = queryAndComponentName.split(Delimiter.delimiter)[0];
                        }
                    }
                }

                if ((accessmode.equalsIgnoreCase("2") || accessmode.equalsIgnoreCase("3")) && (componentClassId.equalsIgnoreCase("Microsoft.ExcelSource") || componentClassId.equalsIgnoreCase("Microsoft.ExcelDestination"))) {
                    tableName = "";
                    isFileComponentFlag = false;
                    fileDataBaseType = "";
                }
                xmlData = dataFlowBean.getXmlData();
                xmlDataVariable = dataFlowBean.getXmlDataVariable();
                if ("XSD".equalsIgnoreCase(fileDataBaseType) && !StringUtils.isBlank(accessmode) && (!StringUtils.isBlank(xmlData) || !StringUtils.isBlank(xmlDataVariable))) {
                    tableName = DataFlowSupport.getTheTableNameBasedOnTheXMLAccessMode(accessmode, xmlData, xmlDataVariable);
                }
                String dummyData = "dummy";
                if (StringUtils.isBlank(tableName)) {
                    componentName = componentName + Delimiter.ed_ge_Delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName + Delimiter.ed_ge_Delimiter + dummyData;
                } else {
                    tableName = tableName + Delimiter.tableDelimiter;
                    keyTableName = tableName;
                    if (!StringUtils.isBlank(fileDataBaseType) && isFileComponentFlag) {;
                        if (StringUtils.isNotBlank(odataSourceConnectionUrl)) {
                            fileDataBaseType = fileDataBaseType + Delimiter.oDataUrlDelimiter + odataSourceConnectionUrl;
                        }
                        tableName = tableName + Delimiter.fileTypeDelimiter + fileDataBaseType;
                    }
                    if (!StringUtils.isBlank(fileExtension)) {
                        tableName = tableName + Delimiter.fileExtensionDelimiter + fileExtension;
                    }
                    if (StringUtils.isNotBlank(databaseName) || StringUtils.isNotBlank(serverName)) {
                        componentName = tableName + Delimiter.ed_ge_Delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName + Delimiter.ed_ge_Delimiter + dummyData + Delimiter.ed_ge_Delimiter + componentName;
                    } else {
                        componentName = tableName + Delimiter.ed_ge_Delimiter + dummyData + Delimiter.ed_ge_Delimiter + componentName;
                    }

                }

                if (!StringUtils.isBlank(joinTypeForMergeComponent)) {
                    if ("2".equalsIgnoreCase(joinTypeForMergeComponent)) {
                        joinTypeNameForMergeComponent = "Inner join";
                    } else if ("1".equalsIgnoreCase(joinTypeForMergeComponent)) {
                        joinTypeNameForMergeComponent = "Left outer join";
                    } else if ("0".equalsIgnoreCase(joinTypeForMergeComponent)) {
                        joinTypeNameForMergeComponent = "Full outer join";
                    }

                    mergeComponentMapForExtendedProperties.put(executableName + "~" + actualComponentName, joinTypeNameForMergeComponent);
                }

                if (StringUtils.isNotBlank(query)) {
                    try {

                        queryMap.put(executableName + Delimiter.ed_ge_Delimiter + actualComponentName + "$queryName", query + Delimiter.delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

            }

            ArrayList<String> inRefList = new ArrayList<>();
            ArrayList<String> outRefList = new ArrayList<>();
            String returnedTotalOutPutColumns = "";
            String returnedTotalInputColumns = "";
            String assignedTotalOutPutColumns = "";
            for (int i = 0; i < componentList.item(j).getChildNodes().getLength(); i++) {
                Node componentChilds = componentList.item(j).getChildNodes().item(i);
                if (componentChilds.getNodeName().equals("inputs")) {
                    returnedTotalInputColumns = "";
                    NodeList inputsList = componentChilds.getChildNodes();
                    for (int k = 0; k < inputsList.getLength(); k++) {
                        if (inputsList.item(k).getNodeName().equals("#text")) {
                            continue;
                        }
                        String inputColumnRefId = inputsList.item(k).getAttributes().getNamedItem("refId").getTextContent();
                        try {
                            String[] stringArr = inputColumnRefId.split("\\\\");
                            dataFlowName = stringArr[stringArr.length - 1];
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        componentClassId = inputsList.item(k).getParentNode().getParentNode().getAttributes().getNamedItem("componentClassID").getTextContent();
                        if (componentClassIdsMap.get(componentClassId) != null) {
                            componentClassId = componentClassIdsMap.get(componentClassId);
                        }

                        inRefList.add(inputColumnRefId);
                        if (componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {
                            returnedTotalInputColumns = IterateInputAndOutPutColumns2014.iterateForInputColumns(inputsList.item(k), inputColumnRefId, componentClassId, componentName, componentConnections, executableName, actualComponentName);
                            IterateInputAndOutPutColumns2014.iterateForDerivedInputColumns(inputsList.item(k), inputColumnRefId, componentClassId, componentName, packageLevelMap);
                        } else {
                            returnedTotalInputColumns = IterateInputAndOutPutColumns2014.iterateForInputColumns(inputsList.item(k), inputColumnRefId, componentClassId, componentName, componentConnections, executableName, actualComponentName);
                        }
                    }

                } else if (componentChilds.getNodeName().equals("outputs")) {
                    NodeList outputsList = componentChilds.getChildNodes();
                    returnedTotalOutPutColumns = "";
                    for (int k = 0; k < outputsList.getLength(); k++) {
                        if (outputsList.item(k).getNodeName().equals("#text")) {
                            continue;
                        }
                        String outputColumnRefId = outputsList.item(k).getAttributes().getNamedItem("refId").getTextContent();
                        String outputName = "";
                        try {

                            outputName = outputsList.item(k).getAttributes().getNamedItem("name").getTextContent();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        outRefList.add(outputColumnRefId);
                        if (outputsList.item(k).getAttributes().getNamedItem("isErrorOut") == null) {
                            String componentRefIdAndcomponentClassIdAndcomponentName
                                    = componentClassId + Delimiter.delimiter + componentName + Delimiter.delimiter + outputColumnRefId;

                            returnedTotalOutPutColumns = IterateInputAndOutPutColumns2014.iterateForOutputColumns(outputsList.item(k), componentRefIdAndcomponentClassIdAndcomponentName, dataFlowName, packageLevelMap, executableName, actualComponentName, componentConnections, outputName);
                            if (!StringUtils.isBlank(returnedTotalOutPutColumns)) {
                                assignedTotalOutPutColumns = returnedTotalOutPutColumns;
                            }

                        }
                    }
                }
            }

            if (!StringUtils.isBlank(storeProcName) && !StringUtils.isBlank(assignedTotalOutPutColumns)) {
                storeProcTableAndItsColumnsMap.put(executableName + storeProcName, assignedTotalOutPutColumns);
            }
            if (isFileComponentFlag && componentDecision.equalsIgnoreCase("Source") && !StringUtils.isBlank(assignedTotalOutPutColumns)) {
                storeProcTableAndItsColumnsMap.put(executableName + "~" + keyTableName, assignedTotalOutPutColumns);
            } else if (isFileComponentFlag && componentDecision.equalsIgnoreCase("target") && !StringUtils.isBlank(returnedTotalInputColumns)) {
                storeProcTableAndItsColumnsMap.put(executableName + "~" + keyTableName, returnedTotalInputColumns);
            } else if (isFileComponentFlag && !StringUtils.isBlank(returnedTotalInputColumns) && StringUtils.isBlank(assignedTotalOutPutColumns)) {
                storeProcTableAndItsColumnsMap.put(executableName + "~" + keyTableName, returnedTotalInputColumns);
            } else if (isFileComponentFlag && StringUtils.isBlank(returnedTotalInputColumns) && !StringUtils.isBlank(assignedTotalOutPutColumns)) {
                storeProcTableAndItsColumnsMap.put(executableName + "~" + keyTableName, assignedTotalOutPutColumns);
            }
            storeProcName = "";
            isFileComponentFlag = false;
//            String modifiedQuery = "";
//            if (!query.equals("") && query.trim().contains("*")) {
//                modifiedQuery = SqlParserUtil.updateQueryIfContainsStarInIt(query, databaseName, assignedTotalOutPutColumns, returnedTotalInputColumns, queryMap, executableName, actualComponentName, serverName, dataFlowBean);
//            }

//            if (StringUtils.isNotBlank(modifiedQuery) && StringUtils.isNotBlank(query)) {
//                actualQueryWithModifiedQuery.put(modifiedQuery, query);
//            }
            DataFlowSupport.prepareComponenInputOutputsPath(inRefList, outRefList, pathInputsOutputsMap);
            componentNamerefIds.put(componentRefId, componentName);
            DataFlowComponentUtil.makeDataFlowSetterMethodsAsEmpty(dataFlowBean);
        }

    }

    /**
     * this method will specify whether the component belongs to source or
     * target,we are ignoring if it is a middle component
     *
     * @param componentClassID
     * @param contactInfo
     * @return component Decision.
     */
    private static String componentsTypeDecision(String componentClassID, String contactInfo) {

        String comType = "";
        if (contactInfo.contains("OLE DB Source")) {
            contactInfo = contactInfo.replace(" ", "").toUpperCase();
        }
        if ((componentClassID.contains("Microsoft.ManagedComponentHost")
                || componentClassID.contains("Microsoft.ExcelSource")
                || componentClassID.contains("Microsoft.FlatFileSource")
                || componentClassID.contains("Microsoft.OLEDBSource")
                || componentClassID.contains("Microsoft.SSISOracleSrc")
                || componentClassID.contains("{165A526D-D5DE-47FF-96A6-F8274C19826B}")
                || componentClassID.contains("Microsoft.RawSource")
                || componentClassID.contains("Attunity.SSISODBCSrc")
                || componentClassID.contains("DTSAdapter.OLEDBSource.3")
                //                || componentClassID.contains("Microsoft.OLEDBSource")
                || componentClassID.contains("Microsoft.SSISODBCSrc"))
                && (contactInfo.contains("Consumes data from SQL Server, OLE DB, ODBC, or Oracle, using the corresponding .NET Framework data provider")
                || contactInfo.contains("Attunity Ltd.; All Rights Reserved; http://www.attunity.com;")
                || contactInfo.contains("Excel Source;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation;")
                || contactInfo.contains("Excel Source;Microsoft Corporation; Microsoft SQL Server")
                || contactInfo.contains("Excel Source")
                || contactInfo.contains("Flat File Source;Microsoft Corporation; Microsoft SQL Server")
                || contactInfo.contains("Flat File Source;Microsoft Corporation; Microsoft SqlServer v10;")
                || contactInfo.contains("Flat File Source")
                || contactInfo.contains("OLEDBSOURCE;MICROSOFTCORPORATION;MICROSOFTSQLSERVER")
                || contactInfo.contains("Reads raw data from a flat file that was previously written by the Raw File destination.")
                || contactInfo.contains("Extracts data from an XML file. For example, extract catalog data from an XML file that represents catalogs and")
                || contactInfo.contains("ODBC Source;Connector for Open Database Connectivity (ODBC) by Attunity")
                || contactInfo.contains("MicrosoftContactInfo") || contactInfo.contains("Source OLE DB;Microsoft Corporation; Microsoft SqlServer v10; (C) Microsoft Corporation; Tous droits réservés; http://www.microsoft.com/sql/support;7")
                || contactInfo.contains("OData Source Component;Microsoft Corporation; Microsoft SQL Server;")
                || contactInfo.contains("OData Source Component")
                || contactInfo.contains("OLEDBSOURCE;MICROSOFTCORPORATION;MICROSOFTSQLSERVERV10;(C)MICROSOFTCORPORATION;ALLRIGHTSRESERVED;HTTP://www.microsoft.com/SQL/SUPPORT;7")
                || contactInfo.contains("Microsoft.OLEDBSource")
                || contactInfo.contains("OLE DB Source;Microsoft Corporation")
                || contactInfo.contains("Origen de OLE DB;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; Todos los derechos reservados")
                || contactInfo.contains("Origen de Excel;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; Todos los derechos reservados; http://www.microsoft.com/sql/support;1")
                || contactInfo.contains("Origen de Excel;Microsoft Corporation")
                || contactInfo.contains("Oracle Source")
                || contactInfo.contains("Origin of Excel; Microsoft Corporation")
                || contactInfo.contains("KingswaySoft Inc.; http://www.kingswaysoft.com; support@kingswaysoft.com; Copyright")
                || contactInfo.contains("Excel Source;Microsoft Corporation; Microsoft SqlServer v10; (C) Microsoft Corporation; All Rights Reserved; http://www.microsoft.com/sql/support;1")
                || contactInfo.contains("ODBC Source;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; All Rights Reserved; http://www.microsoft.com/sql/support;1"))) {
            comType = "Source";

        } else if ((componentClassID.contains("Microsoft.ManagedComponentHost")
                || componentClassID.contains("Microsoft.PXPipelineProcessDM")
                || componentClassID.contains("Microsoft.PXPipelineProcessDimension")
                || componentClassID.contains("Microsoft.ExcelDestination")
                || componentClassID.contains("Microsoft.FlatFileDestination")
                || componentClassID.contains("{8DA75FED-1B7C-407D-B2AD-2B24209CCCA4}")
                || componentClassID.contains("Microsoft.OLEDBDestination")
                || componentClassID.contains("{4ADA7EAA-136C-4215-8098-D7A7C27FC0D1}")
                || componentClassID.contains("Microsoft.PXPipelineProcessPartition")
                || componentClassID.contains("Microsoft.RawDestination")
                || componentClassID.contains("Microsoft.ConditionalSplit")
                || componentClassID.contains("Microsoft.RecordsetDestination")
                || componentClassID.contains("Microsoft.SQLServerDestination")
                || componentClassID.contains("Attunity.SSISODBCDst")
                || componentClassID.contains("Microsoft.Multicast")
                || componentClassID.contains("Microsoft.UnionAll")
                || componentClassID.contains("Microsoft.OLEDBCommand")
                || componentClassID.contains("DTSAdapter.OLEDBDestination.3")
                || componentClassID.contains("Microsoft.SSISODBCDst")
                || componentClassID.contains("Microsoft.SSISOracleDst"))
                && (contactInfo.contains("Loads data into an ADO.NET-compliant database that uses a database table or view")
                || contactInfo.contains("Exposes data in a data flow to other applications by using the ADO.NET DataReader interface")
                || contactInfo.contains("Excel Destination")
                || contactInfo.contains("Upsert Destinaton")
                || contactInfo.contains("Flat File Destination")
                || contactInfo.contains("OLE DB Destination")
                || contactInfo.contains("Oracle Destination")
                || contactInfo.contains("Writes raw data that will not require parsing or translation")
                || contactInfo.contains("Recordset Destination")
                || contactInfo.contains("Writes data to a table in a SQL Server Compact database")
                || contactInfo.contains("SQL Server Destination")
                || contactInfo.contains("No contactInfo")
                || contactInfo.contains("dws@DWS.com.au")
                || contactInfo.contains("DWS GROUP: MERGE COMPONENT")
                || contactInfo.contains("OLE DB Command") || contactInfo.contains("Destination OLE DB;Microsoft Corporation; Microsoft SqlServer v10; (C) Microsoft Corporation; Tous droits réservés; http://www.microsoft.com/sql/support;4")
                || contactInfo.contains("ODBC Destination;Connector for Open Database Connectivity (ODBC) by Attunity")
                || contactInfo.contains("DTSAdapter.OLEDBDestination.3")
                || contactInfo.contains("support@pragmaticworks.com;http://www.pragmaticworks.com")
                || contactInfo.contains("Destino de OLE DB;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; Todos los derechos reservados; http://www.microsoft.com/sql/support;4")
                || contactInfo.contains("ODBC Destination;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; All Rights Reserved; http://www.microsoft.com/sql/support;1"))) {
            comType = "Target";
        } else if (componentClassID.contains("Microsoft.Lookup") || contactInfo.contains("Lookup;Microsoft Corporation; Microsoft SQL Server;")) {
            comType = "Lookup";
        }

        return comType;
    }

    /**
     * this method will return database and serverName from the connections Hash
     * map providing connectionRef id and connections Map are mandatory for the
     * relational databases
     *
     * @param connectionRefId
     * @param connectionsMap
     * @return Server And DatabaseName
     */
    private static String getDatabaseAndServerNameFromConnectionString(String connectionRefId, Map<String, String> connectionsMap) {

        String databaseName = "";
        String serverName = "";
        String dummyData = "dummy_data";
        if (connectionRefId.contains(Constants.external) || connectionRefId.contains(Constants.invalid)) {
            connectionRefId = connectionRefId.replace(Constants.external, "").replace(Constants.invalid, "");
        }
        String objectName = "";
        try {
            if (connectionRefId.contains("[") && connectionRefId.contains("]")) {
                int startIndex = connectionRefId.indexOf("[");
                int endIndex = connectionRefId.indexOf("]");
                objectName = connectionRefId.substring(startIndex + 1, endIndex);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        String connectionString = "";
        if (connectionsMap.get(connectionRefId) != null) {
            connectionString = connectionsMap.get(connectionRefId);
        } else if (connectionsMap.get(objectName) != null) {
            connectionString = connectionsMap.get(objectName);
        }
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
        if (StringUtils.isNotBlank(connectionString)) {
            String[] connectionArray = connectionString.split(Delimiter.delimiter);
            int length = connectionArray.length;
            switch (length) {
                case 1:
                    serverName = connectionArray[0];
                    break;
                case 2:
                    databaseName = connectionArray[1];
                    serverName = connectionArray[0];
                    break;
                default:
                    serverName = "";
                    databaseName = "";
                    break;

            }

        }
        return databaseName + Delimiter.delimiter + serverName + Delimiter.delimiter + dummyData;

    }

    /**
     * in this method we are adding disbaled dataflow details into Hashmap
     *
     * @param dataflowName
     * @param diableValue
     */
    public static void addTheDisbledDataflowtToTheMap(String dataflowName, String diableValue) {

        if (StringUtils.isNotBlank(diableValue)) {
            SSISController.disbaledAndDeletedDataFlows.put(dataflowName, diableValue);
        }
    }

}
