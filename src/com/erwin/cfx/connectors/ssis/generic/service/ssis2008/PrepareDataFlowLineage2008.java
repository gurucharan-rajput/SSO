/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2008;

import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.beans.DataFlowBean;
import com.erwin.cfx.connectors.ssis.generic.beans.SSISInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.util.ConnectionStringUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.DataFlowComponentUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.SqlParserUtil;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author THarika
 */
public class PrepareDataFlowLineage2008 {

    static Map<String, String> inputIdAndOutPutIdAskeyAndComponentIdAsValue = new LinkedHashMap<>();
    static Map<String, String> componentIdAndItsInputSet = new LinkedHashMap<>();
    static Map<String, String> componentLineageMap = new LinkedHashMap<>();
    static Map<String, List<Map<String, String>>> finalColumnLevelLinageMap = new LinkedHashMap<>();
    static Map<String, String> connectionandtableMap = new HashMap<>();
    static Map<String, Map<String, String>> eachExecutableConnections = new HashMap<>();
    static Map<String, String> connections = new HashMap<>();
    static Map<String, String> componentNamerefIds = new LinkedHashMap<>();
    static Map<String, Map<String, List<String>>> querycomponentmap = new LinkedHashMap<>();
    static Map<String, Map<String, Map<String, List<String>>>> componentlevelmap = new LinkedHashMap<>();
    static Map<String, String> componentClassIds = new LinkedHashMap<>();
    static Set<String> removeDuplicates = new HashSet<>();
    static Map<String, String> pathInputsOutputsMap = new LinkedHashMap<>();
    static Map<String, Set<Map<String, String>>> componentColumnIdsForInput = new LinkedHashMap<>();
    static Map<String, String> columnRefIdAndTotalColumnDetailsMap = new LinkedHashMap<>();
    static Map<String, String> startOrEndIdComponentIdHM = new LinkedHashMap<>();
    public static Map<String, String> storeProcTableAndItsColumnsMap = new HashMap<>();
    static Map<String, String> ddlCacheMap = new HashMap<>();
    public static Map<String, String> actualQueryWithModifiedQuery = new HashMap<>();
    static Map<String, Set<Map<String, String>>> componentColumnIdsForOutput = new LinkedHashMap<>();

    static String variablesString = "";
    static boolean rowset = false;

    /**
     * this method clears memory of static variables
     */
    public static void clearStaticMemory() {

        finalColumnLevelLinageMap.clear();
        connections = new HashMap<>();
        eachExecutableConnections.clear();
        componentColumnIdsForInput.clear();
        componentColumnIdsForOutput.clear();
        componentNamerefIds.clear();
        componentClassIds.clear();
        startOrEndIdComponentIdHM.clear();
        columnRefIdAndTotalColumnDetailsMap.clear();
        removeDuplicates.clear();
        pathInputsOutputsMap.clear();
        componentlevelmap.clear();
        variablesString = "";
        rowset = false;
        storeProcTableAndItsColumnsMap = new HashMap<>();

    }

    /**
     * In this method,we are preparing a hash map by iterating each data flow
     * and collecting the components lineage,columns lineage and extended
     * properties present in it
     *
     * @param dataflowComponentNodeList
     * @param dataflowBean
     * @return
     */
    public static Map<String, List<Map<String, String>>> iterateDataFlowComponentsData(NodeList dataflowComponentNodeList, DataFlowBean dataflowBean) {

        clearStaticMemory();
        SSISInputParameterBean ssisInputParameterBean = dataflowBean.getSsisInputParameterBean();

        Set<String> disabledComponentSet = ssisInputParameterBean.getDisabledComponentSet();
        Parser2008XMLFile parser2008XMLFile = ssisInputParameterBean.getParser2008XMLFile();
        connections = ssisInputParameterBean.getConnectionsMap();

        ArrayList<String> executableNameList = new ArrayList<>();

        for (int i = 0; i < dataflowComponentNodeList.getLength(); i++) {
            Node executableNode = dataflowComponentNodeList.item(i).getParentNode().getParentNode();
            String dtsID = "";
            String executableName = "";
            String dataflowname = "";
            String executableReferenceId = "";

            try {
                dataflowname = parser2008XMLFile.getDataFlowName(executableNode);
                executableName = dataflowname.split(Delimiter.delimiter)[0];
                executableName = executableName.replaceAll(Constants.commonRegularExpression, "_");
                if (dataflowname.split(Delimiter.delimiter).length > 1) {

                    executableReferenceId = dataflowname.split(Delimiter.delimiter)[1];
                    dtsID = dataflowname.split(Delimiter.delimiter)[2];
                }
                if (executableNameList.contains(executableName) && !StringUtils.isBlank(executableReferenceId)) {
                    executableName = executableReferenceId.replace("Package\\", "").replace("\\", "_");
                }
                executableNameList.add(executableName);
                if (disabledComponentSet.contains(dtsID)) {
                    continue;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            inputIdAndOutPutIdAskeyAndComponentIdAsValue = new LinkedHashMap<>();
            componentIdAndItsInputSet = new LinkedHashMap<>();
            componentLineageMap = new LinkedHashMap<>();
            Map<String, String> componentConnections = new LinkedHashMap<>();
            NodeList pipeLinesChilds = dataflowComponentNodeList.item(i).getChildNodes();
            HashMap<String, String> queryMap = new HashMap<>();

            for (int j = 0; j < pipeLinesChilds.getLength(); j++) {
                if (pipeLinesChilds.item(j).getNodeName().equals("paths")) {
                    Node pathsNode = pipeLinesChilds.item(j);
                    componentConnections = parser2008XMLFile.prepareComponentConnections(pathsNode.getChildNodes());
                }
            }

            dataflowBean.setComponentConnections(componentConnections);

            for (int j = 0; j < pipeLinesChilds.getLength(); j++) {
                if (pipeLinesChilds.item(j).getNodeName().equals("components")) {
                    Node componentsNode = pipeLinesChilds.item(j);
                    queryMap = new HashMap<>();
                    try {
                        prepareComponentDetailsAndColumnDetails(componentsNode.getChildNodes(), executableName, queryMap, dataflowBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            componentConnections = DataFlowSupport.prepareComponentLineageMap(componentConnections);
            Map<String, Map<String, String>> eachExecutableConnections = new HashMap<>();
            eachExecutableConnections.put(executableName, componentConnections);

            finalColumnLevelLinageMap = BuildDataFlowLineage.createMapping(componentConnections, executableName, queryMap);
        }
        return finalColumnLevelLinageMap;
    }

    /**
     * In this method,we are collecting components details,their
     * lineage,queries(if present) for a given data flow
     *
     * @param componentList
     * @param executableName
     * @param queryMap
     * @param dataFlowBean
     */
    public static void prepareComponentDetailsAndColumnDetails(NodeList componentList, String executableName, Map<String, String> queryMap, DataFlowBean dataFlowBean) {

        long startTime = System.currentTimeMillis(); //prepareComponentDetailsAndColumnDetails
        ACPInputParameterBean acpInputParameterBean = dataFlowBean.getAcpInputParameterBean();
        SSISInputParameterBean ssisInputParameterBean = dataFlowBean.getSsisInputParameterBean();
        Map<String, String> componentConnections = dataFlowBean.getComponentConnections();

        Map<String, String> componentClassIdsMap = acpInputParameterBean.getComponentClassIdsMap();
        String defaultSystemName = acpInputParameterBean.getDefaultSystemName();
        String defaultEnvrionmentName = acpInputParameterBean.getDefaultEnvrionmentName();

        Map<String, String> userReferenceVariablesMap = ssisInputParameterBean.getUserReferenceVariablesMap();

        String dataFlowName = "";
        String queryMapKeyComponentName = "";
        System.out.println("DataFlowName------" + executableName);
        for (int j = 0; j < componentList.getLength(); j++) {
            if (componentList.item(j).getNodeName().equals(Constants.textLiteral)) {
                continue;
            }
            String componentRefId = componentList.item(j).getAttributes().getNamedItem("id").getTextContent();
            String componentName = componentList.item(j).getAttributes().getNamedItem("name").getTextContent();
            String actualComponentName = componentName;
            queryMapKeyComponentName = "";

            String componentClassId = componentList.item(j).getAttributes().getNamedItem("componentClassID").getTextContent();
            if (componentClassIdsMap.get(componentClassId) != null) {
                componentClassId = componentClassIdsMap.get(componentClassId);
                if (StringUtils.isNotBlank(componentClassId)) {
                    componentClassId = componentClassId.trim();
                }
            }
            String contactInfo = "";
            componentClassIds.put(componentRefId, componentClassId);
            if (componentList.item(j).getAttributes().getNamedItem("contactInfo") != null) {
                contactInfo = componentList.item(j).getAttributes().getNamedItem("contactInfo").getTextContent();
            } else {
                contactInfo = componentClassId;
            }
            String componentDecision = DataFlowSupport.componentsTypeDecision(componentClassId, contactInfo);
            String connectionRefId = "";
            String tableName = "";
            String keyTableName = "";
            String openRowsetTableName = "";
            String sheetName = "";
            String query = "";
//            String queryFromVarible = "";

            String environmentName = "";
            String systemName = "";
            String fileDataBaseType = "";
            boolean isFileComponentFlag = false;
            String fileExtension = "";
            String dfUtilDelimiter = Delimiter.dataFlowUtilDelimiter;
            String storeProcName = "";

            DataFlowComponentUtil dataFlowComponentUtil = new DataFlowComponentUtil();

            if (componentDecision.equalsIgnoreCase("Source") || componentDecision.equalsIgnoreCase("Target") || componentDecision.equalsIgnoreCase("Lookup") || componentDecision.equalsIgnoreCase("Multicast")) {

                NodeList nodeListChildNodes = componentList.item(j).getChildNodes();
                for (int k = 0; k < nodeListChildNodes.getLength(); k++) {
                    IntFunction<String> function = (i) -> {
                        String tableCombinationData = "";
                        NodeList p = null;
                        NodeList nodeListChildNodes1 = null;
                        nodeListChildNodes1 = nodeListChildNodes;
                        Node item = null;
                        item = nodeListChildNodes1.item(i);
                        String connectionIdRef = null;
                        String nodeName = item.getNodeName();
                        if (Constants.connections.equalsIgnoreCase(nodeName)) {
                            p = item.getChildNodes();
                            for (int l = 0; l < p.getLength(); l++) {
                                if ("connection".equalsIgnoreCase(p.item(l).getNodeName())) {
                                    connectionIdRef = p.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();
                                    tableCombinationData = DataFlowComponentUtil.getTableRelatedInfoFromConnectionString(connectionIdRef, connections, dataFlowBean);
                                }
                            }
                        }
                        return tableCombinationData;
                    };

                    if ("Microsoft.FlatFileSource".equalsIgnoreCase(componentClassId)) {
                        if (Constants.connections.equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            String tableCombinationData = function.apply(k);
                            try {
                                if (StringUtils.isNotBlank(tableCombinationData)) {
                                    tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                    fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                    isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                    fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } else if ("Microsoft.FlatFileDestination".equalsIgnoreCase(componentClassId)) {
                        if (Constants.connections.equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            String tableCombinationData = function.apply(k);
                            try {
                                tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                    } else if ("properties".equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                        NodeList property = nodeListChildNodes.item(k).getChildNodes();
                        String tableOrQuery = prepareTableNameAndQueryFromProperties(property, userReferenceVariablesMap, componentDecision, componentClassId, dataFlowBean);

                        if (StringUtils.isNotBlank(tableOrQuery) && tableOrQuery.endsWith(Constants.queryConstant)) {
                            query = tableOrQuery.replace(Constants.queryConstant, "");
                        } else if (StringUtils.isNotBlank(tableOrQuery) && tableOrQuery.endsWith(Constants.tableConstant)) {
                            tableName = tableOrQuery.replace(Constants.tableConstant, "");
                        } else {
                            tableName = "";
                            query = "";
                        }
                    } else if ("Microsoft.ExcelSource".equalsIgnoreCase(componentClassId)) {
                        if (Constants.connections.equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            NodeList property = nodeListChildNodes.item(k).getChildNodes();
                            for (int l = 0; l < property.getLength(); l++) {
                                if ("connection".equalsIgnoreCase(property.item(l).getNodeName())) {
                                    connectionRefId = property.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();
                                    String tableCombinationData = DataFlowComponentUtil.getTableRelatedInfoFromConnectionString(connectionRefId, connections, dataFlowBean);
                                    try {
                                        tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                        fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                        isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                        fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }

                            }
                        }
                    } else if ("Microsoft.ExcelDestination".equalsIgnoreCase(componentClassId)) {
                        if (Constants.connections.equalsIgnoreCase(nodeListChildNodes.item(k).getNodeName())) {
                            NodeList property = nodeListChildNodes.item(k).getChildNodes();
                            for (int l = 0; l < property.getLength(); l++) {
                                if ("connection".equalsIgnoreCase(property.item(l).getNodeName())) {
                                    connectionRefId = property.item(l).getAttributes().getNamedItem("connectionManagerID").getTextContent();
                                    String tableCombinationData = DataFlowComponentUtil.getTableRelatedInfoFromConnectionString(connectionRefId, connections, dataFlowBean);
                                    try {
                                        tableName = tableCombinationData.split(dfUtilDelimiter)[0];
                                        fileDataBaseType = tableCombinationData.split(dfUtilDelimiter)[1];
                                        isFileComponentFlag = Boolean.parseBoolean(tableCombinationData.split(dfUtilDelimiter)[2]);
                                        fileExtension = tableCombinationData.split(dfUtilDelimiter)[3];
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
                                connectionandtableMap.put(componentName, connectionRefId);

                                String connectionString = "";
                                if (connections.get(connectionRefId) != null) {

                                    connectionString = connections.get(connectionRefId).toString();

                                    String defaultConnectionString = "";
                                    try {
                                        if (!StringUtils.isBlank(connectionString) && connectionString.contains(Delimiter.connectionStringDelimiter)) {
                                            defaultConnectionString = connectionString.split(Delimiter.connectionStringDelimiter)[1];
                                            connectionString = connectionString.split(Delimiter.connectionStringDelimiter)[0];
                                            if (!StringUtils.isBlank(connectionString)) {
                                                if (connectionString.toLowerCase().contains("user")) {
                                                    connectionString = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbols(connectionString, "user", "");
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
                                    if (!StringUtils.isBlank(connectionString)) {
                                        String tabEnvAndSystemName = DataFlowComponentUtil.getTableAndSystemAndEnvNameFromConnectionString(connectionString);

                                        int length = tabEnvAndSystemName.split(Delimiter.delimiter).length;
                                        switch (length) {

                                            case 2:
                                                environmentName = tabEnvAndSystemName.split(Delimiter.delimiter)[1];
                                                systemName = tabEnvAndSystemName.split(Delimiter.delimiter)[0];
                                                break;
                                            case 3:
                                                environmentName = tabEnvAndSystemName.split(Delimiter.delimiter)[1];
                                                systemName = tabEnvAndSystemName.split(Delimiter.delimiter)[2];
                                                break;
                                            default:
                                                break;
                                        }

                                    }

                                }

                            }
                        }
                    }

                }

                if (environmentName
                        == null || "".equals(environmentName)) {
                    environmentName = defaultEnvrionmentName;
                }

                try {
                    if (!StringUtils.isBlank(query)) {
                        if (query.toUpperCase().contains("EXECUTE ") || query.toUpperCase().contains("EXEC ") || query.toUpperCase().contains("EXEC") || query.toUpperCase().contains("EXECUTE")) {

                            String queryAndComponentName = SqlParserUtil.makeStorePrcedureNameAsComponentName(query, componentName);
                            if (queryAndComponentName.contains(Delimiter.delimiter)) {
                                int length = queryAndComponentName.split(Delimiter.delimiter).length;
                                if (length == 2) {
                                    query = queryAndComponentName.split(Delimiter.delimiter)[0];
                                    storeProcName = queryAndComponentName.split(Delimiter.delimiter)[1];
                                    componentName = storeProcName;
                                } else if (length == 1) {
                                    query = queryAndComponentName.split(Delimiter.delimiter)[0];
                                }
//                            query = "";
                            }
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String dummy = "dummy";

                if (StringUtils.isBlank(tableName)) {
                    componentName = (componentName + Delimiter.ed_ge_Delimiter + environmentName + Delimiter.ed_ge_Delimiter + systemName + Delimiter.ed_ge_Delimiter + dummy);
                } else {

                    tableName = tableName + Delimiter.tableDelimiter;
                    keyTableName = tableName;
                    if (!StringUtils.isBlank(fileDataBaseType) && isFileComponentFlag) {
                        tableName = tableName + Delimiter.fileTypeDelimiter + fileDataBaseType;
                    }
                    if (!StringUtils.isBlank(fileExtension)) {
                        tableName = tableName + Delimiter.fileExtensionDelimiter + fileExtension;
                    }
                    if (StringUtils.isNotBlank(environmentName) || StringUtils.isNotBlank(systemName)) {
                        componentName = tableName + Delimiter.ed_ge_Delimiter + environmentName + Delimiter.ed_ge_Delimiter + systemName + Delimiter.ed_ge_Delimiter + dummy;
                    } else {
                        componentName = tableName + Delimiter.ed_ge_Delimiter + dummy;
                    }
                }
                queryMapKeyComponentName = "";
                queryMapKeyComponentName = componentName;

                if (!"".equals(query)) {

                    queryMap.put(executableName + Delimiter.ed_ge_Delimiter + actualComponentName + "$queryName", query + Delimiter.delimiter + environmentName + Delimiter.ed_ge_Delimiter + systemName);
                }
                fileExtension = "";

            }

            List<String> inRefList = new ArrayList<>();
            List<String> outRefList = new ArrayList<>();
            String returnedTotalOutPutColumns = "";
            String returnedTotalInputColumns = "";
            String assignedTotalOutPutColumns = "";

            if ("Microsoft.DataConvert".equalsIgnoreCase(componentClassId)) {
                for (int i = 0; i < componentList.item(j).getChildNodes().getLength(); i++) {
                    Node componentChilds = componentList.item(j).getChildNodes().item(i);

                    if ("outputs".equals(componentChilds.getNodeName())) {
                        NodeList outputsList = componentChilds.getChildNodes();

                        for (int t = 0; t < outputsList.getLength(); t++) {
                            if (outputsList.item(t).getNodeName().equals(Constants.textLiteral)) {
                                continue;
                            }

                            Node output = outputsList.item(t);

                            IterateInputAndOutPutColumns2008.prepareLineageIdAsKeyAndOutputColumnNameAsValueForDataConversion(output);
                        }

                    }
                }
            }
            for (int i = 0; i < componentList.item(j).getChildNodes().getLength(); i++) {
                Node componentChilds = componentList.item(j).getChildNodes().item(i);

                if (componentChilds.getNodeName().equals("inputs")) {
                    returnedTotalInputColumns = "";
                    NodeList inputsList = componentChilds.getChildNodes();
                    for (int k = 0; k < inputsList.getLength(); k++) {
                        if (inputsList.item(k).getNodeName().equals("#text")) {
                            continue;
                        }
                        String inputAttributeId = inputsList.item(k).getAttributes().getNamedItem("id").getTextContent();
                        startOrEndIdComponentIdHM.put(inputAttributeId, componentRefId);
                        inputIdAndOutPutIdAskeyAndComponentIdAsValue.put(inputAttributeId, componentRefId);
                        if (componentIdAndItsInputSet.get(componentRefId) == null) {
                            componentIdAndItsInputSet.put(componentRefId, inputAttributeId);
                        } else {
                            String prevoiusInputId = componentIdAndItsInputSet.get(componentRefId);
                            componentIdAndItsInputSet.put(componentRefId, prevoiusInputId + Delimiter.delimiter + inputAttributeId);
                        }
                        try {
                            String[] stringArr = inputAttributeId.split("\\\\");
                            dataFlowName = stringArr[stringArr.length - 1];
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        componentClassId = inputsList.item(k).getParentNode().getParentNode().getAttributes().getNamedItem("componentClassID").getTextContent();
                        if (componentClassIdsMap.get(componentClassId) != null) {
                            componentClassId = componentClassIdsMap.get(componentClassId);
                        }

                        inRefList.add(inputAttributeId);
                        if (componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {
                            returnedTotalInputColumns = IterateInputAndOutPutColumns2008.iterateForInputColumns(inputsList.item(k), inputAttributeId, componentClassId, componentName, componentConnections);
                            IterateInputAndOutPutColumns2008.iterateForDerivedInputColumns(inputsList.item(k), inputAttributeId, componentClassId, componentName);
                        } else {
                            returnedTotalInputColumns = IterateInputAndOutPutColumns2008.iterateForInputColumns(inputsList.item(k), inputAttributeId, componentClassId, componentName, componentConnections);
                        }
                    }

                } else if (componentChilds.getNodeName().equals("outputs")) {
                    NodeList outputsList = componentChilds.getChildNodes();
                    returnedTotalOutPutColumns = "";
                    for (int k = 0; k < outputsList.getLength(); k++) {
                        if (outputsList.item(k).getNodeName().equals("#text")) {
                            continue;
                        }
                        String outputRefId = outputsList.item(k).getAttributes().getNamedItem("id").getTextContent();
                        String outputName = outputsList.item(k).getAttributes().getNamedItem("name").getTextContent();
                        startOrEndIdComponentIdHM.put(outputRefId, componentRefId);
                        inputIdAndOutPutIdAskeyAndComponentIdAsValue.put(outputRefId, componentRefId);
                        outRefList.add(outputRefId);
                        try {
                            if (outputsList.item(k).getAttributes().getNamedItem("isErrorOut") == null || outputsList.item(k).getAttributes().getNamedItem("isErrorOut").getTextContent().equalsIgnoreCase("false")) {
                                returnedTotalOutPutColumns = IterateInputAndOutPutColumns2008.iterateForOutputColumns(outputsList.item(k), outputRefId, componentClassId, componentName, dataFlowName, executableName, actualComponentName, userReferenceVariablesMap, outputName);
                            }
                            if (!StringUtils.isBlank(returnedTotalOutPutColumns)) {
                                assignedTotalOutPutColumns = returnedTotalOutPutColumns;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
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
            String modifiedQuery = "";
//            if (!query.equals("") && query.trim().contains("*")) {
//                modifiedQuery = SqlParserUtil.updateQueryIfContainsStarInIt(query, environmentName, assignedTotalOutPutColumns, returnedTotalInputColumns, queryMap, executableName, actualComponentName, systemName, dataFlowBean);
//            }
            storeProcName = "";
            DataFlowComponentUtil.prepareComponenInputOutputsPath(inRefList, outRefList, pathInputsOutputsMap);
            componentNamerefIds.put(componentRefId, componentName);

            if (!querycomponentmap.isEmpty()) {
                componentlevelmap.put(executableName, querycomponentmap);
            }
//            if (StringUtils.isNotBlank(modifiedQuery) && StringUtils.isNotBlank(query)) {
//                actualQueryWithModifiedQuery.put(modifiedQuery, query);
//            }
            long endTme = System.currentTimeMillis(); //prepareComponentDetailsAndColumnDetails
            System.out.println("time taken----in prepareComponentDetailsAndColumnDetails method::" + (endTme - startTime));

            DataFlowComponentUtil.makeDataFlowSetterMethodsAsEmpty(dataFlowBean);
        }
    }

    /**
     * this method returns query or table name based on the access mode for a
     * given property node list
     *
     * @param property
     * @param userReferenceVariablesMap
     * @param componentDecision
     * @param componentClassId
     * @return
     */
    public static String prepareTableNameAndQueryFromProperties(NodeList property, Map<String, String> userReferenceVariablesMap, String componentDecision, String componentClassId, DataFlowBean dataFlowBean) {
        String openRowsetTableName = "";
        String query = "";
        String tableName = "";
        String environmentName = "";
        String queryFromVarible = "";
        String accessmode = "";
        String sheetName = "";
        boolean isFileComponentFlag = false;
        String fileDataBaseType = "";

        for (int l = 0; l < property.getLength(); l++) {
            if ("property".equalsIgnoreCase(property.item(l).getNodeName())) {
                if ("OpenRowset".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    openRowsetTableName = property.item(l).getTextContent();
                    if (!StringUtils.isBlank(openRowsetTableName)) {
                        sheetName = openRowsetTableName;
                        tableName = openRowsetTableName;
                    }
                    if (!StringUtils.isBlank(sheetName) && sheetName.contains("$")) {
                        isFileComponentFlag = true;
                        fileDataBaseType = "CSV";
                    }
                }
                if ("DESTINATION_TABLE_NAME".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    openRowsetTableName = property.item(l).getTextContent();
                    if (openRowsetTableName != null && !StringUtils.isBlank(openRowsetTableName.trim())) {
                        tableName = openRowsetTableName;
                    }
                }
                if ("SqlCommand".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    query = property.item(l).getTextContent();
                } else if (property.item(l) != null && "XMLData".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    tableName = property.item(l).getTextContent();
                    if (tableName.contains(".xml") || tableName.contains(".XML")) {
                        environmentName = tableName;
                        try {
                            if (connections.get(environmentName) != null) {
                                environmentName = connections.get(tableName).split(Delimiter.emm_Delimiter)[2];
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } else if ("SqlCommandVariable".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {

                    String sqlvariable = property.item(l).getTextContent();
                    try {
                        if (sqlvariable.contains("User::")) {

                            sqlvariable = sqlvariable.replace("User::", "");
                            if (userReferenceVariablesMap.get(sqlvariable) != null) {
                                queryFromVarible = userReferenceVariablesMap.get(sqlvariable);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if ("AccessMode".equals(property.item(l).getAttributes().getNamedItem("name").getTextContent())) {
                    accessmode = property.item(l).getTextContent();
                }
            }
        }
        String dummy = "";
        dataFlowBean.setSheetName(sheetName);
        dataFlowBean.setAccessMode(accessmode);

        query = DataFlowComponentUtil.gettingQueryBasedOnAccessMode(query, queryFromVarible, accessmode);
        tableName = DataFlowComponentUtil.makeTableNameAsEmptyBasedOnAccessMode(accessmode, componentDecision, tableName, componentClassId);
        query = DataFlowComponentUtil.makeQueryAsEmptyBasedOnAccessMode(accessmode, componentDecision, query, componentClassId);

        if (StringUtils.isNotBlank(query) && StringUtils.isBlank(tableName)) {
            return query + Constants.queryConstant;
        } else if (StringUtils.isNotBlank(tableName) && StringUtils.isBlank(query)) {
            return tableName + Constants.tableConstant;
        } else {
            return "";
        }
    }

}
