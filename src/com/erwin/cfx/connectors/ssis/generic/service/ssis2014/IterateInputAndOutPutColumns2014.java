/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2014;

import com.erwin.cfx.connectors.ssis.generic.util.BoxingAndUnBoxingWrapper;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.DataFlowComponentUtil;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 24-08-2021
 */
public class IterateInputAndOutPutColumns2014 {

    public IterateInputAndOutPutColumns2014() {

    }

    public static Map<String, Set<Map<String, String>>> componentColumnIdsForInput = new LinkedHashMap<>();
    public static Map<String, Set<Map<String, String>>> componentColumnIdsForOutput = new LinkedHashMap<>();
    public static Map<String, Map<String, String>> conditionalSplitBusinessRuleMap = new HashMap<>();
    public static Map<String, String> externanColumnAndInputColumnMap = new HashMap<>();
    public static Map<String, String> dataConversionColumnMap = new HashMap<>();
    public static Map<String, String> columnLineageIdLookup = new HashMap<>();//derived component linageid as key and refid as value
    private static final String DESCRIPTION = "description";

    /**
     * this method will iterate for derived components and it will store
     * derived columns data in the componentColumnIdsForInput Hash map
     *
     * @param inputNode
     * @param componentRefId
     * @param componentClassId
     * @param componentName
     * @param varibleMap
     */
    public static void iterateForDerivedInputColumns(Node inputNode, String componentRefId, String componentClassId, String componentName, Map<String, String> varibleMap) {
        Node component = inputNode.getParentNode().getParentNode();
        NodeList componentNodeList = component.getChildNodes();
        Set<Map<String, String>> columnsSet = new HashSet<>();
        for (int i = 0; i < componentNodeList.getLength(); i++) {
            if ("inputs".equalsIgnoreCase(componentNodeList.item(i).getNodeName())) {

                NodeList inputsChildNodes = componentNodeList.item(i).getChildNodes();
                columnsSet = iterateDerivedInputColumnNodeList(inputsChildNodes, componentClassId, componentName, varibleMap);

                componentColumnIdsForInput.put(componentRefId, columnsSet);
            }

            if ("outputs".equalsIgnoreCase(componentNodeList.item(i).getNodeName())) {

                NodeList inputsChildNodes = componentNodeList.item(i).getChildNodes();
                for (int j = 0; j < inputsChildNodes.getLength(); j++) {
                    if ("output".equalsIgnoreCase(inputsChildNodes.item(j).getNodeName())) {
                        NodeList inputChildNodes = inputsChildNodes.item(j).getChildNodes();
                        for (int k = 0; k < inputChildNodes.getLength(); k++) {

                            if (inputsChildNodes.item(j).getAttributes().getNamedItem(DESCRIPTION) != null && inputsChildNodes.item(j).getAttributes().getNamedItem(DESCRIPTION).getTextContent().contains("Error")) {
                                continue;
                            }

                            if ("outputColumns".equalsIgnoreCase(inputChildNodes.item(k).getNodeName())) {

                                NodeList inputColumnsChildNodes = inputChildNodes.item(k).getChildNodes();
                                columnsSet = iterateDerivedoutputColumnNodeList(inputColumnsChildNodes, componentClassId, componentName, varibleMap);

                            }
                        }

                    }

                }
                componentColumnIdsForInput.put(componentRefId, columnsSet);
            }

        }

    }

    /**
     *
     * this method will iterate for input components and it will store input
     * component columns data in the componentColumnIdsForInput Hash map
     *
     * @param inputNode
     * @param componentRefId
     * @param componentClassId
     * @param componentName
     * @param componentConnections
     * @param executableName
     * @param actualComponentName
     * @return
     */
    public static String iterateForInputColumns(Node inputNode, String componentRefId, String componentClassId, String componentName, Map<String, String> componentConnections, String executableName, String actualComponentName) {

        if (componentName.contains("RC vLog_SourceRowsDeleted") || componentName.contains("Sort")) {
            int a = 0;
        }
        String returnedTotalInputColumns = "";
        Set<Map<String, String>> list = new HashSet<>();
        String inputColumnRefId = "";
        String inputColumnLineageId = "";
        String externalMetadataColumnId = "";
        String externalMetadataColumnName = "";
        String dataType = "";
        String length = "";
        String precision = "";
        String scale = "";
        try {
            if ("Microsoft.RowCount".equals(componentClassId)
                    || "Microsoft.Multicast".equals(componentClassId)
                    || "Microsoft.ConditionalSplit".equalsIgnoreCase(componentClassId)
                    || "Microsoft.ManagedComponentHost".equalsIgnoreCase(componentClassId)) {
                LinkedHashMap<String, String> inputColumnsMap = new LinkedHashMap<>();
                inputColumnRefId = inputNode.getAttributes().getNamedItem("refId").getTextContent();
                externalMetadataColumnName = inputNode.getAttributes().getNamedItem("name").getTextContent();

                if (externalMetadataColumnName != null && externalMetadataColumnName.toUpperCase().startsWith("INPUT")) {

                    try {

                        for (int i = 0; i < inputNode.getChildNodes().getLength(); i++) {
                            Node inputColumns = inputNode.getChildNodes().item(i);

                            if ("inputColumns".equals(inputColumns.getNodeName())) {
                                for (int j = 0; j < inputColumns.getChildNodes().getLength(); j++) {
                                    Node eachColumn = inputColumns.getChildNodes().item(j);
                                    if (!eachColumn.getNodeName().equals("inputColumn")) {
                                        continue;
                                    }
                                    inputColumnsMap = new LinkedHashMap<>();
                                    inputColumnRefId = eachColumn.getAttributes().getNamedItem("refId").getTextContent();
                                    inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                                    externalMetadataColumnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                    try {
                                        if ("Microsoft.ManagedComponentHost".equalsIgnoreCase(componentClassId)) {
                                            if (columnLineageIdLookup.get(inputColumnLineageId) != null) {
                                                inputColumnLineageId = columnLineageIdLookup.get(inputColumnLineageId);

                                            }

                                        }
                                    } catch (Exception e) {

                                    }

                                    String dataFlowName = "dataflow";
                                    try {
                                        String[] stringArr = inputColumnRefId.split("\\\\");
                                        dataFlowName = stringArr[stringArr.length - 1];
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    externalMetadataColumnName = externalMetadataColumnName.replace("@", "");
                                    inputColumnsMap.put("dataflowname", dataFlowName);
                                    inputColumnsMap.put("refid", inputColumnRefId);
                                    inputColumnsMap.put("lineageid", inputColumnLineageId);
                                    inputColumnsMap.put("componentClassId", componentClassId);
                                    inputColumnsMap.put("componentName", componentName);
                                    inputColumnsMap.put("columnName", externalMetadataColumnName);
                                    inputColumnsMap.put("dataType", dataType);
                                    inputColumnsMap.put("length", length);
                                    inputColumnsMap.put("precision", precision);
                                    inputColumnsMap.put("scale", scale);

                                    list.add(inputColumnsMap);
                                }
                                if (!StringUtils.isBlank(externalMetadataColumnName)) {

                                    if (!externalMetadataColumnName.startsWith("[") && !externalMetadataColumnName.endsWith("]")) {
                                        returnedTotalInputColumns = returnedTotalInputColumns + "[" + externalMetadataColumnName + "]" + ",";
                                    } else {
                                        returnedTotalInputColumns = returnedTotalInputColumns + externalMetadataColumnName + ",";
                                    }

                                }

                            }

                        }
                        if (list.size() <= 0) {
                            list = componentColumnIdsForOutput.get(componentConnections.get(inputColumnRefId));
                            list = DataFlowComponentUtil.updateDataFlowNameInColumnSet(list, componentName, componentClassId);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                    }
                } else {

//                    inputColumnLineageId = inputColumnRefId;
                    try {

                        for (int i = 0; i < inputNode.getChildNodes().getLength(); i++) {
                            Node inputColumns = inputNode.getChildNodes().item(i);

                            if (inputColumns.getNodeName().equals("inputColumns")) {
                                for (int j = 0; j < inputColumns.getChildNodes().getLength(); j++) {
                                    Node eachColumn = inputColumns.getChildNodes().item(j);
                                    if (!eachColumn.getNodeName().equals("inputColumn")) {
                                        continue;
                                    }
                                    inputColumnsMap = new LinkedHashMap<>();
                                    inputColumnRefId = eachColumn.getAttributes().getNamedItem("refId").getTextContent();
                                    inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                                    externalMetadataColumnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                    try {
                                        if ("Microsoft.ManagedComponentHost".equalsIgnoreCase(componentClassId)) {
                                            if (columnLineageIdLookup.get(inputColumnLineageId) != null) {
                                                inputColumnLineageId = columnLineageIdLookup.get(inputColumnLineageId);

                                            }

                                        }
                                    } catch (Exception e) {

                                    }

                                    String dataFlowName = "dataflow";
                                    try {
                                        String[] stringArr = inputColumnRefId.split("\\\\");
                                        dataFlowName = stringArr[stringArr.length - 1];
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    externalMetadataColumnName = externalMetadataColumnName.replace("@", "");
                                    inputColumnsMap.put("dataflowname", dataFlowName);
                                    inputColumnsMap.put("refid", inputColumnRefId);
                                    inputColumnsMap.put("lineageid", inputColumnLineageId);
                                    inputColumnsMap.put("componentClassId", componentClassId);
                                    inputColumnsMap.put("componentName", componentName);
                                    inputColumnsMap.put("columnName", externalMetadataColumnName);
                                    inputColumnsMap.put("dataType", dataType);
                                    inputColumnsMap.put("length", length);
                                    inputColumnsMap.put("precision", precision);
                                    inputColumnsMap.put("scale", scale);

                                    list.add(inputColumnsMap);
                                    if (!StringUtils.isBlank(externalMetadataColumnName)) {
                                        if (!externalMetadataColumnName.startsWith("[") && !externalMetadataColumnName.endsWith("]")) {
                                            returnedTotalInputColumns = returnedTotalInputColumns + "[" + externalMetadataColumnName + "]" + ",";
                                        } else {
                                            returnedTotalInputColumns = returnedTotalInputColumns + externalMetadataColumnName + ",";
                                        }

                                    }

                                }
                            }

                        }
                        if (list.size() <= 0) {
                            list = componentColumnIdsForOutput.get(componentConnections.get(inputColumnRefId));
                            list = DataFlowComponentUtil.updateDataFlowNameInColumnSet(list, componentName, componentClassId);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                    }

                }
            } else {
                String inputColumnName = "";
                for (int i = 0; i < inputNode.getChildNodes().getLength(); i++) {
                    Node inputColumns = inputNode.getChildNodes().item(i);
                    if (inputColumns.getNodeName().equals("inputColumns")) {
                        for (int j = 0; j < inputColumns.getChildNodes().getLength(); j++) {
                            LinkedHashMap<String, String> inputColumnsMap = new LinkedHashMap<>();
                            Node eachColumn = inputColumns.getChildNodes().item(j);
                            if (!eachColumn.getNodeName().equals("inputColumn")) {
                                continue;
                            }
                            inputColumnName = "";
                            inputColumnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                            inputColumnRefId = eachColumn.getAttributes().getNamedItem("refId").getTextContent();
                            inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                            if (componentClassId.equalsIgnoreCase("Microsoft.MergeJoin")) {
                                columnLineageIdLookup.put(inputColumnRefId, inputColumnLineageId);
                            }
                            if (componentClassId.equalsIgnoreCase("Microsoft.UnionAll") || componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {

                                for (int k = 0; k < eachColumn.getChildNodes().getLength(); k++) {
                                    Node properties = eachColumn.getChildNodes().item(k);
                                    for (int l = 0; l < properties.getChildNodes().getLength(); l++) {
                                        Node eachProperty = properties.getChildNodes().item(l);
                                        if (eachProperty.getNodeName().equals("property")) {
                                            if (componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {
                                                inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                                                String bussinesRule = eachProperty.getTextContent();
                                                inputColumnsMap.put("bussinesRule", bussinesRule);
                                                break;
                                            } else {
                                                inputColumnRefId = eachProperty.getTextContent().split("\\{")[1].split("\\}")[0];
                                                break;
                                            }
                                        }
                                    }
                                }

                            } else {
                                inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                            }

                            if (componentClassId.equalsIgnoreCase("Microsoft.Sort")) {
                                for (int k = 0; k < eachColumn.getChildNodes().getLength(); k++) {
                                    Node properties = eachColumn.getChildNodes().item(k);
                                    for (int l = 0; l < properties.getChildNodes().getLength(); l++) {
                                        Node eachProperty = properties.getChildNodes().item(l);
                                        if (eachProperty.getNodeName().equals("property")) {

                                            String namedString = "";
                                            try {
                                                namedString = eachProperty.getAttributes().getNamedItem("name").getTextContent();
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            String sortPositionOrderString = "";
                                            String bussinesRule = "";
                                            if (namedString.equalsIgnoreCase("NewSortKeyPosition")) {
                                                sortPositionOrderString = eachProperty.getTextContent();

                                                int sortPositionOrder = 0;

                                                sortPositionOrder = BoxingAndUnBoxingWrapper.convertStringToInteger(sortPositionOrderString);

                                                if (sortPositionOrder > 0) {
                                                    bussinesRule = "ascending";
                                                } else if (sortPositionOrder < 0) {
                                                    bussinesRule = "dscending";
                                                }
                                                inputColumnsMap.put("bussinesRule", bussinesRule);
                                                break;
                                            }

                                        }
                                    }
                                }
                            }

                            if (eachColumn.getAttributes().getNamedItem("externalMetadataColumnId") != null) {
                                //target input columns
                                externalMetadataColumnId = eachColumn.getAttributes().getNamedItem("externalMetadataColumnId").getTextContent();
                                for (int k = 0; k < inputNode.getChildNodes().getLength(); k++) {
                                    Node inputColumnsForTarget = inputNode.getChildNodes().item(k);
                                    if (inputColumnsForTarget.getNodeName().equals("externalMetadataColumns")) {
                                        for (int l = 0; l < inputColumnsForTarget.getChildNodes().getLength(); l++) {
                                            Node eachexternalMetaDataColumn = inputColumnsForTarget.getChildNodes().item(l);
                                            if (!eachexternalMetaDataColumn.getNodeName().equals("externalMetadataColumn")) {
                                                continue;
                                            }
                                            if (!externalMetadataColumnId.equalsIgnoreCase(eachexternalMetaDataColumn.getAttributes().getNamedItem("refId").getTextContent())) {
                                                continue;
                                            }
                                            if (externalMetadataColumnId.equalsIgnoreCase(eachexternalMetaDataColumn.getAttributes().getNamedItem("refId").getTextContent())) {
                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("name") != null) {
                                                    externalMetadataColumnName = eachexternalMetaDataColumn.getAttributes().getNamedItem("name").getTextContent();
                                                } else if (eachexternalMetaDataColumn.getAttributes().getNamedItem("cachedName") != null) {
                                                    externalMetadataColumnName = eachexternalMetaDataColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                                } else {
                                                    externalMetadataColumnName = "NOT_DEFINED";
                                                }
                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("dataType") != null) {
                                                    dataType = eachexternalMetaDataColumn.getAttributes().getNamedItem("dataType").getTextContent();
                                                } else {
                                                    dataType = "NOT_DEFINED";
                                                }
                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("length") != null) {
                                                    length = eachexternalMetaDataColumn.getAttributes().getNamedItem("length").getTextContent();
                                                } else {
                                                    length = "NOT_DEFINED";
                                                }
                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("precision") != null) {
                                                    precision = eachexternalMetaDataColumn.getAttributes().getNamedItem("precision").getTextContent();
                                                } else {
                                                    precision = "NOT_DEFINED";
                                                }
                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("scale") != null) {
                                                    scale = eachexternalMetaDataColumn.getAttributes().getNamedItem("scale").getTextContent();
                                                } else {
                                                    scale = "NOT_DEFINED";
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                            } else if //other input columns (other than target)
                                    (componentClassId.equalsIgnoreCase("Microsoft.UnionAll") || componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn") || componentClassId.equalsIgnoreCase("Microsoft.Lookup") || componentClassId.equalsIgnoreCase("Microsoft.ConditionalSplit")) {
                                if (componentClassId.equalsIgnoreCase("Microsoft.Lookup")) {
                                    // For lookup component LookupOn(columnName)  exists in properties>property Tag
                                    NodeList propertiesForLookup = eachColumn.getChildNodes();
                                    for (int k = 0; k < propertiesForLookup.getLength(); k++) {
                                        NodeList propertyNode = propertiesForLookup.item(k).getChildNodes();
                                        for (int l = 0; l < propertyNode.getLength(); l++) {
                                            if (propertyNode.item(l).getNodeName().equals("property")) {
                                                if (propertyNode.item(l).getAttributes().getNamedItem("name").getTextContent().equalsIgnoreCase("JoinToReferenceColumn")) {
                                                    externalMetadataColumnName = propertyNode.item(l).getTextContent();
                                                    dataType = propertyNode.item(l).getAttributes().getNamedItem("dataType").getTextContent();
                                                }

                                            }

                                        }
                                    }
                                } else {
                                    if (eachColumn.getAttributes().getNamedItem("cachedName") != null) {
                                        externalMetadataColumnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                        if (externalMetadataColumnName.contains("Error")) {
                                            continue;
                                        }
                                    } else {
                                        externalMetadataColumnName = "NOT_DEFINED";
                                    }
                                    if (eachColumn.getAttributes().getNamedItem("cachedDataType") != null) {
                                        dataType = eachColumn.getAttributes().getNamedItem("cachedDataType").getTextContent();
                                    } else {
                                        dataType = "NOT_DEFINED";
                                    }
                                }

                                if (eachColumn.getAttributes().getNamedItem("length") != null) {
                                    length = eachColumn.getAttributes().getNamedItem("length").getTextContent();
                                } else {
                                    length = "NOT_DEFINED";
                                }
                                if (eachColumn.getAttributes().getNamedItem("precision") != null) {
                                    precision = eachColumn.getAttributes().getNamedItem("precision").getTextContent();
                                } else {
                                    precision = "NOT_DEFINED";
                                }
                                if (eachColumn.getAttributes().getNamedItem("scale") != null) {
                                    scale = eachColumn.getAttributes().getNamedItem("scale").getTextContent();
                                } else {
                                    scale = "NOT_DEFINED";
                                }

                            } else {

                                if (eachColumn.getAttributes().getNamedItem("name") != null) {
                                    externalMetadataColumnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                                    if (externalMetadataColumnName.contains("Error")) {
                                        continue;
                                    } else if ("".equals(externalMetadataColumnName) && eachColumn.getAttributes().getNamedItem("cachedName") != null) {
                                        externalMetadataColumnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                    }

                                } else if (eachColumn.getAttributes().getNamedItem("cachedName") != null) {
                                    externalMetadataColumnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                } else {
                                    externalMetadataColumnName = "NOT_DEFINED";
                                }
                                if (eachColumn.getAttributes().getNamedItem("dataType") != null) {
                                    dataType = eachColumn.getAttributes().getNamedItem("dataType").getTextContent();
                                } else if (eachColumn.getAttributes().getNamedItem("cachedDataType") != null) {
                                    dataType = eachColumn.getAttributes().getNamedItem("cachedDataType").getTextContent();
                                } else {
                                    dataType = "NOT_DEFINED";
                                }
                                if (eachColumn.getAttributes().getNamedItem("length") != null) {
                                    length = eachColumn.getAttributes().getNamedItem("length").getTextContent();
                                } else if (eachColumn.getAttributes().getNamedItem("cachedLength") != null) {
                                    length = eachColumn.getAttributes().getNamedItem("cachedLength").getTextContent();
                                } else {
                                    length = "NOT_DEFINED";
                                }
                                if (eachColumn.getAttributes().getNamedItem("precision") != null) {
                                    precision = eachColumn.getAttributes().getNamedItem("precision").getTextContent();
                                } else {
                                    precision = "NOT_DEFINED";
                                }
                                if (eachColumn.getAttributes().getNamedItem("scale") != null) {
                                    scale = eachColumn.getAttributes().getNamedItem("scale").getTextContent();
                                } else {
                                    scale = "NOT_DEFINED";
                                }
                            }

                            if ("Microsoft.OLEDBCommand".equalsIgnoreCase(componentClassId)) {
                                inputColumnRefId = inputColumnRefId.split(".Columns")[0] + ".Columns[" + inputColumnName + "]";
//                                inputColumnRefId = inputColumnRefId.split("Columns")[0] + "Columns[" + inputColumnName + "]";
                                inputColumnLineageId = inputColumnLineageId.split(".Columns")[0] + ".Columns[" + inputColumnName + "]";
//                                inputColumnLineageId = inputColumnLineageId.split("Columns")[0] + "Columns[" + inputColumnName + "]";
                            } else {
                                inputColumnRefId = inputColumnRefId.split(".Columns")[0] + ".Columns[" + externalMetadataColumnName + "]";
//                                inputColumnRefId = inputColumnRefId.split("Columns")[0] + "Columns[" + externalMetadataColumnName + "]";
                                inputColumnLineageId = inputColumnLineageId.split(".Columns")[0] + ".Columns[" + externalMetadataColumnName + "]";
//                                inputColumnLineageId = inputColumnLineageId.split("Columns")[0] + "Columns[" + externalMetadataColumnName + "]";
                            }
                            inputColumnsMap.put("refid", inputColumnRefId);
                            externanColumnAndInputColumnMap.put(externalMetadataColumnName, inputColumnName);
                            dataConversionColumnMap.put(executableName + Delimiter.delimiter + externalMetadataColumnName, inputColumnName);
                            String dataFlowName = "dataflow";
                            try {
                                String[] stringArr = inputColumnRefId.split("\\\\");
                                dataFlowName = stringArr[stringArr.length - 1];
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            if (columnLineageIdLookup.get(inputColumnLineageId) != null) {
                                inputColumnLineageId = columnLineageIdLookup.get(inputColumnLineageId);
                            }
                            externalMetadataColumnName = externalMetadataColumnName.replace("@", "");
                            inputColumnsMap.put("dataflowname", dataFlowName);
                            inputColumnsMap.put("lineageid", inputColumnLineageId);
                            inputColumnsMap.put("componentClassId", componentClassId);
                            inputColumnsMap.put("componentName", componentName);

                            if ("Microsoft.OLEDBCommand".equalsIgnoreCase(componentClassId)) {
                                inputColumnsMap.put("columnName", inputColumnName);
                            } else {
                                inputColumnsMap.put("columnName", externalMetadataColumnName);

                            }
                            inputColumnsMap.put("dataType", dataType);
                            inputColumnsMap.put("length", length);
                            inputColumnsMap.put("precision", precision);
                            inputColumnsMap.put("scale", scale);

                            list.add(inputColumnsMap);
                            if (!StringUtils.isBlank(externalMetadataColumnName)) {
                                if (!externalMetadataColumnName.startsWith("[") && !externalMetadataColumnName.endsWith("]")) {
                                    returnedTotalInputColumns = returnedTotalInputColumns + "[" + externalMetadataColumnName + "]" + ",";
                                } else {
                                    returnedTotalInputColumns = returnedTotalInputColumns + externalMetadataColumnName + ",";
                                }

                            }

                        }
                    }
                }
            }
            componentColumnIdsForInput.put(componentRefId, list);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return returnedTotalInputColumns;
    }

    /**
     *
     * this method will iterate for input components and it will store input
     * component columns data in the componentColumnIdsForOutput Hash map
     *
     * @param outputNode
     * @param componentRefIdAndcomponentClassIdAndcomponentName
     * @param dataFlow
     * @param varibleMap
     * @param executableName
     * @param actualComponentName
     * @param componentConnections
     * @param outputName
     * @return
     */
    public static String iterateForOutputColumns(Node outputNode, String componentRefIdAndcomponentClassIdAndcomponentName, String dataFlow, Map<String, String> varibleMap, String executableName, String actualComponentName, Map<String, String> componentConnections, String outputName) {
        Set<Map<String, String>> columnSet = new HashSet<>();
        String outputColumnName = "";
        String keyForDataConversionMap = "";
        String returnTotalOutPutColumns = "";
        String componentClassId = "";
        String componentName = "";
        String componentRefId = "";

        try {
            if (componentRefIdAndcomponentClassIdAndcomponentName.contains(Delimiter.delimiter)) {

                String[] componentRefIdAndcomponentClassIdAndcomponentNameArray = componentRefIdAndcomponentClassIdAndcomponentName.split(Delimiter.delimiter);
                int length = componentRefIdAndcomponentClassIdAndcomponentNameArray.length;
                switch (length) {
                    case 3:
                        componentClassId = componentRefIdAndcomponentClassIdAndcomponentNameArray[0];
                        componentName = componentRefIdAndcomponentClassIdAndcomponentNameArray[1];
                        componentRefId = componentRefIdAndcomponentClassIdAndcomponentNameArray[2];
                        break;
                    case 2:
                        componentClassId = componentRefIdAndcomponentClassIdAndcomponentNameArray[0];
                        componentName = componentRefIdAndcomponentClassIdAndcomponentNameArray[1];
                        break;
                    case 1:
                        componentClassId = componentRefIdAndcomponentClassIdAndcomponentNameArray[0];
                        break;
                    default:
                        break;

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if ("Microsoft.RowCount".equals(componentClassId) || "Microsoft.Multicast".equals(componentClassId)) {
            LinkedHashMap<String, String> outputColumnsMap = new LinkedHashMap<>();
            String outputColumnRefId = outputNode.getAttributes().getNamedItem("refId").getTextContent();
            String outputColumnLineageId = outputColumnRefId;
            outputColumnName = outputNode.getAttributes().getNamedItem("name").getTextContent();
            String datatype = "";
            String length = "";
            String precision = "";
            String scale = "";
            String bussinesRule = "";
            outputColumnName = outputColumnName.replace("@", "");
            outputColumnsMap.put("refid", outputColumnRefId);
            outputColumnsMap.put("lineageid", outputColumnLineageId);
            outputColumnsMap.put("componentClassId", componentClassId);
            outputColumnsMap.put("dataflowname", dataFlow);
            outputColumnsMap.put("componentName", componentName);
            outputColumnsMap.put("columnName", outputColumnName);
            outputColumnsMap.put("dataType", datatype);
            outputColumnsMap.put("length", length);
            outputColumnsMap.put("precision", precision);
            outputColumnsMap.put("scale", scale);
            outputColumnsMap.put("bussinesRule", bussinesRule);
            columnSet.add(outputColumnsMap);

            if (!outputColumnName.startsWith("[") && !outputColumnName.endsWith("]")) {
                returnTotalOutPutColumns = "[" + outputColumnName + "]";
            } else {
                returnTotalOutPutColumns = outputColumnName;
            }

        } else if (componentClassId.equalsIgnoreCase("Microsoft.ConditionalSplit")) {

            for (int i = 0; i < outputNode.getChildNodes().getLength(); i++) {
                Node outputColumns = outputNode.getChildNodes().item(i);
                if (outputColumns.getNodeName().equals("properties")) {
                    NodeList propertiesNodeList = outputColumns.getChildNodes();
                    for (int l = 0; l < propertiesNodeList.getLength(); l++) {
                        Node eachProperty = propertiesNodeList.item(l);
                        String outputColumnRefId = "";
                        String outputColumnLineageId = "";
                        String bussinesRule = "";
                        String datatype = "NOT_DEFINED";
                        String length = "NOT_DEFINED";
                        String precision = "NOT_DEFINED";
                        String scale = "NOT_DEFINED";

                        if (eachProperty.getNodeName().equals("property")) {

                            String namedString = "";
                            try {
                                namedString = eachProperty.getAttributes().getNamedItem("name").getTextContent();
                            } catch (Exception e) {

                            }
                            if (namedString.equalsIgnoreCase("FriendlyExpression")) {
                                bussinesRule = eachProperty.getTextContent();
                                try {
                                    if (bussinesRule.contains("@[") && bussinesRule.contains("::") && bussinesRule.contains("]")) {
                                        bussinesRule = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbolsForDerivedComponentBusinessRule(bussinesRule, varibleMap);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            if (namedString.equalsIgnoreCase("Expression")) {
                                outputColumnLineageId = eachProperty.getTextContent();
                                outputColumnLineageId = splitLineages(outputColumnLineageId);

                                String outputColumnLineageIdSplit[] = outputColumnLineageId.split(",");

                                for (String outputColumnLineage : outputColumnLineageIdSplit) {
                                    LinkedHashMap<String, String> outputColumnsMap = new LinkedHashMap<>();
                                    try {
                                        if (outputColumnLineage.split(".Columns").length > 1) {
//                                        if (outputColumnLineage.split("Columns").length > 1) {
                                            outputColumnName = outputColumnLineage.split(".Columns")[1].replace("[", "").replace("]", "").replace("}", "").replace(".", "");
//                                            outputColumnName = outputColumnLineage.split("Columns")[1].replace("[", "").replace("]", "").replace("}", "").replace(".", "");
                                            outputColumnRefId = componentRefId + ".Columns[" + outputColumnName + "]";
//                                            outputColumnRefId = componentRefId + "Columns[" + outputColumnName + "]";
                                            outputColumnName = outputColumnName.replace("@", "");
                                            outputColumnsMap.put("refid", outputColumnRefId);
                                            outputColumnsMap.put("lineageid", outputColumnLineage);
                                            outputColumnsMap.put("componentClassId", componentClassId);
                                            outputColumnsMap.put("dataflowname", dataFlow);
                                            outputColumnsMap.put("componentName", componentName);
                                            outputColumnsMap.put("columnName", outputColumnName);
                                            outputColumnsMap.put("dataType", datatype);
                                            outputColumnsMap.put("length", length);
                                            outputColumnsMap.put("precision", precision);
                                            outputColumnsMap.put("scale", scale);
                                            outputColumnsMap.put("bussinesRule", bussinesRule);
                                            columnSet.add(outputColumnsMap);
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }

                            }
                            if (!StringUtils.isBlank(bussinesRule)) {
                                String key = executableName + "~" + actualComponentName;
                                HashMap<String, String> innerMap = new HashMap<>();
                                innerMap.put(outputName, bussinesRule);
                                if (conditionalSplitBusinessRuleMap.get(key) == null) {
                                    conditionalSplitBusinessRuleMap.put(key, innerMap);
                                } else {
                                    Map<String, String> existedInnerMap = conditionalSplitBusinessRuleMap.get(key);
                                    existedInnerMap.putAll(innerMap);

                                    conditionalSplitBusinessRuleMap.put(key, existedInnerMap);
                                }

                            }

                        }

                    }
                }
            }

        } else {
            for (int i = 0; i < outputNode.getChildNodes().getLength(); i++) {
                Node outputColumns = outputNode.getChildNodes().item(i);
                if (outputColumns.getNodeName().equals("outputColumns")) {
                    for (int j = 0; j < outputColumns.getChildNodes().getLength(); j++) {
                        LinkedHashMap<String, String> outputColumnsMap = new LinkedHashMap<>();
                        Node eachColumn = outputColumns.getChildNodes().item(j);
                        if (!eachColumn.getNodeName().equals("outputColumn")) {
                            continue;
                        }
                        outputColumnName = "";
                        keyForDataConversionMap = "";
                        outputColumnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                        keyForDataConversionMap = outputColumnName;
                        if (outputColumnName.contains("Error")) {
                            continue;
                        }
                        String outputColumnRefId = eachColumn.getAttributes().getNamedItem("refId").getTextContent();
                        String outputColumnLineageId = "";
                        outputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent(); // need to remove
                        String bussinesRule = "";
                        String expression = "";

                        //For derived Column we take Lineage Id  from properties>property
//                        if (componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn") || componentClassId.equalsIgnoreCase("Microsoft.DataConvert") || componentClassId.equalsIgnoreCase("Microsoft.MergeJoin") || componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost") || componentClassId.equalsIgnoreCase("Microsoft.ConditionalSplit")) {
                        if (componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn") || componentClassId.equalsIgnoreCase("Microsoft.DataConvert") || componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost") || componentClassId.equalsIgnoreCase("Microsoft.ConditionalSplit")) {
                            try {
                                for (int k = 0; k < eachColumn.getChildNodes().getLength(); k++) {
                                    Node properties = eachColumn.getChildNodes().item(k);
                                    for (int l = 0; l < properties.getChildNodes().getLength(); l++) {
                                        Node eachProperty = properties.getChildNodes().item(l);
                                        if (eachProperty.getNodeName().equals("property")) {
                                            if (eachProperty.getTextContent().contains("{") && !componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {

                                                outputColumnLineageId = eachProperty.getTextContent();
                                                outputColumnLineageId = splitLineages(outputColumnLineageId);
                                                if (componentClassId.equalsIgnoreCase("Microsoft.DataConvert")) {
                                                    try {
                                                        outputColumnName = outputColumnLineageId.split(".Columns")[1].replace("[", "").replace("]", "").replace("}", "").replace(".", "");
//                                                        outputColumnName = outputColumnLineageId.split("Columns")[1].replace("[", "").replace("]", "").replace("}", "").replace(".", "");

                                                        dataConversionColumnMap.put(executableName + Delimiter.delimiter + actualComponentName + Delimiter.delimiter + keyForDataConversionMap, outputColumnName);
                                                        if (columnLineageIdLookup.get(outputColumnLineageId) != null) {
                                                            outputColumnLineageId = columnLineageIdLookup.get(outputColumnLineageId);
                                                        }
                                                        columnLineageIdLookup.put(outputColumnRefId, outputColumnLineageId);
                                                        outputColumnRefId = outputColumnRefId.split(".Columns")[0] + ".Columns[" + outputColumnName + "]";
//                                                        outputColumnRefId = outputColumnRefId.split("Columns")[0] + "Columns[" + outputColumnName + "]";

                                                    } catch (Exception e) {

                                                    }
                                                } //                                                    else if (componentClassId.equalsIgnoreCase("Microsoft.MergeJoin") || componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost")) {
                                                else if (componentClassId.equalsIgnoreCase("Microsoft.ManagedComponentHost")) {
                                                    try {
//                                                        outputColumnName = outputColumnLineageId.split("Columns")[1].replace("[", "").replace("]", "").replace("}", "").replace(".", "");
                                                        outputColumnName = outputColumnLineageId.split(".Columns")[1].replace("[", "").replace("]", "").replace("}", "").replace(".", "");
                                                        if (columnLineageIdLookup.get(outputColumnLineageId) != null) {
                                                            outputColumnLineageId = columnLineageIdLookup.get(outputColumnLineageId);
                                                        }
                                                        columnLineageIdLookup.put(outputColumnRefId, outputColumnLineageId);
//                                                        outputColumnRefId = outputColumnRefId.split("Columns")[0] + "Columns[" + outputColumnName + "]";
                                                        outputColumnRefId = outputColumnRefId.split(".Columns")[0] + ".Columns[" + outputColumnName + "]";

                                                    } catch (Exception e) {

                                                    }

                                                }

                                            }
                                            if ("Expression".equals(eachProperty.getAttributes().getNamedItem("name").getTextContent())) {
                                                expression = eachProperty.getTextContent();
                                            }
                                            if ("FriendlyExpression".equals(eachProperty.getAttributes().getNamedItem("name").getTextContent())) {
                                                bussinesRule = eachProperty.getTextContent();
                                                try {
                                                    if (bussinesRule.contains("@[") && bussinesRule.contains("::") && bussinesRule.contains("]")) {
                                                        bussinesRule = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbolsForDerivedComponentBusinessRule(bussinesRule, varibleMap);
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }
                                            } else {
                                                bussinesRule = "NOT_DEFINED";
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();

                            }
                        } else {
                            outputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                            bussinesRule = "NOT_DEFINED";
                        }
                        String datatype = "";
                        String length = "";
                        String precision = "";
                        String scale = "";
                        if (eachColumn.getAttributes().getNamedItem("dataType") != null) {
                            datatype = eachColumn.getAttributes().getNamedItem("dataType").getTextContent();
                        } else {
                            datatype = "NOT_DEFINED";
                        }
                        if (eachColumn.getAttributes().getNamedItem("length") != null) {
                            length = eachColumn.getAttributes().getNamedItem("length").getTextContent();
                        } else {
                            length = "NOT_DEFINED";
                        }
                        if (eachColumn.getAttributes().getNamedItem("precision") != null) {
                            precision = eachColumn.getAttributes().getNamedItem("precision").getTextContent();
                        } else {
                            precision = "NOT_DEFINED";
                        }
                        if (eachColumn.getAttributes().getNamedItem("scale") != null) {
                            scale = eachColumn.getAttributes().getNamedItem("scale").getTextContent();
                        } else {
                            scale = "NOT_DEFINED";
                        }
                        outputColumnName = outputColumnName.replace("@", "");
                        outputColumnsMap.put("refid", outputColumnRefId);
                        outputColumnsMap.put("dataflowname", dataFlow);
                        outputColumnsMap.put("lineageid", outputColumnLineageId);
                        outputColumnsMap.put("componentClassId", componentClassId);
                        outputColumnsMap.put("componentName", componentName);
                        outputColumnsMap.put("columnName", outputColumnName);
                        outputColumnsMap.put("dataType", datatype);
                        outputColumnsMap.put("length", length);
                        outputColumnsMap.put("precision", precision);
                        outputColumnsMap.put("scale", scale);
                        if (!componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn") || !expression.contains("#{")) {
                            outputColumnsMap.put("bussinesRule", bussinesRule);
                        }

                        columnSet.add(outputColumnsMap);
                        if (!StringUtils.isBlank(outputColumnName)) {
                            if (!outputColumnName.startsWith("[") && !outputColumnName.endsWith("]")) {
                                returnTotalOutPutColumns = returnTotalOutPutColumns + "[" + outputColumnName + "]" + ",";
                            } else {
                                returnTotalOutPutColumns = returnTotalOutPutColumns + outputColumnName + ",";
                            }

                        }

                    } //inner for loop
                } else {
                    if (columnSet.size() <= 0 && componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {
                        String inputColumnRefId = outputNode.getAttributes().getNamedItem("refId").getTextContent();
                        columnSet = componentColumnIdsForOutput.get(componentConnections.get(inputColumnRefId));
                        columnSet = DataFlowComponentUtil.updateDataFlowNameInColumnSet(columnSet, componentName, componentClassId);
                    }
                }

            }
        }

        componentColumnIdsForOutput.put(componentRefId, columnSet);

        return returnTotalOutPutColumns;

    }

    /**
     *
     * this method will iterate for input columns NodeList and it will return
     * the total input column set for the derived component
     *
     *
     * @param inputsChildNodes
     * @param componentClassId
     * @param componentName
     * @param varibleMap
     * @return inputColumnSet
     */
    public static Set<Map<String, String>> iterateDerivedInputColumnNodeList(NodeList inputsChildNodes, String componentClassId, String componentName, Map<String, String> varibleMap) {
        Set<Map<String, String>> inputColumnsSet = new HashSet<>();
        try {
            for (int j = 0; j < inputsChildNodes.getLength(); j++) {
                if ("input".equalsIgnoreCase(inputsChildNodes.item(j).getNodeName())) {
                    NodeList inputChildNodes = inputsChildNodes.item(j).getChildNodes();
                    for (int k = 0; k < inputChildNodes.getLength(); k++) {
                        if (inputsChildNodes.item(j).getAttributes().getNamedItem(DESCRIPTION) != null && inputsChildNodes.item(j).getAttributes().getNamedItem(DESCRIPTION).getTextContent().contains("Error")) {
                            continue;
                        }
                        if ("inputColumns".equalsIgnoreCase(inputChildNodes.item(k).getNodeName())) {
                            NodeList inputColumnsChildNodes = inputChildNodes.item(k).getChildNodes();
                            for (int l = 0; l < inputColumnsChildNodes.getLength(); l++) {
                                LinkedHashMap<String, String> inputColumnsMap = new LinkedHashMap<>();
                                if ("inputColumn".equalsIgnoreCase(inputColumnsChildNodes.item(l).getNodeName())) {
                                    if (inputColumnsChildNodes.item(l).getChildNodes().getLength() == 0) {
                                        continue;
                                    }
                                    boolean flag = true;
                                    String inputColumnRefId = "";
                                    String inputColumnLineageId = "";
                                    String columnName = "";
                                    String dataType = "";
                                    String length = "";
                                    String precision = "";
                                    String scale = "";
                                    String bussinessRule = "";
                                    inputColumnRefId = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("refId").getTextContent();
                                    inputColumnLineageId = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("lineageId").getTextContent();
                                    if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem(Constants.cachedName) != null) {
                                        columnName = inputColumnsChildNodes.item(l).getAttributes().getNamedItem(Constants.cachedName).getTextContent();
                                        dataType = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("cachedDataType").getTextContent();
                                        if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem(Constants.cachedLength) != null) {
                                            length = inputColumnsChildNodes.item(l).getAttributes().getNamedItem(Constants.cachedLength).getTextContent();
                                        } else {
                                            length = Constants.NOT_DEFINED;
                                        }
                                    } else {
                                        columnName = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("name").getTextContent();
                                        dataType = inputColumnsChildNodes.item(l).getAttributes().getNamedItem(Constants.dataType).getTextContent();
                                        if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem(Constants.length) != null) {
                                            length = inputColumnsChildNodes.item(l).getAttributes().getNamedItem(Constants.length).getTextContent();
                                        } else {
                                            length = Constants.NOT_DEFINED;
                                        }
                                    }
                                    NodeList inputColumnChildNodes = inputColumnsChildNodes.item(l).getChildNodes();
                                    for (int m = 0; m < inputColumnChildNodes.getLength(); m++) {
                                        if ("properties".equalsIgnoreCase(inputColumnChildNodes.item(m).getNodeName())) {
                                            NodeList propertiesChildNodes = inputColumnChildNodes.item(m).getChildNodes();
                                            for (int n = 0; n < propertiesChildNodes.getLength(); n++) {
                                                if ("property".equalsIgnoreCase(propertiesChildNodes.item(n).getNodeName())) {
                                                    if ("Expression".equalsIgnoreCase(propertiesChildNodes.item(n).getAttributes().getNamedItem("name").getTextContent())) {
                                                        if (propertiesChildNodes.item(n).getTextContent().contains("{")) {
                                                            inputColumnLineageId = propertiesChildNodes.item(n).getTextContent();
                                                            inputColumnLineageId = splitLineages(inputColumnLineageId);
                                                        } else {
                                                            flag = false;
                                                        }

                                                    }
                                                    if ("FriendlyExpression".equalsIgnoreCase(propertiesChildNodes.item(n).getAttributes().getNamedItem("name").getTextContent())) {
                                                        bussinessRule = propertiesChildNodes.item(n).getTextContent();

                                                        if (bussinessRule.contains("@[") && bussinessRule.contains("::") && bussinessRule.contains("]")) {
                                                            bussinessRule = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbolsForDerivedComponentBusinessRule(bussinessRule, varibleMap);
                                                        }

                                                    }

                                                }

                                            }
                                        }

                                    }
                                    if (flag || !bussinessRule.equals("")) {
                                        inputColumnsMap.put("refid", inputColumnRefId);
                                        inputColumnsMap.put("lineageid", inputColumnLineageId);
                                        String dataFlowName = "dataflow";

                                        String tempInputColumnRefId = inputColumnRefId;
                                        tempInputColumnRefId = FilenameUtils.normalizeNoEndSeparator(tempInputColumnRefId, flag);
                                        if (tempInputColumnRefId.contains("/")) {
                                            String[] stringArr = tempInputColumnRefId.split("/");
                                            dataFlowName = stringArr[stringArr.length - 1];
                                        }
                                        inputColumnsMap.put("dataflowname", dataFlowName);
                                        inputColumnsMap.put("componentClassId", componentClassId);
                                        inputColumnsMap.put("componentName", componentName);
                                        inputColumnsMap.put("columnName", columnName);
                                        inputColumnsMap.put("dataType", dataType);
                                        inputColumnsMap.put("length", length);
                                        inputColumnsMap.put("precision", precision);
                                        inputColumnsMap.put("scale", scale);
                                        inputColumnsMap.put("bussinesRule", bussinessRule);
                                        inputColumnsSet.add(inputColumnsMap);
                                    }
                                }

                            }

                        }
                    }

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return inputColumnsSet;

    }

    /**
     * this method will iterate for out columns NodeList and it will return the
     * total output column set for the derived component
     *
     *
     * @param inputColumnsChildNodes
     * @param componentClassId
     * @param componentName
     * @param varibleMap
     * @return output column set
     */
    public static Set<Map<String, String>> iterateDerivedoutputColumnNodeList(NodeList inputColumnsChildNodes, String componentClassId, String componentName, Map<String, String> varibleMap) {
        Set<Map<String, String>> outputColumnsSet = new HashSet<>();
        try {
            for (int l = 0; l < inputColumnsChildNodes.getLength(); l++) {
                LinkedHashMap<String, String> inputColumnsMap = new LinkedHashMap<>();
                if ("outputColumn".equalsIgnoreCase(inputColumnsChildNodes.item(l).getNodeName())) {
                    boolean flag = true;
                    String inputColumnRefId = "";
                    String inputColumnLineageId = "";
                    String columnName = "";
                    String dataType = "";
                    String length = "";
                    String precision = "";
                    String scale = "";
                    String bussinessRule = "";
                    if (inputColumnsChildNodes.item(l).getChildNodes().getLength() == 0) {
                        continue;
                    }

                    inputColumnRefId = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("refId").getTextContent();
                    if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("    ") != null) {
                        columnName = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("cachedName").getTextContent();
                        dataType = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("cachedDataType").getTextContent();
                        if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("cachedLength") != null) {
                            length = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("cachedLength").getTextContent();
                        } else {
                            length = "NOT_DEFINED";
                        }
                    } else {
                        columnName = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("name").getTextContent();
                        dataType = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("dataType").getTextContent();
                        if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("length") != null) {
                            length = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("length").getTextContent();
                        } else {
                            length = "NOT_DEFINED";
                        }

                    }

                    NodeList inputColumnChildNodes = inputColumnsChildNodes.item(l).getChildNodes();
                    for (int m = 0; m < inputColumnChildNodes.getLength(); m++) {
                        if ("properties".equalsIgnoreCase(inputColumnChildNodes.item(m).getNodeName())) {
                            NodeList propertiesChildNodes = inputColumnChildNodes.item(m).getChildNodes();
                            for (int n = 0; n < propertiesChildNodes.getLength(); n++) {
                                if ("property".equalsIgnoreCase(propertiesChildNodes.item(n).getNodeName())) {
                                    if ("Expression".equalsIgnoreCase(propertiesChildNodes.item(n).getAttributes().getNamedItem("name").getTextContent())) {
                                        if (propertiesChildNodes.item(n).getTextContent().contains("{")) {

                                            inputColumnLineageId = propertiesChildNodes.item(n).getTextContent();
                                            inputColumnLineageId = splitLineages(inputColumnLineageId);

                                        } else {
                                            flag = false;
                                        }

                                    }
                                    if ("FriendlyExpression".equalsIgnoreCase(propertiesChildNodes.item(n).getAttributes().getNamedItem("name").getTextContent())) {
                                        bussinessRule = propertiesChildNodes.item(n).getTextContent();
                                        try {
                                            if (bussinessRule.contains("@[") && bussinessRule.contains("::") && bussinessRule.contains("]")) {
                                                bussinessRule = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbolsForDerivedComponentBusinessRule(bussinessRule, varibleMap);
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }

                                }

                            }
                        }

                    }
                    if (flag) {
                        inputColumnsMap.put("refid", inputColumnRefId);
                        if (!StringUtils.isBlank(inputColumnLineageId)) {
                            inputColumnLineageId = inputColumnLineageId + Delimiter.derivedColumnDelimiter;
                        }

                        inputColumnsMap.put("lineageid", inputColumnLineageId);
                        String dataFlowName = "dataflow";
                        try {
                            String[] stringArr = inputColumnRefId.split("\\\\");
                            dataFlowName = stringArr[stringArr.length - 1];
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        inputColumnsMap.put("dataflowname", dataFlowName);
                        inputColumnsMap.put("componentClassId", componentClassId);
                        inputColumnsMap.put("componentName", componentName);
                        inputColumnsMap.put("columnName", columnName);
                        inputColumnsMap.put("dataType", dataType);
                        inputColumnsMap.put("length", length);
                        inputColumnsMap.put("precision", precision);
                        inputColumnsMap.put("scale", scale);
                        inputColumnsMap.put("bussinesRule", bussinessRule);
                        outputColumnsSet.add(inputColumnsMap);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return outputColumnsSet;

    }

    /**
     * this method will split the lineage id from the property expression tag
     * and return back to the code.
     *
     * @param columnLineageId
     * @return lineageId(s)
     */
    public static String splitLineages(String columnLineageId) {
        String allLineages = "";
        try {
            int startIndex = columnLineageId.indexOf("#{");
            int endIndex = columnLineageId.indexOf("}");
            while (startIndex >= 0) {
                String lId = columnLineageId.substring(startIndex, endIndex);
                lId = lId.substring(2);
                allLineages = "".equals(allLineages) ? lId : allLineages + "," + lId;
                startIndex = columnLineageId.indexOf("#{", endIndex);
                endIndex = columnLineageId.indexOf("}", startIndex);
            }
        } catch (Exception e) {

        }
        return allLineages;
    }

}
