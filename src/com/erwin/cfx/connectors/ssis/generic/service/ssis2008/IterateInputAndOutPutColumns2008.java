/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2008;

import com.erwin.cfx.connectors.ssis.generic.util.DataFlowComponentUtil;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author THarika
 */
public class IterateInputAndOutPutColumns2008 {

    public static HashMap<String, String> dataConversionColumnMap = new HashMap<>();
    public static Map<String, Map<String, String>> conditionalSplitBusinessRuleMap = new HashMap<>();

    public IterateInputAndOutPutColumns2008() {
    }

    /**
     * this method prepares a hash map containing all the details of input
     * column and returns a string appended with same input details
     *
     * @param input
     * @param componentRefId
     * @param componentClassId
     * @param componentName
     * @param componentConnections
     * @return
     */
    public static String iterateForInputColumns(Node input, String componentRefId, String componentClassId, String componentName, Map<String, String> componentConnections) {

        long startTime = System.currentTimeMillis();//iterateForInputColumns
        String returnedTotalInputColumns = "";

        Set<Map<String, String>> list = new HashSet<>();
        String inputColumnRefId = "";
        String inputColumnLineageId = "";
        String externalMetadataColumnId = "";
        String columnName = "";
        String dataType = "";
        String length = "";
        String precision = "";
        String scale = "";
        try {
            if ("Microsoft.RowCount".equals(componentClassId)
                    || "Microsoft.Multicast".equals(componentClassId)
                    || "Microsoft.ConditionalSplit".equalsIgnoreCase(componentClassId)
                    || "Microsoft.ManagedComponentHost".equalsIgnoreCase(componentClassId)) {
                Map<String, String> inputColumnsMap = new LinkedHashMap<>();
                inputColumnRefId = input.getAttributes().getNamedItem("id").getTextContent();
                columnName = input.getAttributes().getNamedItem("name").getTextContent();

                if (columnName != null && columnName.toUpperCase().startsWith("INPUT ")) {
                    try {

                        for (int i = 0; i < input.getChildNodes().getLength(); i++) {
                            Node inputColumns = input.getChildNodes().item(i);
                            if (inputColumns.getNodeName().equals("inputColumns")) {
                                for (int j = 0; j < inputColumns.getChildNodes().getLength(); j++) {
                                    Node eachColumn = inputColumns.getChildNodes().item(j);
                                    if (!eachColumn.getNodeName().equals("inputColumn")) {
                                        continue;
                                    }
                                    inputColumnsMap = new LinkedHashMap<>();
                                    inputColumnRefId = eachColumn.getAttributes().getNamedItem("id").getTextContent();
                                    inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                                    columnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();

                                    String dataFlowName = "dataflow";
                                    try {
                                        String[] stringArr = inputColumnRefId.split("\\\\");
                                        dataFlowName = stringArr[stringArr.length - 1];
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    inputColumnsMap.put("dataflowname", dataFlowName);
                                    inputColumnsMap.put("refid", inputColumnRefId);
                                    inputColumnsMap.put("lineageid", inputColumnLineageId);
                                    inputColumnsMap.put("componentClassId", componentClassId);
                                    inputColumnsMap.put("componentName", componentName);
                                    inputColumnsMap.put("columnName", columnName);
                                    inputColumnsMap.put("dataType", dataType);
                                    inputColumnsMap.put("length", length);
                                    inputColumnsMap.put("precision", precision);
                                    inputColumnsMap.put("scale", scale);

                                    String separator = "!@ERWIN@!";
                                    String refKey = inputColumnRefId;
                                    String columnValues = dataFlowName + separator + inputColumnRefId + separator + inputColumnLineageId
                                            + separator + componentClassId + separator + componentName + separator
                                            + columnName + separator + dataType + separator
                                            + length + separator + precision + separator + scale;

                                    PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.put(refKey, columnValues);
                                    if (StringUtils.isNotBlank(columnName)) {
                                        if (!columnName.startsWith("[") && !columnName.endsWith("]")) {
                                            returnedTotalInputColumns = returnedTotalInputColumns + "[" + columnName + "]" + ",";
                                        } else {
                                            returnedTotalInputColumns = returnedTotalInputColumns + columnName + ",";
                                        }

                                    }
                                    list.add(inputColumnsMap);
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {

                    for (int i = 0; i < input.getChildNodes().getLength(); i++) {
                        Node inputColumns = input.getChildNodes().item(i);
                        if (inputColumns.getNodeName().equals("inputColumns")) {
                            for (int j = 0; j < inputColumns.getChildNodes().getLength(); j++) {
                                Node eachColumn = inputColumns.getChildNodes().item(j);
                                if (!eachColumn.getNodeName().equals("inputColumn")) {
                                    continue;
                                }
                                inputColumnsMap = new LinkedHashMap<>();
                                inputColumnRefId = eachColumn.getAttributes().getNamedItem("id").getTextContent();
                                inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                                columnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();

                                String dataFlowName = "dataflow";
                                try {
                                    String[] stringArr = inputColumnRefId.split("\\\\");
                                    dataFlowName = stringArr[stringArr.length - 1];
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                inputColumnsMap.put("dataflowname", dataFlowName);
                                inputColumnsMap.put("refid", inputColumnRefId);
                                inputColumnsMap.put("lineageid", inputColumnLineageId);
                                inputColumnsMap.put("componentClassId", componentClassId);
                                inputColumnsMap.put("componentName", componentName);
                                inputColumnsMap.put("columnName", columnName);
                                inputColumnsMap.put("dataType", dataType);
                                inputColumnsMap.put("length", length);
                                inputColumnsMap.put("precision", precision);
                                inputColumnsMap.put("scale", scale);

                                if (!StringUtils.isBlank(columnName)) {
                                    if (!columnName.startsWith("[") && !columnName.endsWith("]")) {
                                        returnedTotalInputColumns = returnedTotalInputColumns + "[" + columnName + "]" + ",";
                                    } else {
                                        returnedTotalInputColumns = returnedTotalInputColumns + columnName + ",";
                                    }

                                }

                                String separator = "!@ERWIN@!";
                                String refKey = inputColumnRefId;
                                String columnValues = dataFlowName + separator + inputColumnRefId + separator + inputColumnLineageId
                                        + separator + componentClassId + separator + componentName + separator
                                        + columnName + separator + dataType + separator
                                        + length + separator + precision + separator + scale;

                                PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.put(refKey, columnValues);

                                list.add(inputColumnsMap);
                            }
                        }
                    }
                    if (list.size() <= 0) {
                        list = PrepareDataFlowLineage2008.componentColumnIdsForOutput.get(componentConnections.get(inputColumnRefId));
                        list = DataFlowComponentUtil.updateDataFlowNameInColumnSet(list, componentName, componentClassId);
                    }
                }
            } else {

                for (int i = 0; i < input.getChildNodes().getLength(); i++) {
                    Node inputColumns = input.getChildNodes().item(i);
                    if (inputColumns.getNodeName().equals("inputColumns")) {
                        for (int j = 0; j < inputColumns.getChildNodes().getLength(); j++) {
                            LinkedHashMap<String, String> inputColumnsMap = new LinkedHashMap<String, String>();
                            Node eachColumn = inputColumns.getChildNodes().item(j);
                            if (!eachColumn.getNodeName().equals("inputColumn")) {
                                continue;
                            }
                            inputColumnRefId = eachColumn.getAttributes().getNamedItem("id").getTextContent();
                            inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                            columnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                            if (componentClassId.equalsIgnoreCase("Microsoft.UnionAll") || componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {
                                for (int k = 0; k < eachColumn.getChildNodes().getLength(); k++) {
                                    Node properties = eachColumn.getChildNodes().item(k);
                                    for (int l = 0; l < properties.getChildNodes().getLength(); l++) {
                                        Node eachProperty = properties.getChildNodes().item(l);
                                        try {
                                            if (eachProperty.getNodeName().equals("property")) {
                                                String propertyData = eachProperty.getTextContent();
                                                if (StringUtils.isNotBlank(propertyData) && propertyData.split("\\{").length >= 2 && propertyData.split("\\{")[1].split("\\}").length >= 1) {
                                                    inputColumnRefId = propertyData.split("\\{")[1].split("\\}")[0];
                                                    if (StringUtils.isNotBlank(inputColumnRefId) && inputColumnRefId.endsWith(")")) {
                                                        inputColumnRefId = inputColumnRefId.substring(0, inputColumnRefId.lastIndexOf(")"));
                                                    }
                                                    break;
                                                }

                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }

                            } else {
                                inputColumnLineageId = eachColumn.getAttributes().getNamedItem("lineageId").getTextContent();
                                columnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                            }
                            if ("Microsoft.DataConvert".equalsIgnoreCase(componentClassId) && StringUtils.isBlank(columnName)) {
                                if (dataConversionColumnMap.get(inputColumnLineageId) != null) {
                                    columnName = dataConversionColumnMap.get(inputColumnLineageId);
                                }
                            }
                            if (eachColumn.getAttributes().getNamedItem("externalMetadataColumnId") != null) {
                                //target input columns
                                externalMetadataColumnId = eachColumn.getAttributes().getNamedItem("externalMetadataColumnId").getTextContent();
                                for (int k = 0; k < input.getChildNodes().getLength(); k++) {
                                    Node inputColumnsForTarget = input.getChildNodes().item(k);
                                    if (inputColumnsForTarget.getNodeName().equals("externalMetadataColumns")) {
                                        for (int l = 0; l < inputColumnsForTarget.getChildNodes().getLength(); l++) {
                                            Node eachexternalMetaDataColumn = inputColumnsForTarget.getChildNodes().item(l);
                                            if (!eachexternalMetaDataColumn.getNodeName().equals("externalMetadataColumn")) {
                                                continue;
                                            }
                                            if (!externalMetadataColumnId.equalsIgnoreCase(eachexternalMetaDataColumn.getAttributes().getNamedItem("id").getTextContent())) {
                                                continue;
                                            }
                                            if (externalMetadataColumnId.equalsIgnoreCase(eachexternalMetaDataColumn.getAttributes().getNamedItem("id").getTextContent())) {
                                                Function<String, String> function = (StringLiteral) -> {
                                                    String value = "";
                                                    if (eachexternalMetaDataColumn.getAttributes().getNamedItem(StringLiteral) != null) {
                                                        value = eachexternalMetaDataColumn.getAttributes().getNamedItem(StringLiteral).getTextContent();
                                                    } else {
                                                        value = "NOT_DEFINED";
                                                    }
                                                    return value;
                                                };
                                                columnName = function.apply("name");
                                                if (columnName.equalsIgnoreCase("NOT_DEFINED")) {
                                                    columnName = function.apply("cachedName");
                                                }
                                                dataType = function.apply("dataType");
                                                length = function.apply("length");
                                                precision = function.apply("precision");
                                                scale = function.apply("scale");

//                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("name") != null) {
//                                                    columnName = eachexternalMetaDataColumn.getAttributes().getNamedItem("name").getTextContent();
//                                                } else if (eachexternalMetaDataColumn.getAttributes().getNamedItem("cachedName") != null) {
//                                                    columnName = eachexternalMetaDataColumn.getAttributes().getNamedItem("cachedName").getTextContent();
//                                                } else {
//                                                    columnName = "NOT_DEFINED";
//                                                }
//                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("dataType") != null) {
//                                                    dataType = eachexternalMetaDataColumn.getAttributes().getNamedItem("dataType").getTextContent();
//                                                } else {
//                                                    dataType = "NOT_DEFINED";
//                                                }
//                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("length") != null) {
//                                                    length = eachexternalMetaDataColumn.getAttributes().getNamedItem("length").getTextContent();
//                                                } else {
//                                                    length = "NOT_DEFINED";
//                                                }
//                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("precision") != null) {
//                                                    precision = eachexternalMetaDataColumn.getAttributes().getNamedItem("precision").getTextContent();
//                                                } else {
//                                                    precision = "NOT_DEFINED";
//                                                }
//                                                if (eachexternalMetaDataColumn.getAttributes().getNamedItem("scale") != null) {
//                                                    scale = eachexternalMetaDataColumn.getAttributes().getNamedItem("scale").getTextContent();
//                                                } else {
//                                                    scale = "NOT_DEFINED";
//                                                }
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
                                                    columnName = propertyNode.item(l).getTextContent();
                                                    dataType = propertyNode.item(l).getAttributes().getNamedItem("dataType").getTextContent();
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    if (eachColumn.getAttributes().getNamedItem("name") != null) {
                                        columnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                                        if (columnName.contains("Error")) {
                                            continue;
                                        }
                                    } else {
                                        columnName = "NOT_DEFINED";
                                    }
                                    if (eachColumn.getAttributes().getNamedItem("dataType") != null) {
                                        dataType = eachColumn.getAttributes().getNamedItem("dataType").getTextContent();
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
                                    columnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                                    if (columnName.contains("Error")) {
                                        continue;
                                    } else if ("".equals(columnName) && eachColumn.getAttributes().getNamedItem("cachedName") != null) {
                                        columnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                    }

                                } else if (eachColumn.getAttributes().getNamedItem("cachedName") != null) {
                                    columnName = eachColumn.getAttributes().getNamedItem("cachedName").getTextContent();
                                } else {
                                    columnName = "NOT_DEFINED";
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

                            inputColumnsMap.put("refid", inputColumnRefId);
                            String dataFlowName = "dataflow";
                            try {
                                String[] stringArr = inputColumnRefId.split("\\\\");
                                dataFlowName = stringArr[stringArr.length - 1];
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            inputColumnsMap.put("dataflowname", dataFlowName);
                            inputColumnsMap.put("lineageid", inputColumnLineageId);
                            inputColumnsMap.put("componentClassId", componentClassId);
                            inputColumnsMap.put("componentName", componentName);
                            inputColumnsMap.put("columnName", columnName);
                            inputColumnsMap.put("dataType", dataType);
                            inputColumnsMap.put("length", length);
                            inputColumnsMap.put("precision", precision);
                            inputColumnsMap.put("scale", scale);

                            if (!StringUtils.isBlank(columnName)) {
                                if (!columnName.startsWith("[") && !columnName.endsWith("]")) {
                                    returnedTotalInputColumns = returnedTotalInputColumns + "[" + columnName + "]" + ",";
                                } else {
                                    returnedTotalInputColumns = returnedTotalInputColumns + columnName + ",";
                                }

                            }
                            String separator = "!@ERWIN@!";
                            String refKey = inputColumnRefId;
                            String columnValues = dataFlowName + separator + inputColumnRefId + separator + inputColumnLineageId
                                    + separator + componentClassId + separator + componentName + separator
                                    + columnName + separator + dataType + separator
                                    + length + separator + precision + separator + scale;

                            PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.put(refKey, columnValues);

                            list.add(inputColumnsMap);
                        }
                    }
                }
            }
            PrepareDataFlowLineage2008.componentColumnIdsForInput.put(componentRefId, list);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();//iterateForInputColumns
        System.out.println("time difference----in iterateForInputColumns method::" + (endTime - startTime));
        return returnedTotalInputColumns;
    }

    /**
     * this method prepares a hash map containing all the details of output
     * column and returns a string appended with same output details
     *
     * @param output
     * @param componentRefId
     * @param componentClassId
     * @param componentName
     * @param dataFlow
     * @param executableName
     * @param actualComponentName
     * @param packageLevelVariableMap
     * @param outputName
     * @return
     */
    public static String iterateForOutputColumns(Node output, String componentRefId, String componentClassId, String componentName, String dataFlow, String executableName, String actualComponentName, Map<String, String> packageLevelVariableMap, String outputName) {
        if (componentName.toLowerCase().contains("ecommunicationselection")) {
            int a = 0;
        }
        long startTime = System.currentTimeMillis();//iterateForOutputColumns;
        Set<Map<String, String>> list = new HashSet<>();
        String returnTotalOutPutColumns = "";

        if ("Microsoft.RowCount".equals(componentClassId) || "Microsoft.Multicast".equals(componentClassId)) {
            LinkedHashMap<String, String> outputColumnsMap = new LinkedHashMap<String, String>();
            String outputColumnRefId = output.getAttributes().getNamedItem("id").getTextContent();
            String outputColumnLineageId = outputColumnRefId;
            String outputColumnName = output.getAttributes().getNamedItem("name").getTextContent();
            String datatype = "";
            String length = "";
            String precision = "";
            String scale = "";
            String bussinesRule = "";

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
            outputColumnsMap.put("bussinessRule", bussinesRule);
            String separator = "!@ERWIN@!";
            String refKey = outputColumnRefId;
            String columnValues = dataFlow + separator + outputColumnRefId + separator + outputColumnLineageId
                    + separator + componentClassId + separator + componentName + separator
                    + outputColumnName + separator + datatype + separator
                    + length + separator + precision + separator + scale + separator + bussinesRule;
            PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.put(refKey, columnValues);

            if (!StringUtils.isBlank(outputColumnName)) {
                if (!outputColumnName.startsWith("[") && !outputColumnName.endsWith("]")) {
                    returnTotalOutPutColumns = returnTotalOutPutColumns + "[" + outputColumnName + "]" + ",";
                } else {
                    returnTotalOutPutColumns = returnTotalOutPutColumns + outputColumnName + ",";
                }
            }
            list.add(outputColumnsMap);

        } else if (componentClassId.equalsIgnoreCase("Microsoft.ConditionalSplit")) {

            for (int i = 0; i < output.getChildNodes().getLength(); i++) {
                Node outputColumns = output.getChildNodes().item(i);
                if (outputColumns.getNodeName().equals("properties")) {
                    NodeList propertiesNodeList = outputColumns.getChildNodes();
                    for (int l = 0; l < propertiesNodeList.getLength(); l++) {
                        Node eachProperty = propertiesNodeList.item(l);
                        String outputColumnRefId = "";
                        String outputColumnLineageId = "";
                        String businessRule = "";
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
                                businessRule = eachProperty.getTextContent();
                                try {
                                    if (businessRule.contains("@[") && businessRule.contains("::") && businessRule.contains("]")) {
                                        businessRule = DynamicVaribleValueReplacement.getActualNameWithOutSpecialSymbolsForDerivedComponentBusinessRule(businessRule, packageLevelVariableMap);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            if (namedString.equalsIgnoreCase("Expression")) {
                                outputColumnLineageId = eachProperty.getTextContent();
                                outputColumnLineageId = DataFlowSupport.splitLineages(outputColumnLineageId);

                                String outputColumnLineageIdSplit[] = outputColumnLineageId.split(",");

                                for (String outputColumnLineage : outputColumnLineageIdSplit) {
                                    LinkedHashMap<String, String> outputColumnsMap = new LinkedHashMap<>();
                                    try {
                                        if (outputColumnLineage.split(".Columns").length > 1) {
                                            outputColumnsMap.put("refid", outputColumnRefId);
                                            outputColumnsMap.put("lineageid", outputColumnLineage);
                                            outputColumnsMap.put("componentClassId", componentClassId);
                                            outputColumnsMap.put("dataflowname", dataFlow);
                                            outputColumnsMap.put("componentName", componentName);
                                            outputColumnsMap.put("columnName", "");
                                            outputColumnsMap.put("dataType", datatype);
                                            outputColumnsMap.put("length", length);
                                            outputColumnsMap.put("precision", precision);
                                            outputColumnsMap.put("scale", scale);
                                            outputColumnsMap.put("bussinessRule", businessRule);
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                            if (!StringUtils.isBlank(businessRule)) {
                                String key = executableName + "~" + actualComponentName;
                                HashMap<String, String> innerMap = new HashMap<>();
                                innerMap.put(outputName, businessRule);
                                if (conditionalSplitBusinessRuleMap.get(key) == null) {
                                    conditionalSplitBusinessRuleMap.put(key, innerMap);
                                } else {
                                    Map<String, String> oldMap = conditionalSplitBusinessRuleMap.get(key);
                                    oldMap.putAll(innerMap);
                                    conditionalSplitBusinessRuleMap.put(key, oldMap);
                                }
                            }
                        }

                    }
                }
            }
        } else {
            for (int i = 0; i < output.getChildNodes().getLength(); i++) {
                Node outputColumns = output.getChildNodes().item(i);
                if (outputColumns.getNodeName().equals("outputColumns")) {
                    for (int j = 0; j < outputColumns.getChildNodes().getLength(); j++) {
                        LinkedHashMap<String, String> outputColumnsMap = new LinkedHashMap<>();
                        Node eachColumn = outputColumns.getChildNodes().item(j);
                        if (!eachColumn.getNodeName().equals("outputColumn")) {
                            continue;
                        }
                        String outputColumnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                        if (outputColumnName.contains("Error")) {
                            continue;
                        }
                        String outputColumnRefId = eachColumn.getAttributes().getNamedItem("id").getTextContent();
                        String outputColumnLineageId = "";
                        String bussinesRule = "";
                        //For derived Column we take Lineage Id  from properties>property
                        if (componentClassId.equalsIgnoreCase("Microsoft.DerivedColumn")) {
                            for (int k = 0; k < eachColumn.getChildNodes().getLength(); k++) {
                                Node properties = eachColumn.getChildNodes().item(k);
                                for (int l = 0; l < properties.getChildNodes().getLength(); l++) {
                                    Node eachProperty = properties.getChildNodes().item(l);
                                    if (eachProperty.getNodeName().equals("property")) {
                                        if (eachProperty.getTextContent().contains("{")) {
                                            outputColumnLineageId = eachProperty.getTextContent();
                                            outputColumnLineageId = DataFlowSupport.splitLineages(outputColumnLineageId);
                                        }
                                        if ("FriendlyExpression".equals(eachProperty.getAttributes().getNamedItem("name").getTextContent())) {
                                            bussinesRule = eachProperty.getTextContent();
                                        } else {
                                            bussinesRule = "NOT_DEFINED";
                                        }
                                    }
                                }
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
                        outputColumnsMap.put("bussinessRule", bussinesRule);

                        if (!StringUtils.isBlank(outputColumnName)) {
                            if (!outputColumnName.startsWith("[") && !outputColumnName.endsWith("]")) {
                                returnTotalOutPutColumns = returnTotalOutPutColumns + "[" + outputColumnName + "]" + ",";
                            } else {
                                returnTotalOutPutColumns = returnTotalOutPutColumns + outputColumnName + ",";
                            }
                        }
                        String separator = "!@ERWIN@!";
                        String refKey = outputColumnRefId;
                        String columnValues = dataFlow + separator + outputColumnRefId + separator + outputColumnLineageId
                                + separator + componentClassId + separator + componentName + separator
                                + outputColumnName + separator + datatype + separator
                                + length + separator + precision + separator + scale;

                        PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.put(refKey, columnValues);

                        list.add(outputColumnsMap);
                    }
                }
            }
        }
        PrepareDataFlowLineage2008.componentColumnIdsForOutput.put(componentRefId, list);
        long endTime = System.currentTimeMillis();//iterateForOutputColumns;
        System.out.println("time difference----in iterateForOutputColumns method::" + (endTime - startTime));
        return returnTotalOutPutColumns;
    }

    /**
     * this method prepares a hash map containing component lineage id as key
     * and column name as value for columns present in data conversion component
     *
     * @param output
     */
    public static void prepareLineageIdAsKeyAndOutputColumnNameAsValueForDataConversion(Node output) {

        for (int i = 0; i < output.getChildNodes().getLength(); i++) {
            Node outputColumns = output.getChildNodes().item(i);
            String outputColumnName = "";
            String outputColumnLineageId = "";

            if (outputColumns.getNodeName().equals("outputColumns")) {
                outputColumnName = "";
                outputColumnLineageId = "";
                for (int j = 0; j < outputColumns.getChildNodes().getLength(); j++) {

                    Node eachColumn = outputColumns.getChildNodes().item(j);
                    if (!eachColumn.getNodeName().equals("outputColumn")) {
                        continue;
                    }
                    outputColumnName = eachColumn.getAttributes().getNamedItem("name").getTextContent();
                    if (outputColumnName.contains("Error")) {
                        continue;
                    }
                    for (int k = 0; k < eachColumn.getChildNodes().getLength(); k++) {
                        Node properties = eachColumn.getChildNodes().item(k);
                        for (int l = 0; l < properties.getChildNodes().getLength(); l++) {
                            Node eachProperty = properties.getChildNodes().item(l);
                            if (eachProperty.getNodeName().equals("property")) {
                                outputColumnLineageId = eachProperty.getTextContent();
                                break;
                            }
                        }
                    }
                    if (StringUtils.isNotBlank(outputColumnLineageId) && StringUtils.isNotBlank(outputColumnName)) {
                        dataConversionColumnMap.put(outputColumnLineageId, outputColumnName);
                    }
                }

            }
        }

    }

    /**
     * this method prepares a hash map containing key as component Id and value
     * as list of input/output column details for derived component
     *
     * @param input
     * @param componentRefId
     * @param componentClassId
     * @param componentName
     */
    public static void iterateForDerivedInputColumns(Node input, String componentRefId, String componentClassId, String componentName) {
        Node component = input.getParentNode().getParentNode();
        NodeList componentNodeList = component.getChildNodes();
        Set<Map<String, String>> list = new HashSet<>();
        try {
            for (int i = 0; i < componentNodeList.getLength(); i++) {
                if ("inputs".equalsIgnoreCase(componentNodeList.item(i).getNodeName())) {

                    NodeList inputsChildNodes = componentNodeList.item(i).getChildNodes();
                    for (int j = 0; j < inputsChildNodes.getLength(); j++) {
                        if ("input".equalsIgnoreCase(inputsChildNodes.item(j).getNodeName())) {
                            NodeList inputChildNodes = inputsChildNodes.item(j).getChildNodes();
                            for (int k = 0; k < inputChildNodes.getLength(); k++) {
                                if (inputsChildNodes.item(j).getAttributes().getNamedItem("description") != null) {
                                    if (inputsChildNodes.item(j).getAttributes().getNamedItem("description").getTextContent().contains("Error")) {
                                        continue;
                                    }
                                }
                                if ("inputColumns".equalsIgnoreCase(inputChildNodes.item(k).getNodeName())) {

                                    NodeList inputColumnsChildNodes = inputChildNodes.item(k).getChildNodes();
                                    for (int l = 0; l < inputColumnsChildNodes.getLength(); l++) {
                                        LinkedHashMap<String, String> inputColumnsMap = new LinkedHashMap<String, String>();
                                        try {
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
                                                if (inputColumnsChildNodes.item(l).getChildNodes().getLength() == 0) {
                                                    continue;
                                                }
                                                inputColumnRefId = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("id").getTextContent();
                                                inputColumnLineageId = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("lineageId").getTextContent();
                                                if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("name") != null) {
                                                    columnName = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("name").getTextContent();

                                                    if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("dataType") != null) {
                                                        dataType = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("dataType").getTextContent();
                                                    }

                                                    if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("length") != null) {
                                                        length = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("length").getTextContent();
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
                                                                        inputColumnLineageId = DataFlowSupport.splitLineages(inputColumnLineageId);
                                                                    } else {
                                                                        flag = false;
                                                                    }

                                                                }
                                                                if ("FriendlyExpression".equalsIgnoreCase(propertiesChildNodes.item(n).getAttributes().getNamedItem("name").getTextContent())) {
                                                                    bussinessRule = propertiesChildNodes.item(n).getTextContent();
                                                                }

                                                            }

                                                        }
                                                    }

                                                }
                                                if (flag || StringUtils.isNotBlank(bussinessRule)) {
                                                    inputColumnsMap.put("refid", inputColumnRefId);
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
                                                    inputColumnsMap.put("bussinessRule", bussinessRule);

                                                    String separator = "!@ERWIN@!";
                                                    String refKey = inputColumnRefId;
                                                    String columnValues = dataFlowName + separator + inputColumnRefId + separator + inputColumnLineageId
                                                            + separator + componentClassId + separator + componentName + separator
                                                            + columnName + separator + dataType + separator
                                                            + length + separator + precision + separator + scale;

                                                    PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.put(refKey, columnValues);
                                                    list.add(inputColumnsMap);
                                                }
                                            }

                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }

                        }

                    }
                    PrepareDataFlowLineage2008.componentColumnIdsForInput.put(componentRefId, list);
                }
// for outputs            

                if ("outputs".equalsIgnoreCase(componentNodeList.item(i).getNodeName())) {

                    NodeList inputsChildNodes = componentNodeList.item(i).getChildNodes();
                    for (int j = 0; j < inputsChildNodes.getLength(); j++) {
                        if ("output".equalsIgnoreCase(inputsChildNodes.item(j).getNodeName())) {
                            NodeList inputChildNodes = inputsChildNodes.item(j).getChildNodes();
                            for (int k = 0; k < inputChildNodes.getLength(); k++) {
                                if (inputsChildNodes.item(j).getAttributes().getNamedItem("description") != null) {
                                    if (inputsChildNodes.item(j).getAttributes().getNamedItem("description").getTextContent().contains("Error")) {
                                        continue;
                                    }
                                }

                                if ("outputColumns".equalsIgnoreCase(inputChildNodes.item(k).getNodeName())) {

                                    NodeList inputColumnsChildNodes = inputChildNodes.item(k).getChildNodes();
                                    for (int l = 0; l < inputColumnsChildNodes.getLength(); l++) {
                                        LinkedHashMap<String, String> inputColumnsMap = new LinkedHashMap<String, String>();
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

                                            inputColumnRefId = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("id").getTextContent();
                                            if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("    ") != null) {
                                                columnName = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("name").getTextContent();
                                                dataType = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("dataType").getTextContent();
                                                if (inputColumnsChildNodes.item(l).getAttributes().getNamedItem("length") != null) {
                                                    length = inputColumnsChildNodes.item(l).getAttributes().getNamedItem("length").getTextContent();
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
                                                                if (propertiesChildNodes.item(n).getTextContent().contains("#")) {

                                                                    inputColumnLineageId = propertiesChildNodes.item(n).getTextContent();
                                                                    inputColumnLineageId = DataFlowSupport.splitLineages(inputColumnLineageId);

                                                                } else {
                                                                    flag = false;
                                                                }

                                                            }
                                                            if ("FriendlyExpression".equalsIgnoreCase(propertiesChildNodes.item(n).getAttributes().getNamedItem("name").getTextContent())) {
                                                                bussinessRule = propertiesChildNodes.item(n).getTextContent();
                                                            }

                                                        }

                                                    }
                                                }

                                            }
                                            if (flag) {
                                                inputColumnsMap.put("refid", inputColumnRefId);
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

                                                String separator = "!@ERWIN@!";
                                                String refKey = inputColumnRefId;
                                                String columnValues = dataFlowName + separator + inputColumnRefId + separator + inputColumnLineageId
                                                        + separator + componentClassId + separator + componentName + separator
                                                        + columnName + separator + dataType + separator
                                                        + length + separator + precision + separator + scale;

                                                PrepareDataFlowLineage2008.columnRefIdAndTotalColumnDetailsMap.put(refKey, columnValues);
                                                list.add(inputColumnsMap);
                                            }
                                        }

                                    }

                                }
                            }

                        }

                    }
                    PrepareDataFlowLineage2008.componentColumnIdsForInput.put(componentRefId, list);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
