/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2014;

import com.erwin.cfx.connectors.ssis.generic.beans.ACPInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.beans.DataFlowBean;
import com.erwin.cfx.connectors.ssis.generic.beans.SSISInputParameterBean;
import com.erwin.cfx.connectors.ssis.generic.util.Constants;
import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import com.erwin.cfx.connectors.ssis.generic.util.MappingManagerUtilAutomation;
import com.erwin.cfx.connectors.ssis.generic.util.variablereplacement.DynamicVaribleValueReplacement;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 * @Date : 18-08-2021
 */
public class SSIS2014 {

    public static boolean normalControlFlowComponentExtendedPropertiesFlag = false;
    List<String> controlFlowObjectNameList = new ArrayList<>();
    HashSet<String> removeContrloFlowDuplicates = new HashSet<>();
    HashSet<String> controlFlowComponentNameSet = new HashSet<>();
    public static boolean missingControlFlowComponentExtendedPropertiesFlag = false;
    public Map<String, Object> globalMap = new HashMap<>();

    /**
     * clears all the global variables data
     */
    public void clearStaticAllocation() {
        normalControlFlowComponentExtendedPropertiesFlag = false;
        removeContrloFlowDuplicates = new HashSet<>();
        controlFlowObjectNameList = new ArrayList<>();
        controlFlowComponentNameSet = new HashSet<>();
        missingControlFlowComponentExtendedPropertiesFlag = false;
        globalMap = new HashMap<String, Object>();

    }

    /**
     * this method starts the processing of individual 2008 dtsx file
     *
     * @param inputParameterBean
     * @return Map Object
     */
    public Map<String, Object> startExecution(ACPInputParameterBean inputParameterBean) {
        globalMap = new HashMap<>();
        File inputFile = null;
        Document document = null;
        Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageNameMap
                = null;
        Map<String, String> parameterHashMap = null;
        Map<String, String> childLevelVaribleMap = null;
        boolean childLevelPackageFlag = false;
        Map<String, String> projectReferenceVariableMap = null;
        Map<String, String> connectionPropertiesHashMap = new HashMap<>();
        Map<String, String> ProjectLevelConnectionInfo = new HashMap<>();
        try {
            if (inputParameterBean != null) {
                parentComponentAndChildPackageNameMap
                        = inputParameterBean.getParentComponentAndChildPackageName();
                parameterHashMap = inputParameterBean.getParameterHashMap();
                childLevelVaribleMap = inputParameterBean.getChildLevelVaribleMap();
                childLevelPackageFlag = inputParameterBean.isChildLevelPackageFlag();
                inputFile = inputParameterBean.getInputFile();
                document = inputParameterBean.getDocument();
                projectReferenceVariableMap = inputParameterBean.getProjectLevelVaraiblesMap();
                connectionPropertiesHashMap = inputParameterBean.getConnectionPropertiesHashMap();
                ProjectLevelConnectionInfo = inputParameterBean.getProjectLevelConnectionInfo();
            } else {
                inputParameterBean = new ACPInputParameterBean();
            }

            String fileName = "";
            if (inputFile != null) {
                fileName = inputFile.getName();
            }
            if (fileName.contains(".")) {
                fileName = fileName.substring(0, fileName.lastIndexOf("."));
            }
            Parser2014XMLFile parser2014XMLFile = new Parser2014XMLFile();
            fileName = fileName.replaceAll(Constants.commonRegularExpression, "_");

            Map<String, String> userReferenceVariableMap = parser2014XMLFile.prepareVariablesMap(document);

            Map<String, String> packageReferenceVariableMap = parser2014XMLFile.preparePackageParametersVaraibles(document);

            Map<String, List<HashMap<String, String>>> singleCrontrolFlowLevelMapWithExcecutables = new HashMap<>();
            userReferenceVariableMap.put("PackageName", fileName);

            List<Map<String, String>> updatedPackageAndProjectList = null;
            updatedPackageAndProjectList = DynamicVaribleValueReplacement.replaceSpecialSymobalsFromPackageAndProjectNameAndMakeOnlyThierNamesAsValues(userReferenceVariableMap, projectReferenceVariableMap, childLevelVaribleMap, packageReferenceVariableMap);

            if (updatedPackageAndProjectList.size() >= 2) {
                userReferenceVariableMap = updatedPackageAndProjectList.get(0);
                projectReferenceVariableMap = updatedPackageAndProjectList.get(1);
            }

            XPath xPath = XPathFactory.newInstance().newXPath();
            Map<String, String> connectionsMap = parser2014XMLFile.prepareConnectionsMap(document, xPath, connectionPropertiesHashMap);
            if (connectionsMap.isEmpty() && connectionPropertiesHashMap != null && !connectionPropertiesHashMap.isEmpty()) {
                connectionsMap.putAll(connectionPropertiesHashMap);
            }

            if (ProjectLevelConnectionInfo != null) {
                connectionsMap.putAll(ProjectLevelConnectionInfo);
            }
            String allExecutablesXpath = "//Executables/Executable";
            NodeList allExecutablesNodeList = null;
            try {
                allExecutablesNodeList = (NodeList) xPath.compile(allExecutablesXpath).evaluate(document, XPathConstants.NODESET);
            } catch (Exception e) {
                e.printStackTrace();

            }
            Set<String> disabledComponentSet = new HashSet<>();
            if (allExecutablesNodeList != null) {
                disabledComponentSet = parser2014XMLFile.getDeactiveComponentList(allExecutablesNodeList);
            }

            String packageObjectName = "";
            try {
                String packagePath = "/Executable";
                NodeList packagePathNodeList = (NodeList) xPath.compile(packagePath).evaluate(document, XPathConstants.NODESET);
                for (int j = 0; j < packagePathNodeList.getLength(); j++) {
                    if (Constants.executableLiteral.equalsIgnoreCase(packagePathNodeList.item(j).getNodeName())) {
                        packageObjectName = packagePathNodeList.item(j).getAttributes().getNamedItem(Constants.objectNameLiteral).getTextContent();
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Map<String, List<HashMap<String, String>>> controlFlowLevelLineageMap = new HashMap<>();
            String controlFlowLevelPath = "//Executable/PrecedenceConstraints";
            NodeList controlFlowLevelNodeList = null;
            try {
                controlFlowLevelNodeList = (NodeList) xPath.compile(controlFlowLevelPath).evaluate(document, XPathConstants.NODESET);
            } catch (Exception e) {
                e.printStackTrace();
            }

            SSISInputParameterBean ssisInputParameterBean = new SSISInputParameterBean();
            ssisInputParameterBean.setInputParameterBean(inputParameterBean);
            ssisInputParameterBean.setUserReferenceVariablesMap(userReferenceVariableMap);
            ssisInputParameterBean.setConnectionsMap(connectionsMap);
            ssisInputParameterBean.setControlFlowLevelNodeList(controlFlowLevelNodeList);
            ssisInputParameterBean.setDtsxFileName(fileName);
            ssisInputParameterBean.setPackageObjectName(packageObjectName);
            ssisInputParameterBean.setParser2014XMLFile(parser2014XMLFile);
            ssisInputParameterBean.setDisabledComponentSet(disabledComponentSet);
            ssisInputParameterBean.setPackageReferenceVariablesMap(packageReferenceVariableMap);

            inputParameterBean.setProjectLevelVaraiblesMap(projectReferenceVariableMap);

            controlFlowLevelLineageMap = prepareControlFlowComponentLineageMap(ssisInputParameterBean);

            String controlFlowMissingCompExp = "//Executables/Executable";
            NodeList controlFlowMissingCompNodeList = null;
            try {
                controlFlowMissingCompNodeList = (NodeList) xPath.compile(controlFlowMissingCompExp).evaluate(document, XPathConstants.NODESET);
            } catch (Exception e) {
                e.printStackTrace();
            }

            controlFlowLevelLineageMap = prepareMissingComponentsLineage(controlFlowLevelLineageMap, controlFlowMissingCompNodeList, parentComponentAndChildPackageNameMap, parameterHashMap, ssisInputParameterBean);

            inputParameterBean.setParentComponentAndChildPackageName(parentComponentAndChildPackageNameMap);
            inputParameterBean.setParameterHashMap(parameterHashMap);
            boolean controlFlowMapFlag = Boolean.parseBoolean(inputParameterBean.isControlflowMappingCheckFlag());
            boolean dataFlowMapFlag = inputParameterBean.isDataFlowQueryMapCheckFlag();
            if (!controlFlowMapFlag) {
                singleCrontrolFlowLevelMapWithExcecutables.put(packageObjectName, controlFlowLevelLineageMap.get(packageObjectName));
                if (singleCrontrolFlowLevelMapWithExcecutables.size() > 0) {
                    globalMap.put("CONTROLFLOWMAPPINGS", singleCrontrolFlowLevelMapWithExcecutables);
                }
            } else {
                if (controlFlowLevelLineageMap.size() > 0) {
                    globalMap.put("CONTROLFLOWMAPPINGS", controlFlowLevelLineageMap);
                }
            }
            String dataflowComponentXpath = "//Executable/Executables/Executable/ObjectData/pipeline";
            NodeList dataflowComponentNodeList = null;
            try {
                dataflowComponentNodeList = (NodeList) xPath.compile(dataflowComponentXpath).evaluate(document, XPathConstants.NODESET);
            } catch (Exception e) {
                e.printStackTrace();
            }

            DataFlowBean dataflowBean = new DataFlowBean();
            dataflowBean.setAcpInputParameterBean(inputParameterBean);
            dataflowBean.setSsisInputParameterBean(ssisInputParameterBean);

            Map<String, List<Map<String, String>>> dataflowLineage = PrepareDataFlowLineage2014.iterateDataFlowComponentsData(dataflowComponentNodeList, dataflowBean);

            globalMap.put("dataflowLineage", dataflowLineage);
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "startExecution(-,-)");
        }
        return globalMap;
    }

    /**
     * this method will build the control flow components lineage and store into
     * the controlFlowLevelComponentSpecificationsWithExecutables map
     *
     * @param ssisInputParameterBean
     * @return controlFlowLevelComponentSpecificationsWithExecutables map
     */
    public Map<String, List<HashMap<String, String>>> prepareControlFlowComponentLineageMap(SSISInputParameterBean ssisInputParameterBean) {

        NodeList controlFlowLevelNodeList = ssisInputParameterBean.getControlFlowLevelNodeList();
        Set<String> disabledComponentSet = ssisInputParameterBean.getDisabledComponentSet();
        String fileName = ssisInputParameterBean.getDtsxFileName();
        Map<String, String> packageLevelVariablesMap = ssisInputParameterBean.getUserReferenceVariablesMap();
        HashMap<String, String> projectLevelVaraiblesMap = new HashMap<>();
        String packageObjectName = ssisInputParameterBean.getPackageObjectName();
        boolean controlFlowMapsCheck = Boolean.parseBoolean(ssisInputParameterBean.getInputParameterBean().isControlflowMappingCheckFlag());

        Map<String, List<HashMap<String, String>>> controlFlowLevelComponentSpecificationsWithExecutables = new HashMap<>();
        List<HashMap<String, String>> leanageAndExtendedProp = new ArrayList<>();
        String executableName = "";
        HashMap<String, String> sourceTargeteMap = new HashMap<>();
        HashMap<String, String> extendedProperties = new HashMap<>();
        ArrayList<String> executableNameList = new ArrayList<>();
        normalControlFlowComponentExtendedPropertiesFlag = true;
        HashMap<String, String> executablePropertiesMap = new HashMap<>();

        for (int i = 0; i < controlFlowLevelNodeList.getLength(); i++) {
            Node rootparentNode = controlFlowLevelNodeList.item(i).getParentNode();
            if (rootparentNode.getParentNode() != null && rootparentNode.getParentNode().getParentNode() != null && (!Constants.executableLiteral.equalsIgnoreCase(rootparentNode.getParentNode().getParentNode().getNodeName()))) {
                continue;
            }

            executableName = rootparentNode.getAttributes().getNamedItem("DTS:ObjectName").getTextContent();

            String executableRefId = rootparentNode.getAttributes().getNamedItem("DTS:refId").getTextContent();

            // Added On 01stAPril 2021 By Dinesh 
            if (executableNameList.contains(executableName)) {
                executableName = executableRefId.replace("\\", "_");

            }

            executableNameList.add(executableName);
            String parentRefId = "";
            try {
                if (executableRefId.contains("\\")) {
                    parentRefId = executableRefId.substring(0, executableRefId.lastIndexOf("\\"));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (disabledComponentSet != null && (disabledComponentSet.contains(executableRefId) || disabledComponentSet.contains(parentRefId))) {
                continue;
            }
            NodeList rootChildNodeList = controlFlowLevelNodeList.item(i).getChildNodes();
            for (int j = 0; j < rootChildNodeList.getLength(); j++) {
                if ("DTS:PrecedenceConstraint".equalsIgnoreCase(rootChildNodeList.item((j)).getNodeName())) {
                    String source = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:From").getTextContent();
                    String target = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:To").getTextContent();

                    if (disabledComponentSet.contains(source)) {
                        source = source + "_ERWINDISABLED";
                    }

                    if (disabledComponentSet.contains(target)) {
                        target = target + "_ERWINDISABLED";
                    }

                    if (sourceTargeteMap.get(target) == null) {
                        sourceTargeteMap.put(target, source);
                    } else {
                        source = sourceTargeteMap.get(target) + "_ADS_" + source;
                        sourceTargeteMap.put(target, source);

                    }
                    removeContrloFlowDuplicates.add(source);
                    removeContrloFlowDuplicates.add(target);
                }
            }
            NodeList siblingsList = rootparentNode.getChildNodes();
            Map<String, String> connectionMap = ssisInputParameterBean.getConnectionsMap();
            Parser2014XMLFile parser2014XMLFile = ssisInputParameterBean.getParser2014XMLFile();
            extendedProperties = parser2014XMLFile.readQueriesFromControlFlowComponents(siblingsList, connectionMap);
            extendedProperties = DynamicVaribleValueReplacement.replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(packageLevelVariablesMap, extendedProperties, controlFlowComponentNameSet, projectLevelVaraiblesMap);
            executablePropertiesMap.putAll(extendedProperties);

            if (!controlFlowMapsCheck) {
                if (executableName.equalsIgnoreCase(packageObjectName)) {
                    leanageAndExtendedProp.add(sourceTargeteMap);
                    leanageAndExtendedProp.add(executablePropertiesMap);
                    controlFlowLevelComponentSpecificationsWithExecutables.put(executableName, leanageAndExtendedProp);
                }
            } else {
                leanageAndExtendedProp.add(sourceTargeteMap);
                leanageAndExtendedProp.add(extendedProperties);
                controlFlowLevelComponentSpecificationsWithExecutables.put(executableName, leanageAndExtendedProp);
            }

            leanageAndExtendedProp = new ArrayList<>();
            sourceTargeteMap = new HashMap<>();
            extendedProperties = new HashMap<>();
        }
        return controlFlowLevelComponentSpecificationsWithExecutables;
    }

    /**
     * in this method we are preparing the lineage for the orphan or individual
     * control flow components lineage to the
     * controlFlowLevelComponentSpecificationsWithExecutables map
     *
     * @param crontrolFlowLevelMapWithExcecutables
     * @param controlFlowMissingCompNodeList
     * @param parentComponentAndChildPackageName
     * @param parameterHashMap
     * @param ssisInputParameterBean
     * @return controlFlowLevelComponentSpecificationsWithExecutables
     */
    @SuppressWarnings("all")
    public Map<String, List<HashMap<String, String>>> prepareMissingComponentsLineage(Map<String, List<HashMap<String, String>>> crontrolFlowLevelMapWithExcecutables, NodeList controlFlowMissingCompNodeList, Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageName, Map<String, String> parameterHashMap, SSISInputParameterBean ssisInputParameterBean) {
        String objectName = "";
        String refId = "";
        ArrayList<String> objectNameList = new ArrayList<>();
        Set<String> parentRefSet = new HashSet<>();
        missingControlFlowComponentExtendedPropertiesFlag = true;
        HashMap<String, String> allExtendedProperties = new HashMap<>();
        boolean controlFlowMapsCheck = Boolean.parseBoolean(ssisInputParameterBean.getInputParameterBean().isControlflowMappingCheckFlag());
        Set<String> disabledComponentSet = ssisInputParameterBean.getDisabledComponentSet();
        Map<String, String> packageLevelVariableMap = ssisInputParameterBean.getUserReferenceVariablesMap();
        Map<String, String> projectLevelVariablesMap = ssisInputParameterBean.getInputParameterBean().getProjectLevelVaraiblesMap();
        String fileName = ssisInputParameterBean.getPackageObjectName();
        String packageObjectName = ssisInputParameterBean.getPackageObjectName();
        Set<String> keySet = crontrolFlowLevelMapWithExcecutables.keySet();
        Set<String> eventHandlerSet = new HashSet<>();
        HashMap<String, String> sourceTargeteMap = new HashMap<>();
        HashMap<String, String> extendedProperties = new HashMap<>();
        List<HashMap<String, String>> leanageAndExtendedProp = new ArrayList<>();
        Map<String, String> connections = ssisInputParameterBean.getConnectionsMap();
        Parser2014XMLFile parser2014XMLFile = ssisInputParameterBean.getParser2014XMLFile();
        try {
            for (int j = 0; j < controlFlowMissingCompNodeList.getLength(); j++) {
                if ("DTS:Executable".equalsIgnoreCase(controlFlowMissingCompNodeList.item(j).getNodeName())) {
                    String objectRefId = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:refId").getTextContent();
                    objectName = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();

                    String parentObjectName = "";
                    String disabledProperties = "";

                    try {
                        String parentNodeName = controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getNodeName();
                        if (!parentNodeName.contains("Executable")) {
                            eventHandlerSet.add(objectName);
                            continue;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    try {

                        parentObjectName = objectName;

                        disabledProperties = "";
                        try {
                            if (controlFlowMissingCompNodeList.item(j) != null
                                    && controlFlowMissingCompNodeList.item(j).getAttributes() != null
                                    && controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:Disabled") != null) {
                                disabledProperties = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                            }

                            if ("TRUE".equalsIgnoreCase(disabledProperties)) {
                                disabledComponentSet.add(objectRefId);
                                continue;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {

                    }
                    String parentRefId = "";
                    try {
                        parentRefId = objectRefId.substring(0, objectRefId.lastIndexOf("\\"));
                        if (disabledComponentSet.contains(objectRefId) || disabledComponentSet.contains(parentRefId)) {
                            disabledComponentSet.add(objectRefId);
                            continue;
                        }
                    } catch (Exception e) {

                    }

                    String componentType = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:CreationName").getTextContent();
                    boolean visitedFlag = checkComponentVisited(keySet, objectRefId);
                    if (!visitedFlag && ("STOCK:FOREACHLOOP".equals(componentType) || "STOCK:SEQUENCE".equals(componentType))) {

                        try {
                            updateTheControlFlowMissingComponentsMap(j, controlFlowMissingCompNodeList, parentObjectName,
                                    disabledProperties, eventHandlerSet, objectRefId, objectName,
                                    sourceTargeteMap,
                                    extendedProperties,
                                    leanageAndExtendedProp,
                                    crontrolFlowLevelMapWithExcecutables
                            );
                            leanageAndExtendedProp = new ArrayList<>();
                            sourceTargeteMap = new HashMap<>();
                            extendedProperties = new HashMap<>();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        NodeList executablesList = controlFlowMissingCompNodeList.item(j).getChildNodes();
                        for (int k = 0; k < executablesList.getLength(); k++) {
                            if ("DTS:Executables".equalsIgnoreCase(executablesList.item(k).getNodeName())) {
                                NodeList executableList = executablesList.item(k).getChildNodes();
                                for (int l = 0; l < executableList.getLength(); l++) {
                                    if ("DTS:Executable".equalsIgnoreCase(executableList.item(l).getNodeName())) {

                                        if (executableList.item(l).getAttributes().getNamedItem("DTS:ObjectName") != null) {
                                            objectName = executableList.item(l).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                                        }
                                        disabledProperties = "";
                                        try {
                                            if (executableList.item(l).getAttributes().getNamedItem("DTS:Disabled") != null) {
                                                disabledProperties = executableList.item(l).getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                                            }

                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }

                                        refId = executableList.item(l).getAttributes().getNamedItem("DTS:refId").getTextContent();
                                        if (!removeContrloFlowDuplicates.contains(refId) && !disabledProperties.toUpperCase().equals("TRUE")) {
                                            removeContrloFlowDuplicates.add(refId);
                                            sourceTargeteMap.put(objectName, "");
                                        }
                                    }
                                    String executableComponentRefId = refId;
                                    executableComponentRefId = executableComponentRefId.replace("Package\\", "").replace("\\", "_");

                                    String objectNameOrRefId = "";

                                    if (disabledComponentSet.contains(refId)) {

                                        continue;
                                    }

                                    if (objectNameList.contains(objectName)) {
                                        objectNameOrRefId = executableComponentRefId;
                                    } else {
                                        objectNameOrRefId = objectName;
                                    }
                                    objectNameList.add(objectName);
                                    DynamicVaribleValueReplacement.getChildPackageNameAndVaribleData(executableList, l, objectNameOrRefId, parentComponentAndChildPackageName, parameterHashMap);

                                }
                            }
                        }
                        extendedProperties = parser2014XMLFile.readQueriesFromControlFlowComponents(executablesList, connections);
                        extendedProperties = DynamicVaribleValueReplacement.replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(packageLevelVariableMap, extendedProperties, controlFlowComponentNameSet, projectLevelVariablesMap);
                        allExtendedProperties.putAll(extendedProperties);
                        try {
                            if (crontrolFlowLevelMapWithExcecutables.get(parentObjectName) == null) {
                                parentRefSet.add(parentRefId);

                                leanageAndExtendedProp.add(sourceTargeteMap);
                                leanageAndExtendedProp.add(extendedProperties);
                                crontrolFlowLevelMapWithExcecutables.put(parentObjectName, leanageAndExtendedProp);
                            } else if (!"Package".equals(parentRefId) && !parentRefSet.contains(parentRefId)) {
                                parentObjectName = parentRefId.replaceAll("[^a-zA-Z0-9\\p{L}]", "_").replace("Package_", "") + "_" + parentObjectName;
                                parentRefSet.add(parentRefId);
                                if (!sourceTargeteMap.isEmpty()) {
                                    leanageAndExtendedProp.add(sourceTargeteMap);
                                    leanageAndExtendedProp.add(extendedProperties);
                                    crontrolFlowLevelMapWithExcecutables.put(parentObjectName, leanageAndExtendedProp);
                                }

                            } else {
                                parentRefSet.add(parentRefId);
                                if (!sourceTargeteMap.isEmpty()) {

                                    crontrolFlowLevelMapWithExcecutables.get(parentObjectName).get(0).putAll(sourceTargeteMap);
                                    crontrolFlowLevelMapWithExcecutables.get(parentObjectName).get(1).putAll(extendedProperties);

                                }
                            }
                        } catch (Exception e) {
                        }
                        leanageAndExtendedProp = new ArrayList();
                        sourceTargeteMap = new HashMap();
                        extendedProperties = new HashMap();

                    } else if ("Microsoft.ExecuteSQLTask".equals(componentType)) {
                        NodeList executablesList = controlFlowMissingCompNodeList.item(j).getChildNodes();
                        try {
                            disabledProperties = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                        } catch (Exception e) {

                        }
                        try {
                            String parentNodeName = controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getNodeName();

                            if (controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:ObjectName") != null) {
                                parentObjectName = controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                            }

                            if (!parentNodeName.contains("Executable") || eventHandlerSet.contains(parentObjectName)) {
                                continue;
                            }

                            disabledProperties = "";
                            try {
                                if (controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:Disabled") != null) {
                                    disabledProperties = controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                                }

                                if (disabledProperties.toUpperCase().equals("TRUE")) {
                                    continue;
                                }
                            } catch (Exception e) {

                            }
                        } catch (Exception e) {

                        }
                        for (int k = 0; k < executablesList.getLength(); k++) {
                            if ("DTS:ObjectData".equalsIgnoreCase(executablesList.item(k).getNodeName())) {
                                NodeList executableList = executablesList.item(k).getChildNodes();
                                for (int l = 0; l < executableList.getLength(); l++) {
                                    if ("SQLTask:SqlTaskData".equalsIgnoreCase(executableList.item(l).getNodeName())) {
                                        String query = executableList.item(l).getAttributes().getNamedItem("SQLTask:SqlStatementSource").getTextContent();

                                        objectName = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                                        refId = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:refId").getTextContent();
                                        String queryconnection = "";
                                        if (controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("SQLTask:Connection") != null) {
                                            queryconnection = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("SQLTask:Connection").getTextContent();
                                        } else if (executableList.item(l).getAttributes().getNamedItem("SQLTask:Connection") != null) {
                                            queryconnection = executableList.item(l).getAttributes().getNamedItem("SQLTask:Connection").getTextContent();
                                        }
                                        String serverName = "";
                                        String databaseName = "";
                                        try {
                                            if (!StringUtils.isBlank(queryconnection)) {
                                                String connectionInfo = connections.get(queryconnection).toString();
                                                if (connectionInfo.contains(Delimiter.emm_Delimiter)) {
                                                    databaseName = connectionInfo.split(Delimiter.emm_Delimiter)[2];
                                                    serverName = connectionInfo.split(Delimiter.emm_Delimiter)[3];
                                                }
                                                if (connectionInfo.contains(Delimiter.delimiter)) {
                                                    databaseName = connectionInfo.split(Delimiter.delimiter)[1];
                                                    serverName = connectionInfo.split(Delimiter.delimiter)[0];
                                                }
                                            }
                                        } catch (Exception e) {

                                        }

                                        if (!removeContrloFlowDuplicates.contains(refId) && !disabledProperties.toUpperCase().equals("TRUE")) {
                                            sourceTargeteMap.put(objectName, "");
                                            extendedProperties.put(objectName + "$queryName", query + Delimiter.delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName);
                                            extendedProperties = DynamicVaribleValueReplacement.replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(packageLevelVariableMap, extendedProperties, controlFlowComponentNameSet, projectLevelVariablesMap);

                                            allExtendedProperties.putAll(extendedProperties);
                                        } else {
                                            continue;
                                        }
                                        try {
                                            if (crontrolFlowLevelMapWithExcecutables.get(parentObjectName) == null && !"".equals(parentObjectName)) {
                                                leanageAndExtendedProp.add(sourceTargeteMap);
                                                leanageAndExtendedProp.add(extendedProperties);
                                                crontrolFlowLevelMapWithExcecutables.put(parentObjectName, leanageAndExtendedProp);
                                            } else {
                                                crontrolFlowLevelMapWithExcecutables.get(parentObjectName).get(0).putAll(sourceTargeteMap);
                                                crontrolFlowLevelMapWithExcecutables.get(parentObjectName).get(1).putAll(extendedProperties);
                                            }
                                        } catch (Exception e) {
                                        }
                                    }
                                }
                            }
                        }
                        leanageAndExtendedProp = new ArrayList<>();
                        sourceTargeteMap = new HashMap<>();
                        extendedProperties = new HashMap<>();
                    } else if ("Microsoft.ExecutePackageTask".equalsIgnoreCase(componentType) || "SSIS.ExecutePackageTask.3".equalsIgnoreCase(componentType)) {

                        if (!parentComponentAndChildPackageName.containsKey(parentObjectName)) {
                            DynamicVaribleValueReplacement.getChildPackageNameAndVaribleData(controlFlowMissingCompNodeList, j, parentObjectName, parentComponentAndChildPackageName, parameterHashMap);
                        }

                    } else {
                        updateTheControlFlowMissingComponentsMap(j, controlFlowMissingCompNodeList, parentObjectName,
                                disabledProperties, eventHandlerSet, objectRefId, objectName,
                                sourceTargeteMap,
                                extendedProperties,
                                leanageAndExtendedProp,
                                crontrolFlowLevelMapWithExcecutables
                        );
                    }
                    if (!fileName.equalsIgnoreCase(parentObjectName)) {

                        if (controlFlowMissingCompNodeList.getLength() <= 2 && crontrolFlowLevelMapWithExcecutables.get(fileName) == null) {
                            sourceTargeteMap.put(parentObjectName, "");
                            leanageAndExtendedProp.add(sourceTargeteMap);
                            leanageAndExtendedProp.add(extendedProperties);
                            crontrolFlowLevelMapWithExcecutables.put(fileName, leanageAndExtendedProp);

                        } else if (controlFlowMissingCompNodeList.getLength() <= 2 && crontrolFlowLevelMapWithExcecutables.get(fileName) != null) {
                            crontrolFlowLevelMapWithExcecutables.get(fileName).get(0).putAll(sourceTargeteMap);
                            crontrolFlowLevelMapWithExcecutables.get(fileName).get(1).putAll(extendedProperties);
                        }
                    }

                    leanageAndExtendedProp = new ArrayList<>();
                    sourceTargeteMap = new HashMap<>();
                    extendedProperties = new HashMap<>();
                }
            }
            if (!controlFlowMapsCheck && crontrolFlowLevelMapWithExcecutables.get(fileName) != null) {
                crontrolFlowLevelMapWithExcecutables.get(fileName).get(1).putAll(allExtendedProperties);
            }
        } catch (Exception e) {
            e.printStackTrace();
//            exceptionBuilder.append("\n" + e + "\n");
        }
        return crontrolFlowLevelMapWithExcecutables;
    }

    /**
     * this method updates the crontrolFlowLevelMapWithExcecutables map with
     * missing components
     *
     * @param j
     * @param controlFlowMissingCompNodeList
     * @param parentObjectName
     * @param disabledProperties
     * @param eventHandlerSet
     * @param objectRefId
     * @param objectName
     * @param sourceTargeteMap
     * @param extendedProperties
     * @param leanageAndExtendedProp
     * @param crontrolFlowLevelMapWithExcecutables
     */
    @SuppressWarnings("all")
    public void updateTheControlFlowMissingComponentsMap(int j, NodeList controlFlowMissingCompNodeList, String parentObjectName,
            String disabledProperties, Set<String> eventHandlerSet, String objectRefId, String objectName,
            HashMap<String, String> sourceTargeteMap,
            HashMap<String, String> extendedProperties,
            List<HashMap<String, String>> leanageAndExtendedProp,
            Map<String, List<HashMap<String, String>>> crontrolFlowLevelMapWithExcecutables
    ) {

        try {
            String parentNodeName = controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getNodeName();
            if (controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:ObjectName") != null) {
                parentObjectName = controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
            }

            if (!parentNodeName.contains("Executable") || eventHandlerSet.contains(parentObjectName)) {
                return;
            }

            disabledProperties = "";
            try {
                if (controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:Disabled") != null) {
                    disabledProperties = controlFlowMissingCompNodeList.item(j).getParentNode().getParentNode().getAttributes().getNamedItem("DTS:Disabled").getTextContent();
                }

                if (disabledProperties.equalsIgnoreCase("TRUE")) {
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!removeContrloFlowDuplicates.contains(objectRefId) && !disabledProperties.equalsIgnoreCase("TRUE")) {
            removeContrloFlowDuplicates.add(objectRefId);
            sourceTargeteMap.put(objectName, "");
            extendedProperties = new HashMap<>();
        } else {
            return;
        }
        try {
            if (crontrolFlowLevelMapWithExcecutables.get(parentObjectName) == null && !"".equals(parentObjectName)) {
                leanageAndExtendedProp.add(sourceTargeteMap);
                leanageAndExtendedProp.add(extendedProperties);
                crontrolFlowLevelMapWithExcecutables.put(parentObjectName, leanageAndExtendedProp);
            } else {
                crontrolFlowLevelMapWithExcecutables.get(parentObjectName).get(0).putAll(sourceTargeteMap);
                crontrolFlowLevelMapWithExcecutables.get(parentObjectName).get(1).putAll(extendedProperties);
            }
        } catch (Exception e) {
        }

    }

    /**
     * this method will return true or false based on that whether the component
     * all ready visited or not if component all ready visited then this method
     * will return true otherwise it will return false
     *
     * @param keySet
     * @param objectRefId
     * @return true/false
     */
    public static boolean checkComponentVisited(Set<String> keySet, String objectRefId) {
        boolean flag = false;
        if (objectRefId != null && objectRefId.contains("Package\\")) {
            objectRefId = objectRefId.replace("Package\\", "");
        }
        for (String key : keySet) {
            if (key.contains(objectRefId)) {
                flag = true;
                return flag;
            }
        }
        return flag;
    }

}
