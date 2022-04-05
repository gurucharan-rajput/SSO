/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2008;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.function.LongSupplier;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author THarika
 */
public class SSIS2008 {

    String packageName = "";
    Set<String> eventHandlerExecutables = new HashSet<>();
    Set<String> removeControlFlowDuplicates = new HashSet<>();
    static Map<String, String> objectNameAndQueryMap = new LinkedHashMap<>();
    public Map<String, Object> globalMap = new HashMap<>();
    HashSet<String> controlFlowComponentNameSet = new HashSet<>();

    LongSupplier supplier = () -> System.currentTimeMillis();
    LongBinaryOperator binaryOperator = (start, end) -> (end - start);

    /**
     * clears all the global variables data
     */
    public void clearStaticMemory() {
        packageName = "";
        removeControlFlowDuplicates.clear();
        globalMap.clear();
        controlFlowComponentNameSet.clear();

    }

    /**
     * this method starts the processing of individual 2008 dtsx file
     *
     * @param acpInputParameterBean
     */
    public Map<String, Object> startExecution(ACPInputParameterBean acpInputParameterBean) {

        long startTime = supplier.getAsLong(); //startExecution
        clearStaticMemory();
        File inputFile = null;
        Document xmlDocument = null;
        Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageNameMap
                = null;
        Map<String, String> parameterHashMap = null;
        String fileName = "";
        try {
            if (acpInputParameterBean != null) {
                inputFile = acpInputParameterBean.getInputFile();
                xmlDocument = acpInputParameterBean.getDocument();
                parentComponentAndChildPackageNameMap
                        = acpInputParameterBean.getParentComponentAndChildPackageName();
                parameterHashMap = acpInputParameterBean.getParameterHashMap();
                fileName = inputFile.getName();
                if (StringUtils.isNotBlank(fileName)) {
                    fileName = fileName.substring(0, fileName.lastIndexOf("."));
                    fileName = fileName.replaceAll(Constants.commonRegularExpression, "_");
                    packageName = fileName;
                }
                Parser2008XMLFile parser2008XMLFile = new Parser2008XMLFile();

                Map<String, String> userReferenceVariableMap = parser2008XMLFile.prepareUserVariablesMap(xmlDocument);
                userReferenceVariableMap.put("PackageName", packageName);

                userReferenceVariableMap = DynamicVaribleValueReplacement.replaceSpecialSymobalsFromPackageAndProjectNameAndMakeOnlyThierNamesAsValues_2008(userReferenceVariableMap);

                XPath xPath = XPathFactory.newInstance().newXPath();
                Map<String, String> connectionsMap = parser2008XMLFile.prepareConnectionsMap(xmlDocument);

                String disabledExecutablePropertyPath = "//Executable/Executable/Property";
                NodeList disabledExecutablePropertyNodeList = ((NodeList) xPath.compile(disabledExecutablePropertyPath).evaluate(xmlDocument, XPathConstants.NODESET));

                HashSet<String> disabledComponentSet = new HashSet<>();
                if (disabledExecutablePropertyNodeList != null) {
                    disabledComponentSet = parser2008XMLFile.getDeactiveComponentList(disabledExecutablePropertyNodeList);
                }
                String packageObjectName = "";
                String packagePath = "/Executable";
                NodeList packagePathNodeList = (NodeList) xPath.compile(packagePath).evaluate(xmlDocument, XPathConstants.NODESET);

                try {
                    for (int j = 0; j < packagePathNodeList.getLength(); j++) {
                        NodeList childNodes = packagePathNodeList.item(j).getChildNodes();
                        for (int i = 0; i < childNodes.getLength(); i++) {
                            Node childNodeitem = childNodes.item(i);
                            if (StringUtils.isBlank(packageObjectName)) {
                                packageObjectName = parser2008XMLFile.getTextContentFromXml(childNodeitem, Constants.objectLiteral);
                            } else {
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String controlFlowLevelPath = "//Executable/PrecedenceConstraint";
                NodeList controlFlowLevelNodeList = ((NodeList) xPath.compile(controlFlowLevelPath).evaluate(xmlDocument, XPathConstants.NODESET));

                SSISInputParameterBean ssisInputParameterBean = new SSISInputParameterBean();
                ssisInputParameterBean.setInputParameterBean(acpInputParameterBean);
                ssisInputParameterBean.setUserReferenceVariablesMap(userReferenceVariableMap);
                ssisInputParameterBean.setConnectionsMap(connectionsMap);
                ssisInputParameterBean.setControlFlowLevelNodeList(controlFlowLevelNodeList);
                ssisInputParameterBean.setDtsxFileName(fileName);
                ssisInputParameterBean.setPackageObjectName(packageObjectName);
                ssisInputParameterBean.setParser2008XMLFile(parser2008XMLFile);
                ssisInputParameterBean.setDisabledComponentSet(disabledComponentSet);

                String executablePropertyPath = "//Executable/Property";
                NodeList executablePropertyNodeList = ((NodeList) xPath.compile(executablePropertyPath).evaluate(xmlDocument, XPathConstants.NODESET));

                Map<String, List<Map<String, String>>> crontrolFlowLevelMapWithExcecutables = prepareControlFlowComponentLineageMap(ssisInputParameterBean, executablePropertyNodeList);

                String controlFlowMissingComponentPath = "//Executable/Executable";
                NodeList controlFlowMissingComponentNodeList = ((NodeList) xPath.compile(controlFlowMissingComponentPath).evaluate(xmlDocument, XPathConstants.NODESET));

                crontrolFlowLevelMapWithExcecutables = controlFlowMissingComponentsLineage(crontrolFlowLevelMapWithExcecutables, controlFlowMissingComponentNodeList, ssisInputParameterBean, parentComponentAndChildPackageNameMap, parameterHashMap);
                String componentpath = "//Executable/Executable/ObjectData/pipeline/components/component";

                acpInputParameterBean.setParentComponentAndChildPackageName(parentComponentAndChildPackageNameMap);
                acpInputParameterBean.setParameterHashMap(parameterHashMap);
                NodeList componentnodelist = (NodeList) xPath.compile(componentpath).evaluate(xmlDocument, XPathConstants.NODESET);

                Map<String, List<Map<String, String>>> singleCrontrolFlowLevelMapWithExcecutables = new HashMap<>();
                boolean controlFlowMapFlag = Boolean.parseBoolean(acpInputParameterBean.isControlflowMappingCheckFlag());
                if (!controlFlowMapFlag) {
                    singleCrontrolFlowLevelMapWithExcecutables.put(packageObjectName, crontrolFlowLevelMapWithExcecutables.get(packageObjectName));
                    if (singleCrontrolFlowLevelMapWithExcecutables.size() > 0) {
                        globalMap.put("CONTROLFLOWMAPPINGS", singleCrontrolFlowLevelMapWithExcecutables);
                    }
                } else {
                    if (crontrolFlowLevelMapWithExcecutables.size() > 0) {
                        globalMap.put("CONTROLFLOWMAPPINGS", crontrolFlowLevelMapWithExcecutables);
                    }
                }
                String requiredXpath = "//Executable/Executable/ObjectData/pipeline";
                NodeList dataflowComponentNodeList = (NodeList) xPath.compile(requiredXpath).evaluate(xmlDocument, XPathConstants.NODESET);

                DataFlowBean dataflowBean = new DataFlowBean();
                dataflowBean.setAcpInputParameterBean(acpInputParameterBean);
                dataflowBean.setSsisInputParameterBean(ssisInputParameterBean);

                Map<String, List<Map<String, String>>> dataFlowComponentsLineage = new LinkedHashMap<>();
                dataFlowComponentsLineage = PrepareDataFlowLineage2008.iterateDataFlowComponentsData(dataflowComponentNodeList, dataflowBean);

                globalMap.put("dataflowLineage", dataFlowComponentsLineage);
            }

        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "startExecution(-,-)");
        }
        return globalMap;
    }

    /**
     * creates a hash map containing details of all executables along with its
     * DTSId,queries(if present),lineage flow
     *
     * @param ssisInputParameterBean
     * @param executablePropertyNodeList
     * @return
     */
    public Map<String, List<Map<String, String>>> prepareControlFlowComponentLineageMap(SSISInputParameterBean ssisInputParameterBean, NodeList executablePropertyNodeList) {

        long startTime = supplier.getAsLong();
        NodeList controlFlowLevelNodeList = ssisInputParameterBean.getControlFlowLevelNodeList();
        Set<String> disabledComponentSet = ssisInputParameterBean.getDisabledComponentSet();
        Parser2008XMLFile parser2008XMLFile = ssisInputParameterBean.getParser2008XMLFile();
        String packageObjectName = ssisInputParameterBean.getPackageObjectName();
        Map<String, String> packageLevelVariablesMap = ssisInputParameterBean.getUserReferenceVariablesMap();
        HashMap<String, String> projectLevelVaraiblesMap = new HashMap<>();

        boolean controlFlowMapsCheck = Boolean.parseBoolean(ssisInputParameterBean.getInputParameterBean().isControlflowMappingCheckFlag());
        Map<String, String> executablePropertyHashMap = getExecutablePropertyHashMap(executablePropertyNodeList, parser2008XMLFile);
        eventHandlerExecutables = parser2008XMLFile.eventHandlerExecutables;
        try {
            eventHandlerExecutables.forEach((String key) -> {
                if (executablePropertyHashMap.containsKey(key)) {
                    executablePropertyHashMap.remove(key);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Map<String, String>> executableSrcNTgtHashMap = new HashMap<>();

        Map<String, List<Map<String, String>>> controlFlowComponents = new HashMap<>();

        List<Map<String, String>> leanageAndExtendedProp = new ArrayList<>();
        String executableRefId = "TempExecutableId";
        String executableRefIdPrevios = "TempExecutableId";
        Map<String, String> sourceTargeteMap = new HashMap<>();
        HashMap<String, String> extendedProperties = new HashMap<>();
        Map<String, String> allExtendedProperties = new HashMap<>();
        for (int i = 0; i < controlFlowLevelNodeList.getLength(); i++) {
            Node rootparentNode = controlFlowLevelNodeList.item(i).getParentNode();
            String executableObjectDtsId = parser2008XMLFile.getObjectName(rootparentNode);
            executableRefId = executableObjectDtsId.split(Delimiter.delimiter)[0];
            try {
                String dtsId = "";
                if (executableObjectDtsId.split(Delimiter.delimiter).length > 1) {
                    dtsId = executableObjectDtsId.split(Delimiter.delimiter)[1];
                }
                if (eventHandlerExecutables.contains(dtsId)) {
                    executableRefId = "";
                    executableRefIdPrevios = "";
                    continue;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (i == 0) {
                executableRefIdPrevios = executableRefId;
            } else if (!executableRefIdPrevios.equals(executableRefId)) {
                if (!controlFlowMapsCheck) {
                    if (executableRefIdPrevios.equalsIgnoreCase(packageObjectName)) {
                        leanageAndExtendedProp.add(sourceTargeteMap);
                        leanageAndExtendedProp.add(extendedProperties);
                        controlFlowComponents.put(executableRefIdPrevios, leanageAndExtendedProp);
                    }
                } else {
                    leanageAndExtendedProp.add(sourceTargeteMap);
                    leanageAndExtendedProp.add(extendedProperties);
                    controlFlowComponents.put(executableRefIdPrevios, leanageAndExtendedProp);
                }
                leanageAndExtendedProp = new ArrayList<>();
                sourceTargeteMap = new HashMap<>();
                extendedProperties = new HashMap<>();
                executableRefIdPrevios = executableRefId;
            }

            String source = "";
            String target = "";
            NodeList rootChildNodeList = controlFlowLevelNodeList.item(i).getChildNodes();
            for (int j = 0; j < rootChildNodeList.getLength(); j++) {
                try {
                    if (Constants.executableLiteral.equalsIgnoreCase(rootChildNodeList.item((j)).getNodeName())) {
                        String iDREF = rootChildNodeList.item((j)).getAttributes().getNamedItem("IDREF").getTextContent();
                        String isFrom = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:IsFrom").getTextContent();
                        String quryValue = "";
                        Function<String, String> function = (String s) -> {
                            String queryValue = "";
                            if (objectNameAndQueryMap.get(s) != null) {
                                queryValue = objectNameAndQueryMap.get(s);
                            }
//                            if (disabledComponentSet.contains(iDREF)) {
//                                s = s + "_ERWINDISABLED";
//                            }
                            return queryValue;
                        };
                        if (isFrom != null && isFrom.equals("-1")) {
                            source = executablePropertyHashMap.get(iDREF);
                            String sourceKey = source + "$queryName";
                            quryValue = function.apply(sourceKey);
                            if (disabledComponentSet.contains(iDREF)) {
                                source = source + "_ERWINDISABLED";
                            }
                            if (StringUtils.isNotBlank(quryValue) && !source.contains("_ERWINDISABLED")) {
                                extendedProperties.put(sourceKey, quryValue);
                            }

                        } else if (isFrom != null && isFrom.equals("0")) {
                            target = executablePropertyHashMap.get(iDREF);
                            String targetKey = target + "$queryName";
                            quryValue = function.apply(targetKey);
                            if (disabledComponentSet.contains(iDREF)) {
                                target = target + "_ERWINDISABLED";
                            }
                            if (StringUtils.isNotBlank(quryValue) && !target.contains("_ERWINDISABLED")) {
                                extendedProperties.put(targetKey, quryValue);
                            }

                        }

                        if (StringUtils.isBlank(source) || StringUtils.isBlank(target)) {
                            continue;
                        } else if (StringUtils.isNotBlank(source) && StringUtils.isNotBlank(target)) {
                            if (sourceTargeteMap.get(target) == null) {
                                sourceTargeteMap.put(target, source);
                            } else {
                                source = sourceTargeteMap.get(target) + "_ADS_" + source;
                                sourceTargeteMap.put(target, source);
                            }
                            removeControlFlowDuplicates.add(source);
                            removeControlFlowDuplicates.add(target);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            extendedProperties = DynamicVaribleValueReplacement.replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(packageLevelVariablesMap, extendedProperties, controlFlowComponentNameSet, projectLevelVaraiblesMap);

            allExtendedProperties.putAll(extendedProperties);
            if (executableSrcNTgtHashMap.get(executableRefId) != null) {
                executableSrcNTgtHashMap.get(executableRefId).putAll(sourceTargeteMap);
            } else {
                executableSrcNTgtHashMap.put(executableRefId, sourceTargeteMap);
            }
            if (i == controlFlowLevelNodeList.getLength() - 1) {

                leanageAndExtendedProp.add(sourceTargeteMap);
                leanageAndExtendedProp.add(extendedProperties);
                controlFlowComponents.put(executableRefIdPrevios, leanageAndExtendedProp);

                leanageAndExtendedProp = new ArrayList<>();
                sourceTargeteMap = new HashMap<>();
                extendedProperties = new HashMap<>();
                executableRefIdPrevios = executableRefId;
            }
        }
        if (!controlFlowMapsCheck && controlFlowComponents.get(packageObjectName) != null) {
            controlFlowComponents.get(packageObjectName).get(1).putAll(allExtendedProperties);
        }
        long endTime = supplier.getAsLong();

        System.out.println(
                "time taken ---------- in prepareControlFlowComponentLineageMap method" + binaryOperator.applyAsLong(endTime, startTime));
        return controlFlowComponents;
    }

    /**
     * Returns a Hash map containing key as executable name and value as its
     * dtsId
     *
     * @param executablePropertyNodeList
     * @param parser2008XMLFile
     * @return
     */
    public static HashMap<String, String> getExecutablePropertyHashMap(NodeList executablePropertyNodeList, Parser2008XMLFile parser2008XMLFile) {
        HashMap<String, String> executablePropertyHashMap = new HashMap();
        String objectName = "";
        String objectId = "";
        String objectcontent = "";
        String containername = "";
        String EventID = "";
        Node parentPropertyNode = null;
        ArrayList<String> objectslist = new ArrayList<String>();
        HashMap<String, List<String>> containernameanditscomponenets = new HashMap<String, List<String>>();
        List<String> objlist = new ArrayList<String>();

        try {
            for (int i = 0; i < executablePropertyNodeList.getLength(); i++) {
                Node propertyNode = executablePropertyNodeList.item(i);
                parentPropertyNode = propertyNode.getParentNode();

                if (propertyNode.getNodeName().equals("#text")) {
                    continue;
                } else if ("ObjectName".equalsIgnoreCase(propertyNode.getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                    objectcontent = propertyNode.getTextContent();
                    objectslist.add(objectcontent);
//                    break;
                } else if ("CreationName".equalsIgnoreCase(propertyNode.getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                    containername = propertyNode.getTextContent();
                    if ("STOCK:SEQUENCE".equals(containername)) {
                        containernameanditscomponenets.put(objectcontent, new ArrayList<>(objectslist));
                        objectslist.clear();
                    }
                }
            }

            for (int i = 0; i < executablePropertyNodeList.getLength(); i++) {
                Node propertyNode = executablePropertyNodeList.item(i);
                parentPropertyNode = propertyNode.getParentNode();
                NodeList childNodes = parentPropertyNode.getChildNodes();
                if (propertyNode.getNodeName().equals("#text")) {
                    continue;
                } else if ("ObjectName".equalsIgnoreCase(propertyNode.getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                    objectName = propertyNode.getTextContent();
//                    break;
                } else if ("DTSID".equalsIgnoreCase(propertyNode.getAttributes().getNamedItem("DTS:Name").getTextContent())) {
                    objectId = propertyNode.getTextContent();
                }
                try {
                    if (StringUtils.isNotBlank(objectName) && StringUtils.isNotBlank(objectId)) {
                        for (int j = 0; j < childNodes.getLength(); j++) {
                            if ("#text".equalsIgnoreCase(childNodes.item(j).getNodeName())) {
                                continue;
                            }
                            if ("DTS:EventHandler".equalsIgnoreCase(childNodes.item(j).getNodeName())) {
                                Node eventHandlerNode = childNodes.item(j);
                                NodeList eventHandlerChildNodes = eventHandlerNode.getChildNodes();
                                parser2008XMLFile.eventHandler(eventHandlerChildNodes);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (!"".equals(objectName) && !"".equals(objectId)) {
                    executablePropertyHashMap.put(objectId, objectName);
                    objectName = "";
                    objectId = "";
                    EventID = "";
                }
                if (!"".equals(objectName)) {
                    if (objlist.contains(objectName)) {
                        for (Map.Entry me : containernameanditscomponenets.entrySet()) {
                            List valuelist = (List) me.getValue();
                            if (valuelist.contains(objectName)) {
                                objectName = (String) me.getKey() + "_" + objectName;
                                break;
                            }
                        }
                    }
                    objlist.add(objectName);

                    parser2008XMLFile.prepareObjectNameAndSqlQueryForExtendedProperties(parentPropertyNode, objectName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return executablePropertyHashMap;
    }

    /**
     * updates the controlFlowLevelMapWithExcecutables map with the missing
     * executables
     *
     * @param controlFlowLevelMapWithExecutables
     * @param controlFlowMissingCompNodeList
     * @param ssisInputParameterBean
     * @return crontrolFlowLevelMapWithExcecutables
     */
    public Map<String, List<Map<String, String>>> controlFlowMissingComponentsLineage(Map<String, List<Map<String, String>>> controlFlowLevelMapWithExecutables, NodeList controlFlowMissingCompNodeList, SSISInputParameterBean ssisInputParameterBean, Map<String, HashMap<String, HashMap<String, String>>> parentComponentAndChildPackageNameMap, Map<String, String> parameterHashMap) {

        Set<String> disabledComponentSet = ssisInputParameterBean.getDisabledComponentSet();
        String packageObjectName = ssisInputParameterBean.getPackageObjectName();
        Map<String, String> connections = ssisInputParameterBean.getConnectionsMap();
        Parser2008XMLFile parser2008XMLFile = ssisInputParameterBean.getParser2008XMLFile();
        Map<String, String> packageLevelVariablesMap = ssisInputParameterBean.getUserReferenceVariablesMap();
        HashMap<String, String> projectLevelVaraiblesMap = new HashMap<>();

        Set<String> keySet = controlFlowLevelMapWithExecutables.keySet();
        Map<String, String> sourceTargeteMap = new HashMap<>();
        HashMap<String, String> allExtendedProperties = new HashMap<>();
        boolean controlFlowMapsCheck = Boolean.parseBoolean(ssisInputParameterBean.getInputParameterBean().isControlflowMappingCheckFlag());
        HashMap<String, String> extendedProperties = new HashMap<>();
        List<Map<String, String>> leanageAndExtendedProp = new ArrayList<>();
        try {
            for (int j = 0; j < controlFlowMissingCompNodeList.getLength(); j++) {
                if (!Constants.textLiteral.equalsIgnoreCase(controlFlowMissingCompNodeList.item(j).getNodeName())) {
                    if (Constants.executableLiteral.equalsIgnoreCase(controlFlowMissingCompNodeList.item(j).getNodeName())) {
                        String objectName = "";
                        String parentObjectName = "";
                        String componentType = "";
                        String objectDtsId = "";
                        NodeList controlFlowChildNodes = controlFlowMissingCompNodeList.item(j).getChildNodes();
                        for (int i = 0; i < controlFlowChildNodes.getLength(); i++) {
                            try {
                                Node childNode = controlFlowChildNodes.item(i);
                                if (StringUtils.isBlank(objectName)) {
                                    objectName = parser2008XMLFile.getTextContentFromXml(childNode, Constants.objectLiteral);
                                }

                                if (StringUtils.isBlank(parentObjectName) && StringUtils.isNotBlank(objectName)) {
                                    parentObjectName = objectName;
                                }

                                if (StringUtils.isBlank(objectDtsId)) {
                                    objectDtsId = parser2008XMLFile.getTextContentFromXml(childNode, Constants.dtsIdLiteral);
                                }
                                if (StringUtils.isBlank(componentType)) {
                                    componentType = parser2008XMLFile.getTextContentFromXml(childNode, "CreationName");
                                    if (StringUtils.isNotBlank(componentType)) {
                                        break;
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        if (eventHandlerExecutables.contains(objectDtsId) || disabledComponentSet.contains(objectDtsId) || (keySet.contains(objectName) && keySet.contains(packageObjectName))) {
                            continue;
                        }

                        boolean flag = checkComponentVisited(keySet, objectDtsId);
                        if (!flag && "STOCK:FOREACHLOOP".equals(componentType) || "STOCK:SEQUENCE".equals(componentType)) {
                            String childObjectName = "";
                            String childObjectDtsId = "";
                            NodeList executablesList = controlFlowMissingCompNodeList.item(j).getChildNodes();
                            for (int k = 0; k < executablesList.getLength(); k++) {

                                if (!executablesList.item(k).getNodeName().equalsIgnoreCase(Constants.textLiteral)) {
                                    if (Constants.executableLiteral.equalsIgnoreCase(executablesList.item(k).getNodeName())) {
                                        NodeList childExecutableList = executablesList.item(k).getChildNodes();
                                        for (int l = 0; l < childExecutableList.getLength(); l++) {
                                            try {
                                                Node childExecutableListNode = childExecutableList.item(l);
                                                if (StringUtils.isBlank(childObjectDtsId)) {
                                                    childObjectDtsId = parser2008XMLFile.getTextContentFromXml(childExecutableListNode, Constants.dtsIdLiteral);
                                                }
                                                if (StringUtils.isBlank(childObjectName)) {
                                                    childObjectName = parser2008XMLFile.getTextContentFromXml(childExecutableListNode, Constants.objectLiteral);
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }
                                            if (eventHandlerExecutables.contains(childObjectDtsId) || disabledComponentSet.contains(childObjectDtsId)) {
                                                continue;
                                            }
                                            if (!removeControlFlowDuplicates.contains(childObjectName) && StringUtils.isNotBlank(childObjectName)) {
                                                sourceTargeteMap.put(childObjectName, "");
                                            }
                                            DynamicVaribleValueReplacement.getChildPackageNameAndVaribleData(childExecutableList, l, childObjectName, parentComponentAndChildPackageNameMap, parameterHashMap);
                                        }
                                        childObjectDtsId = "";
                                        childObjectName = "";
                                    }
                                }
                            }
                            if (keySet.contains(objectName) && keySet.contains(packageObjectName)) {
                                continue;
                            }
                            extendedProperties = parser2008XMLFile.getExtendedProperties(executablesList);
                            extendedProperties = DynamicVaribleValueReplacement.replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(packageLevelVariablesMap, extendedProperties, controlFlowComponentNameSet, projectLevelVaraiblesMap);
                            allExtendedProperties.putAll(extendedProperties);
                            leanageAndExtendedProp.add(sourceTargeteMap);
                            leanageAndExtendedProp.add(extendedProperties);
                            if (controlFlowLevelMapWithExecutables.get(packageObjectName) == null) {
                                sourceTargeteMap.put(objectName, "");
                                leanageAndExtendedProp.add(sourceTargeteMap);
                                leanageAndExtendedProp.add(extendedProperties);
                                controlFlowLevelMapWithExecutables.put(packageObjectName, leanageAndExtendedProp);
                            } else {
                                objectName = objectName.replaceAll(Constants.commonRegularExpression, "_");
                                controlFlowLevelMapWithExecutables.put(objectName, leanageAndExtendedProp);
                            }

                        } else if (componentType.contains("ExecuteSQLTask")) {
                            NodeList executablesList = controlFlowMissingCompNodeList.item(j).getChildNodes();
                            String query = "";
                            String queryconnection = "";
                            String serverName = "";
                            String databaseName = "";
                            for (int k = 0; k < executablesList.getLength(); k++) {
                                if (Constants.objectDataLiteral.equalsIgnoreCase(executablesList.item(k).getNodeName())) {
                                    NodeList executableList = executablesList.item(k).getChildNodes();
                                    for (int l = 0; l < executableList.getLength(); l++) {
                                        try {
                                            if (executableList.item(l).getNodeName().equalsIgnoreCase(Constants.sqlTaskDataLiteral)) {
                                                query = executableList.item(l).getAttributes().getNamedItem(Constants.sqlStatementLiteral).getTextContent();
                                                queryconnection = executableList.item(l).getAttributes().getNamedItem(Constants.sqlConnectionLiteral).getTextContent();
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
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
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                            if (!removeControlFlowDuplicates.contains(objectName)) {
                                sourceTargeteMap.put(objectName, "");
                                extendedProperties.put(objectName + "$queryName", query + Delimiter.delimiter + databaseName + Delimiter.ed_ge_Delimiter + serverName);
                            } else {
                                continue;
                            }
                            extendedProperties = DynamicVaribleValueReplacement.replaceVaribleNameWithActualQueryInTheExtendedPropertesMap(packageLevelVariablesMap, extendedProperties, controlFlowComponentNameSet, projectLevelVaraiblesMap);
                            allExtendedProperties.putAll(extendedProperties);
                            leanageAndExtendedProp.add(sourceTargeteMap);
                            leanageAndExtendedProp.add(extendedProperties);
                            if (controlFlowLevelMapWithExecutables.size() <= 1 && controlFlowLevelMapWithExecutables.get(packageObjectName) == null) {
                                controlFlowLevelMapWithExecutables.put(packageObjectName, leanageAndExtendedProp);
                            }
                        } else if ("SSIS.ExecutePackageTask.2".equalsIgnoreCase(componentType)) {
                            if (!parentComponentAndChildPackageNameMap.containsKey(parentObjectName)) {
                                DynamicVaribleValueReplacement.getChildPackageNameAndVaribleData(controlFlowMissingCompNodeList, j, parentObjectName, parentComponentAndChildPackageNameMap, parameterHashMap);
                            }
                        } else {
                            sourceTargeteMap.put(objectName, "");
                            leanageAndExtendedProp.add(sourceTargeteMap);
                            leanageAndExtendedProp.add(extendedProperties);
                            if (controlFlowLevelMapWithExecutables.size() <= 1 && controlFlowLevelMapWithExecutables.get(packageObjectName) == null) {
                                controlFlowLevelMapWithExecutables.put(packageObjectName, leanageAndExtendedProp);
                            }
                        }

                        leanageAndExtendedProp = new ArrayList<>();
                        sourceTargeteMap = new HashMap<>();
                        extendedProperties = new HashMap<>();
                    }
                }
            }
            if (!controlFlowMapsCheck && controlFlowLevelMapWithExecutables.get(packageObjectName) != null) {
                controlFlowLevelMapWithExecutables.get(packageObjectName).get(1).putAll(allExtendedProperties);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return controlFlowLevelMapWithExecutables;
    }

    /**
     * this method returns Boolean for the given key set and object id ,checking
     * if object id present in the key set or not
     *
     * @param keySet
     * @param objectRefId
     * @return
     */
    public static boolean checkComponentVisited(Set<String> keySet, String objectRefId) {
        boolean flag = false;
        String packageConstant = "Package\\";
        if (objectRefId != null && objectRefId.contains(packageConstant)) {
            objectRefId = objectRefId.replace(packageConstant, "");
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
