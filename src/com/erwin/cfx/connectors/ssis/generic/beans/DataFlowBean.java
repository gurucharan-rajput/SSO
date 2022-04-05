/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.beans;

import java.util.Map;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 19-08-2021
 */
public class DataFlowBean {

    private ACPInputParameterBean acpInputParameterBean;
    private SSISInputParameterBean ssisInputParameterBean;
    private NodeList componentList;
    private Map<String, String> queryMap;
    private String dataflowName;
    private String actualDataFlowName;
    private Map<String, String> componentConnections;
    private Map<String, String> componentNamerefIds;
    private Map<String, String> pathInputsOutputsMap;
    private String fileNameFromForEachLoop;
    private String userOrProjectReference;
    private String userOrProjectName;
    private String sheetName;
    private String accessMode;
    private String rawSourceFileName;
    private String rawSourceVariableFileName;
    private String collectionNameForODataSource;
    private String xmlData;
    private String xmlDataVariable;
    private String fileExtension;
    private boolean isFileComponent;
    private String fileDataBaseType;

    public String getFileDataBaseType() {
        return fileDataBaseType;
    }

    public void setFileDataBaseType(String fileDataBaseType) {
        this.fileDataBaseType = fileDataBaseType;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public boolean isIsFileComponent() {
        return isFileComponent;
    }

    public void setIsFileComponent(boolean isFileComponent) {
        this.isFileComponent = isFileComponent;
    }

    public String getXmlData() {
        return xmlData;
    }

    public void setXmlData(String xmlData) {
        this.xmlData = xmlData;
    }

    public String getXmlDataVariable() {
        return xmlDataVariable;
    }

    public void setXmlDataVariable(String xmlDataVariable) {
        this.xmlDataVariable = xmlDataVariable;
    }

    public String getRawSourceFileName() {
        return rawSourceFileName;
    }

    public void setRawSourceFileName(String rawSourceFileName) {
        this.rawSourceFileName = rawSourceFileName;
    }

    public String getRawSourceVariableFileName() {
        return rawSourceVariableFileName;
    }

    public void setRawSourceVariableFileName(String rawSourceVariableFileName) {
        this.rawSourceVariableFileName = rawSourceVariableFileName;
    }

    public String getCollectionNameForODataSource() {
        return collectionNameForODataSource;
    }

    public void setCollectionNameForODataSource(String collectionNameForODataSource) {
        this.collectionNameForODataSource = collectionNameForODataSource;
    }

    public String getAccessMode() {
        return accessMode;
    }

    public void setAccessMode(String accessMode) {
        this.accessMode = accessMode;
    }

    public String getSheetName() {
        return sheetName;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }

    public String getActualDataFlowName() {
        return actualDataFlowName;
    }

    public void setActualDataFlowName(String actualDataFlowName) {
        this.actualDataFlowName = actualDataFlowName;
    }

    public ACPInputParameterBean getAcpInputParameterBean() {
        return acpInputParameterBean;
    }

    public void setAcpInputParameterBean(ACPInputParameterBean acpInputParameterBean) {
        this.acpInputParameterBean = acpInputParameterBean;
    }

    public SSISInputParameterBean getSsisInputParameterBean() {
        return ssisInputParameterBean;
    }

    public void setSsisInputParameterBean(SSISInputParameterBean ssisInputParameterBean) {
        this.ssisInputParameterBean = ssisInputParameterBean;
    }

    public NodeList getComponentList() {
        return componentList;
    }

    public void setComponentList(NodeList componentList) {
        this.componentList = componentList;
    }

    public Map<String, String> getQueryMap() {
        return queryMap;
    }

    public void setQueryMap(Map<String, String> queryMap) {
        this.queryMap = queryMap;
    }

    public String getDataflowName() {
        return dataflowName;
    }

    public void setDataflowName(String dataflowName) {
        this.dataflowName = dataflowName;
    }

    public Map<String, String> getComponentConnections() {
        return componentConnections;
    }

    public void setComponentConnections(Map<String, String> componentConnections) {
        this.componentConnections = componentConnections;
    }

    public Map<String, String> getComponentNamerefIds() {
        return componentNamerefIds;
    }

    public void setComponentNamerefIds(Map<String, String> componentNamerefIds) {
        this.componentNamerefIds = componentNamerefIds;
    }

    public Map<String, String> getPathInputsOutputsMap() {
        return pathInputsOutputsMap;
    }

    public void setPathInputsOutputsMap(Map<String, String> pathInputsOutputsMap) {
        this.pathInputsOutputsMap = pathInputsOutputsMap;
    }

    public String getFileNameFromForEachLoop() {
        return fileNameFromForEachLoop;
    }

    public void setFileNameFromForEachLoop(String fileNameFromForEachLoop) {
        this.fileNameFromForEachLoop = fileNameFromForEachLoop;
    }

    public String getUserOrProjectReference() {
        return userOrProjectReference;
    }

    public void setUserOrProjectReference(String userOrProjectReference) {
        this.userOrProjectReference = userOrProjectReference;
    }

    public String getUserOrProjectName() {
        return userOrProjectName;
    }

    public void setUserOrProjectName(String userOrProjectName) {
        this.userOrProjectName = userOrProjectName;
    }

}
