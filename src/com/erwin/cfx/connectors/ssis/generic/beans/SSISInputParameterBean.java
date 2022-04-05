/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.beans;

import com.erwin.cfx.connectors.ssis.generic.service.ssis2008.Parser2008XMLFile;
import com.erwin.cfx.connectors.ssis.generic.service.ssis2014.Parser2014XMLFile;
import java.util.Map;
import java.util.Set;
import org.w3c.dom.NodeList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 18-08-2021
 */
public class SSISInputParameterBean {

    private ACPInputParameterBean inputParameterBean;
    private Map<String, String> connectionsMap;
    private Map<String, String> userReferenceVariablesMap;
    private NodeList controlFlowLevelNodeList;
    private String dtsxFileName;
    private String packageObjectName;
    private Set<String> disabledComponentSet;
    private Parser2014XMLFile parser2014XMLFile;
    private Parser2008XMLFile parser2008XMLFile;
    private Map<String, String> packageReferenceVariablesMap;

    public Map<String, String> getPackageReferenceVariablesMap() {
        return packageReferenceVariablesMap;
    }

    public void setPackageReferenceVariablesMap(Map<String, String> packageReferenceVariablesMap) {
        this.packageReferenceVariablesMap = packageReferenceVariablesMap;
    }

    public Parser2008XMLFile getParser2008XMLFile() {
        return parser2008XMLFile;
    }

    public void setParser2008XMLFile(Parser2008XMLFile parser2008XMLFile) {
        this.parser2008XMLFile = parser2008XMLFile;
    }

    public Parser2014XMLFile getParser2014XMLFile() {
        return parser2014XMLFile;
    }

    public void setParser2014XMLFile(Parser2014XMLFile parser2014XMLFile) {
        this.parser2014XMLFile = parser2014XMLFile;
    }

    public Map<String, String> getUserReferenceVariablesMap() {
        return userReferenceVariablesMap;
    }

    public void setUserReferenceVariablesMap(Map<String, String> userReferenceVariablesMap) {
        this.userReferenceVariablesMap = userReferenceVariablesMap;
    }

    public NodeList getControlFlowLevelNodeList() {
        return controlFlowLevelNodeList;
    }

    public void setControlFlowLevelNodeList(NodeList controlFlowLevelNodeList) {
        this.controlFlowLevelNodeList = controlFlowLevelNodeList;
    }

    public String getDtsxFileName() {
        return dtsxFileName;
    }

    public void setDtsxFileName(String dtsxFileName) {
        this.dtsxFileName = dtsxFileName;
    }

    public String getPackageObjectName() {
        return packageObjectName;
    }

    public void setPackageObjectName(String packageObjectName) {
        this.packageObjectName = packageObjectName;
    }

    public Set<String> getDisabledComponentSet() {
        return disabledComponentSet;
    }

    public void setDisabledComponentSet(Set<String> disabledComponentSet) {
        this.disabledComponentSet = disabledComponentSet;
    }

    public ACPInputParameterBean getInputParameterBean() {
        return inputParameterBean;
    }

    public void setInputParameterBean(ACPInputParameterBean inputParameterBean) {
        this.inputParameterBean = inputParameterBean;
    }

    public Map<String, String> getConnectionsMap() {
        return connectionsMap;
    }

    public void setConnectionsMap(Map<String, String> connectionsMap) {
        this.connectionsMap = connectionsMap;
    }

    public Map<String, String> getVariablesMap() {
        return userReferenceVariablesMap;
    }

    public void setVariablesMap(Map<String, String> variablesMap) {
        this.userReferenceVariablesMap = variablesMap;
    }

}
