/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.beans;

import com.ads.api.beans.mm.MappingSpecificationRow;
import java.util.ArrayList;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date 28-08-2021
 */
public class SyncUpBean {

    private String tableName;
    private String databaseName;
    private String serverName;
    private String mapName;
    private String schemaName;
    private String columns;
    private boolean isFileTypeComponent;
    private String fileDatabaseType;
    private String excellFileName;
    private ACPInputParameterBean acpInputParameterBean;

    private String json;
    private ArrayList<MappingSpecificationRow> inputSpecList;
    private String heirarchyFolderEnvName;

    public String getHeirarchyFolderEnvName() {
        return heirarchyFolderEnvName;
    }

    public void setHeirarchyFolderEnvName(String heirarchyFolderEnvName) {
        this.heirarchyFolderEnvName = heirarchyFolderEnvName;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public ArrayList<MappingSpecificationRow> getInputSpecList() {
        return inputSpecList;
    }

    public void setInputSpecList(ArrayList<MappingSpecificationRow> inputSpecList) {
        this.inputSpecList = inputSpecList;
    }

    public ACPInputParameterBean getAcpInputParameterBean() {
        return acpInputParameterBean;
    }

    public void setAcpInputParameterBean(ACPInputParameterBean acpInputParameterBean) {
        this.acpInputParameterBean = acpInputParameterBean;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public boolean isIsFileTypeComponent() {
        return isFileTypeComponent;
    }

    public void setIsFileTypeComponent(boolean isFileTypeComponent) {
        this.isFileTypeComponent = isFileTypeComponent;
    }

    public String getFileDatabaseType() {
        return fileDatabaseType;
    }

    public void setFileDatabaseType(String fileDatabaseType) {
        this.fileDatabaseType = fileDatabaseType;
    }

    public String getExcellFileName() {
        return excellFileName;
    }

    public void setExcellFileName(String excellFileName) {
        this.excellFileName = excellFileName;
    }

}
