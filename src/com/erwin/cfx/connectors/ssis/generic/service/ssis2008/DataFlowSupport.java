/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.service.ssis2008;

import com.erwin.cfx.connectors.ssis.generic.util.Delimiter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author THarika
 */
public class DataFlowSupport {

    /**
     * this method returns type of component based on the given component class
     * Id and contact Info
     *
     * @param componentClassID
     * @param contactInfo
     * @return
     */
    public static String componentsTypeDecision(String componentClassID, String contactInfo) {

        String comType = "";
        if (contactInfo.contains("OLE DB Source")) {
            contactInfo = contactInfo.replace(" ", "").toUpperCase();
        }

        if ("Microsoft.RowCount".equals(componentClassID)) {
            return comType;
        } else if ((componentClassID.contains("Microsoft.ManagedComponentHost")
                || componentClassID.contains("Microsoft.ExcelSource")
                || componentClassID.contains("Microsoft.FlatFileSource")
                || componentClassID.contains("Microsoft.OLEDBSource")
                || componentClassID.contains("{165A526D-D5DE-47FF-96A6-F8274C19826B}")
                || componentClassID.contains("Microsoft.RawSource")
                || componentClassID.contains("Attunity.SSISODBCSrc"))
                && (contactInfo.contains("Consumes data from SQL Server, OLE DB, ODBC, or Oracle, using the corresponding .NET Framework data provider")
                || contactInfo.contains("Attunity Ltd.; All Rights Reserved; http://www.attunity.com;")
                || contactInfo.contains("Excel Source;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation;")
                || contactInfo.contains("Excel Source")
                || contactInfo.contains("Flat File Source;Microsoft Corporation; Microsoft SQL Server")
                || contactInfo.contains("Flat File Source")
                || contactInfo.contains("Flat File Source;Microsoft Corporation; Microsoft SqlServer v10;")
                || contactInfo.contains("OLEDBSOURCE;MICROSOFTCORPORATION;MICROSOFTSQLSERVER")
                //                || contactInfo.contains("OLE DB Source;Microsoft Corporation;Microsoft SqlServer")
                || contactInfo.contains("Reads raw data from a flat file that was previously written by the Raw File destination.")
                || contactInfo.contains("Extracts data from an XML file. For example, extract catalog data from an XML file that represents catalogs and")
                || contactInfo.contains("ODBC Source;Connector for Open Database Connectivity (ODBC) by Attunity")
                || contactInfo.contains("MicrosoftContactInfo") || contactInfo.contains("Source OLE DB;Microsoft Corporation; Microsoft SqlServer v10; (C) Microsoft Corporation; Tous droits réservés; http://www.microsoft.com/sql/support;7")
                || contactInfo.contains("OData Source Component;Microsoft Corporation; Microsoft SQL Server;"))) {
            comType = "Source";

        } else if ((componentClassID.contains("Microsoft.ManagedComponentHost")
                || componentClassID.contains("Microsoft.PXPipelineProcessDM")
                || componentClassID.contains("Microsoft.ManagedComponentHost")
                || componentClassID.contains("Microsoft.ManagedComponentHost")
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
                || componentClassID.contains("Microsoft.ManagedComponentHost")
                || componentClassID.contains("Microsoft.SQLServerDestination")
                || componentClassID.contains("Attunity.SSISODBCDst")
                || componentClassID.contains("Microsoft.Multicast")
                || componentClassID.contains("Microsoft.ManagedComponentHost")
                || componentClassID.contains("Microsoft.UnionAll")
                || componentClassID.contains("Microsoft.OLEDBCommand"))
                && (contactInfo.contains("Loads data into an ADO.NET-compliant database that uses a database table or view")
                || contactInfo.contains("Exposes data in a data flow to other applications by using the ADO.NET DataReader interface")
                || contactInfo.contains("Excel Destination")
                || contactInfo.contains("Upsert Destinaton")
                || contactInfo.contains("Flat File Destination")
                || contactInfo.contains("OLE DB Destination")
                || contactInfo.contains("Writes raw data that will not require parsing or translation")
                || contactInfo.contains("Recordset Destination")
                || contactInfo.contains("Writes data to a table in a SQL Server Compact database")
                || contactInfo.contains("SQL Server Destination")
                || contactInfo.contains("No contactInfo")
                || contactInfo.contains("OLE DB Command") || contactInfo.contains("Destination OLE DB;Microsoft Corporation; Microsoft SqlServer v10; (C) Microsoft Corporation; Tous droits réservés; http://www.microsoft.com/sql/support;4")
                || contactInfo.contains("ODBC Destination;Connector for Open Database Connectivity (ODBC) by Attunity"))) {
            comType = "Target";
        } else if (componentClassID.contains("Microsoft.Lookup")
                || contactInfo.contains("Lookup;Microsoft Corporation")) {

            comType = "Lookup";
        }

        return comType;
    }

    /**
     * this method returns updatedPathLineageMap by mapping target component Id
     * to source component Id
     *
     * @param componentConnections
     * @return
     */
    public static LinkedHashMap<String, String> prepareComponentLineageMap(Map<String, String> componentConnections) {

        LinkedHashMap<String, String> updatedPathLineageMap = new LinkedHashMap<>();
        try {

            for (Map.Entry<String, String> pathsEntrySet : componentConnections.entrySet()) {

                String inputId = pathsEntrySet.getKey();
                String outputId = pathsEntrySet.getValue();

                String targetComponentId = "";
                String sourceComponentId = "";

                if (PrepareDataFlowLineage2008.inputIdAndOutPutIdAskeyAndComponentIdAsValue.get(inputId) != null) {
                    targetComponentId = PrepareDataFlowLineage2008.inputIdAndOutPutIdAskeyAndComponentIdAsValue.get(inputId);
                }
                if (PrepareDataFlowLineage2008.inputIdAndOutPutIdAskeyAndComponentIdAsValue.get(outputId) != null) {
                    sourceComponentId = PrepareDataFlowLineage2008.inputIdAndOutPutIdAskeyAndComponentIdAsValue.get(outputId);
                }
                if (StringUtils.isNotBlank(targetComponentId) && StringUtils.isNotBlank(sourceComponentId)) {

                    if (PrepareDataFlowLineage2008.componentLineageMap.get(targetComponentId) != null) {
                        String oldSourceCompnentId = PrepareDataFlowLineage2008.componentLineageMap.get(targetComponentId);
                        PrepareDataFlowLineage2008.componentLineageMap.put(targetComponentId, oldSourceCompnentId + Delimiter.delimiter + sourceComponentId);
                    } else {
                        PrepareDataFlowLineage2008.componentLineageMap.put(targetComponentId, sourceComponentId);
                    }
                }

            }
            Set<String> extreamTargetComponentIdSet = getExtreamTargetTablesSetFor2008(PrepareDataFlowLineage2008.componentLineageMap);

            for (String extreamComponentId : extreamTargetComponentIdSet) {

                String inputSetId = "";

                if (PrepareDataFlowLineage2008.componentIdAndItsInputSet.get(extreamComponentId) != null) {
                    inputSetId = PrepareDataFlowLineage2008.componentIdAndItsInputSet.get(extreamComponentId);
                }
                String inputSetIdArray[] = inputSetId.split(Delimiter.delimiter);
                for (String individualInputSetId : inputSetIdArray) {
                    String outPutSetId = "";
                    if (componentConnections.get(individualInputSetId) != null) {
                        outPutSetId = componentConnections.get(individualInputSetId);
                    }
                    if (StringUtils.isNotBlank(individualInputSetId)) {
                        updatedPathLineageMap.put(individualInputSetId, outPutSetId);
                    }

                }
                iterateComponentLineageMapToCreateTheUpdatedPathLineage(PrepareDataFlowLineage2008.componentLineageMap, updatedPathLineageMap, extreamComponentId, componentConnections);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return updatedPathLineageMap;
    }

    /**
     * this method returns a set containing the extreme target components
     *
     * @param componetLineageMap
     * @return
     */
    private static Set<String> getExtreamTargetTablesSetFor2008(Map<String, String> componetLineageMap) {
        String sourceTable = "";
        String targetTable = "";
        Set<String> allSrcTblSet = new HashSet();
        Set<String> allTrtTblSet = new HashSet();
        Set<String> extreamTargetTableSet = new HashSet();
        try {
            for (Map.Entry<String, String> componentLineageEntrySet : componetLineageMap.entrySet()) {
                targetTable = componentLineageEntrySet.getKey();
                sourceTable = componentLineageEntrySet.getValue();

                String srcTab[] = sourceTable.split(Delimiter.delimiter);
                for (String sourceTableSplit : srcTab) {
                    allSrcTblSet.add(sourceTableSplit);
                }

                allTrtTblSet.add(targetTable);

            }
            allTrtTblSet.stream().filter((trttbl) -> (!(allSrcTblSet.contains(trttbl)))).forEachOrdered((trttbl) -> {
                extreamTargetTableSet.add(trttbl.toUpperCase());
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        return extreamTargetTableSet;

    }

    /**
     * this method updates updatedPathLineageMap by mapping individual source
     * component Id to target component Id
     *
     * @param componentLineageMap
     * @param updatedPathLineageMap
     * @param extreamTargetComponentId
     * @param componentConnections
     */
    public static void iterateComponentLineageMapToCreateTheUpdatedPathLineage(Map<String, String> componentLineageMap, LinkedHashMap<String, String> updatedPathLineageMap, String extreamTargetComponentId, Map<String, String> componentConnections) {

        if (componentLineageMap.get(extreamTargetComponentId) != null) {

            String sourceComponentId = "";
            sourceComponentId = componentLineageMap.get(extreamTargetComponentId);

            String sourceComponentIdArray[] = sourceComponentId.split(Delimiter.delimiter);
            for (String individualSourceComponentId : sourceComponentIdArray) {

                String inputSetId = "";

                if (PrepareDataFlowLineage2008.componentIdAndItsInputSet.get(individualSourceComponentId) != null) {
                    inputSetId = PrepareDataFlowLineage2008.componentIdAndItsInputSet.get(individualSourceComponentId);
                }
                String inputSetIdArray[] = inputSetId.split(Delimiter.delimiter);
                for (String individualInputSetId : inputSetIdArray) {
                    String outPutSetId = "";
                    if (componentConnections.get(individualInputSetId) != null) {
                        outPutSetId = componentConnections.get(individualInputSetId);
                    }
                    if (StringUtils.isNotBlank(individualInputSetId)) {
                        updatedPathLineageMap.put(individualInputSetId, outPutSetId);
                    }
                }
                iterateComponentLineageMapToCreateTheUpdatedPathLineage(componentLineageMap, updatedPathLineageMap, individualSourceComponentId, componentConnections);
            }
        } else {

            extreamTargetComponentId = "";
        }
    }

    /**
     * this method splits the given input value and returns the string present
     * at first index
     *
     * @param outputColumnLineageId
     * @return
     */
    public static String splitLineages(String outputColumnLineageId) {
        String allLineages = "";
        String[] outputColumnSplit = outputColumnLineageId.split("#");
        int splitLength = outputColumnSplit.length;
        try {
            if (splitLength > 2) {
                allLineages = outputColumnSplit[splitLength - 1];
            } else {
                allLineages = outputColumnSplit[1];
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return allLineages;
    }
}
