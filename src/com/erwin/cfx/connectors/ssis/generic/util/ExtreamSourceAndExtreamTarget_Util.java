/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import com.ads.api.beans.mm.MappingSpecificationRow;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Sadar/Dinesh Arasankala/Harika
 * @Date : 30-08-2021
 */
public class ExtreamSourceAndExtreamTarget_Util {

    ExtreamSourceAndExtreamTarget_Util() {

    }

    /**
     * this method returns a list of source tables and target tables
     *
     * @param specificationList
     * @return
     */
    public static List<Set<String>> getExtreamSourceAndTargetTablesListOfSet(ArrayList<MappingSpecificationRow> specificationList) {

        Set<String> sourceTableSet = new HashSet<>();
        Set<String> targetTableSet = new HashSet<>();
        Set<String> extreamTargetTableSet = new HashSet();
        Set<String> extremeSourceSet = new HashSet<>();
        List<Set<String>> sourceAndTargetExtreamList = new ArrayList();
        try {
            for (MappingSpecificationRow mappingSpecificationRow : specificationList) {
                String sourceTableName = mappingSpecificationRow.getSourceTableName();
                String targetTableName = mappingSpecificationRow.getTargetTableName();

                String sourceTableArray[] = sourceTableName.split("\n");
                for (String sourceTable : sourceTableArray) {
                    if (!StringUtils.isBlank(sourceTable)) {
                        sourceTableSet.add(sourceTable.toUpperCase());
                    }

                }
                String targetTableArray[] = targetTableName.split("\n");
                for (String targetTable : targetTableArray) {
                    if (!StringUtils.isBlank(targetTable)) {
                        targetTableSet.add(targetTable.toUpperCase());
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        targetTableSet.stream().filter((trttbl) -> (!(sourceTableSet.contains(trttbl)))).forEachOrdered((trttbl) -> {
            extreamTargetTableSet.add(trttbl.toUpperCase());
        });
        sourceTableSet.stream().filter((srcTable) -> (!(targetTableSet.contains(srcTable)))).forEachOrdered((srcTable) -> {
            extremeSourceSet.add(srcTable.toUpperCase());
        });
        sourceAndTargetExtreamList.add(extremeSourceSet);
        sourceAndTargetExtreamList.add(extreamTargetTableSet);
//        Set<String> extremeSourceSet1 = getExtremeSource(sourceTableSet, targetTableSet);

        return sourceAndTargetExtreamList;
    }

    /**
     * this method returns a set of all extreme source tables
     *
     * @param sourceTableSet
     * @param targetTableSet
     * @return
     */
    public static Set<String> getExtremeSource(Set<String> sourceTableSet, Set<String> targetTableSet) {
        Set<String> extremeSourceSet = new HashSet<>();
        try {
            Iterator sourceSetIterator = sourceTableSet.iterator();
            while (sourceSetIterator.hasNext()) {
                String sourceTable = sourceSetIterator.next().toString();
                if (!targetTableSet.contains(sourceTable)) {
                    extremeSourceSet.add(sourceTable);
                }
            }

        } catch (Exception e) {
        }
        return extremeSourceSet;
    }

    /**
     * this method returns a set of extreme target tables
     *
     * @param mapSpecs
     * @param hierarchyEnvironementName
     * @param intermediateSet
     * @return
     */
    public static Set<String> getExtreamTargetTablesSet(ArrayList<MappingSpecificationRow> mapSpecs, String hierarchyEnvironementName, Set<String> intermediateSet) {
        String srcTbl = "";
        String trtTbl = "";
        Set<String> allSrcTblSet = new HashSet<>();
        Set<String> allTrtTblSet = new HashSet<>();
        Set<String> extreamTargetTableSet = new HashSet<>();
        try {
            for (MappingSpecificationRow mapspec : mapSpecs) {
                srcTbl = mapspec.getSourceTableName();
                trtTbl = mapspec.getTargetTableName();

                String srcTab[] = srcTbl.split("\n");
                for (String sourceTable : srcTab) {

                    allSrcTblSet.add(sourceTable);
                }
                String trtTab[] = trtTbl.split("\n");
                for (String targetTable : trtTab) {

                    allTrtTblSet.add(targetTable);
                }
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
     * this method returns a set of extreme target tables
     *
     * @param componetLineageMap
     * @return
     */
    public static Set<String> getExtreamTargetTablesSetFor2008(LinkedHashMap<String, String> componetLineageMap) {
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

    public static void updateInterMediateEnvironmentName(MappingSpecificationRow mapspec, String tableName, Set<String> interMediateSet, String hierarchyEnvironementName, String envName, boolean isSource) {

        if (interMediateSet.contains(tableName.toUpperCase())) {
            String[] sourceEnvNameArray = envName.split("\n");
            envName = "";
            for (String srcEnv : sourceEnvNameArray) {
                StringBuilder stringBuilder = new StringBuilder();
                if (StringUtils.isNotBlank(envName)) {
                    envName = stringBuilder.append(envName).append("\n").append(hierarchyEnvironementName).toString();
                } else {
                    envName = hierarchyEnvironementName;
                }

            }
            if (isSource) {
                mapspec.setSourceSystemEnvironmentName(envName);
            } else {
                mapspec.setTargetSystemEnvironmentName(envName);
            }

        }
    }

}
