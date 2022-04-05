/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.icc.util.RequestStatus;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Chirishma Kakarla / Dinesh Arasankala
 */
public class CreateMappingVersion {

    CreateMappingVersion() {

    }

    /**
     * this method versions the already existing map
     *
     * @param projectId
     * @param mappingName
     * @param parentSubectId
     * @param mapspecList
     * @param mappingManagerUtil
     * @param keyValueUtil
     * @return
     */
    public static int creatingMapVersionForIncremental(int projectId, String mappingName, int parentSubectId, ArrayList<MappingSpecificationRow> mapspecList, MappingManagerUtil mappingManagerUtil, KeyValueUtil keyValueUtil) {
        Mapping latestMappingObj = null;
        List<Float> latestMappingVersion = null;
        Float updateMappingVersion = 0.0f;
        RequestStatus resultStatus = null;
        int latestMapId = 0;
        try {
            latestMappingVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil, projectId);
            updateMappingVersion = latestMappingVersion.get(latestMappingVersion.size() - 1);
            Mapping mappingObj = new Mapping();
            try {

                if (parentSubectId > 0) {
                    mappingObj = mappingManagerUtil.getMapping(parentSubectId, Node.NodeType.MM_SUBJECT, mappingName, updateMappingVersion);
                } else {
                    mappingObj = mappingManagerUtil.getMapping(projectId, Node.NodeType.MM_PROJECT, mappingName, updateMappingVersion);
                }

            } catch (Exception e) {
                MappingManagerUtilAutomation.writeExeceptionLog(e, "creatingMapVersionForIncremental(-,-)");
                e.printStackTrace();
            }

            int mappId = mappingObj.getMappingId();
            mappingObj.setProjectId(projectId);
            mappingObj.setSubjectId(parentSubectId);
            mappingObj.setMappingId(mappId);
            mappingObj.setChangedDescription("Mapping " + mappingName + " changed! as Version Done: " + updateMappingVersion);

            String status = mappingManagerUtil.versionMapping(mappingObj).getStatusMessage();

            List<Float> latestMapVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil, projectId);
            Float latestMapV = latestMapVersion.get(latestMapVersion.size() - 1);

            try {

                if (parentSubectId > 0) {
                    latestMappingObj = mappingManagerUtil.getMapping(parentSubectId, Node.NodeType.MM_SUBJECT, mappingName, latestMapV);
                } else {
                    latestMappingObj = mappingManagerUtil.getMapping(projectId, Node.NodeType.MM_PROJECT, mappingName, latestMapV);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            if (latestMappingObj != null) {
                latestMapId = latestMappingObj.getMappingId();
                mappingManagerUtil.deleteMappingSpecifications(latestMapId);
                keyValueUtil.deleteKeyValues("8", latestMapId + "");
            }
            resultStatus = mappingManagerUtil.addMappingSpecifications(latestMapId, mapspecList);
        } catch (Exception e) {
            StringWriter exceptionLog = new StringWriter();
            e.printStackTrace(new PrintWriter(exceptionLog));
            MappingManagerUtilAutomation.writeExeceptionLog(e, "creatingMapVersionForIncremental(-,-)");

        }
//        return resultStatus.getStatusMessage() + MappingCreator.DELIMITER + latestMapId;//FIX T5 Adding Delimiter
        return latestMapId;

    }

    /**
     * this method returns the list of mapping version numbers for a given map
     *
     * @param subjectId
     * @param mapName
     * @param mappingManagerUtil
     * @param projectId
     * @return
     */
    public static List<Float> getMappingVersions(int subjectId, String mapName, MappingManagerUtil mappingManagerUtil, int projectId) {
        List<Float> mapVersionList = new ArrayList<>();
        try {

            ArrayList<Mapping> mappings = null;

            try {

                if (subjectId > 0) {
                    mappings = mappingManagerUtil.getMappings(subjectId, Node.NodeType.MM_SUBJECT);
                } else {
                    mappings = mappingManagerUtil.getMappings(projectId, Node.NodeType.MM_PROJECT);
                }

            } catch (Exception e) {
                MappingManagerUtilAutomation.writeExeceptionLog(e, "getMappingVersions(-,-)");
                e.printStackTrace();
            }

            if (mappings != null && !mappings.isEmpty()) {
                for (int map = 0; map < mappings.size(); map++) {
                    String mappingName = mappings.get(map).getMappingName();
                    float mappingVersion = mappings.get(map).getMappingSpecVersion();
                    if (mapName.equalsIgnoreCase(mappingName)) {
                        mapVersionList.add(mappingVersion);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            MappingManagerUtilAutomation.writeExeceptionLog(e, "getMappingVersions(-,-)");
        }
        return mapVersionList;
    }

}
