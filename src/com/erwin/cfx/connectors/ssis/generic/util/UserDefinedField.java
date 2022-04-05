/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssis.generic.util;

import com.ads.api.beans.mm.MappingSpecificationRow;

/**
 *
 * @author Sanjit Sourav
 */
public class UserDefinedField {

    UserDefinedField() {

    }

    /**
     * this method returns mapping specification row by setting given
     * userDefinedNumber and data to a particular user defined map spec
     *
     * @param mapSpec
     * @param userDefinedNumber
     * @param data
     * @return
     */
    public static MappingSpecificationRow setData(MappingSpecificationRow mapSpec, int userDefinedNumber, String data) {

        try {
            switch (userDefinedNumber) {
                case 1:
                    mapSpec.setUserDefined1(data);
                    break;
                case 2:
                    mapSpec.setUserDefined2(data);
                    break;
                case 3:
                    mapSpec.setUserDefined3(data);
                    break;
                case 4:
                    mapSpec.setUserDefined4(data);
                    break;
                case 5:
                    mapSpec.setUserDefined5(data);
                    break;
                case 6:
                    mapSpec.setUserDefined6(data);
                    break;
                case 7:
                    mapSpec.setUserDefined7(data);
                    break;
                case 8:
                    mapSpec.setUserDefined8(data);
                    break;
                case 9:
                    mapSpec.setUserDefined9(data);
                    break;
                case 10:
                    mapSpec.setUserDefined10(data);
                    break;
                case 11:
                    mapSpec.setUserDefined11(data);
                    break;
                case 12:
                    mapSpec.setUserDefined12(data);
                    break;
                case 13:
                    mapSpec.setUserDefined13(data);
                    break;
                case 14:
                    mapSpec.setUserDefined14(data);
                    break;
                case 15:
                    mapSpec.setUserDefined15(data);
                    break;
                case 16:
                    mapSpec.setUserDefined16(data);
                    break;
                case 17:
                    mapSpec.setUserDefined17(data);
                    break;
                case 18:
                    mapSpec.setUserDefined18(data);
                    break;
                case 19:
                    mapSpec.setUserDefined19(data);
                    break;
                case 20:
                    mapSpec.setUserDefined20(data);
                    break;
                case 21:
                    mapSpec.setUserDefined21(data);
                    break;
                case 22:
                    mapSpec.setUserDefined22(data);
                    break;
                case 23:
                    mapSpec.setUserDefined23(data);
                    break;
                case 24:
                    mapSpec.setUserDefined24(data);
                    break;
                case 25:
                    mapSpec.setUserDefined25(data);
                    break;
                case 26:
                    mapSpec.setUserDefined26(data);
                    break;
                case 27:
                    mapSpec.setUserDefined27(data);
                    break;
                case 28:
                    mapSpec.setUserDefined28(data);
                    break;
                case 29:
                    mapSpec.setUserDefined29(data);
                    break;
                case 30:
                    mapSpec.setUserDefined30(data);
                    break;
                case 31:
                    mapSpec.setUserDefined31(data);
                    break;
                case 32:
                    mapSpec.setUserDefined32(data);
                    break;
                case 33:
                    mapSpec.setUserDefined33(data);
                    break;
                case 34:
                    mapSpec.setUserDefined34(data);
                    break;
                case 35:
                    mapSpec.setUserDefined35(data);
                    break;
                case 36:
                    mapSpec.setUserDefined36(data);
                    break;
                case 37:
                    mapSpec.setUserDefined37(data);
                    break;
                case 38:
                    mapSpec.setUserDefined38(data);
                    break;
                case 39:
                    mapSpec.setUserDefined39(data);
                    break;
                case 40:
                    mapSpec.setUserDefined40(data);
                    break;
                case 41:
                    mapSpec.setUserDefined41(data);
                    break;
                case 42:
                    mapSpec.setUserDefined42(data);
                    break;
                case 43:
                    mapSpec.setUserDefined43(data);
                    break;
                case 44:
                    mapSpec.setUserDefined44(data);
                    break;
                case 45:
                    mapSpec.setUserDefined45(data);
                    break;
                case 46:
                    mapSpec.setUserDefined46(data);
                    break;
                case 47:
                    mapSpec.setUserDefined47(data);
                    break;
                case 48:
                    mapSpec.setUserDefined48(data);
                    break;
                case 49:
                    mapSpec.setUserDefined49(data);
                    break;
                case 50:
                    mapSpec.setUserDefined50(data);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mapSpec;
    }
}
