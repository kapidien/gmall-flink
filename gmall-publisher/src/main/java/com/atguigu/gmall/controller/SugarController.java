package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.KeywordStats;
import com.atguigu.gmall.bean.ProductStats;
import com.atguigu.gmall.bean.ProvinceStats;
import com.atguigu.gmall.bean.VisitorStats;
import com.atguigu.gmall.service.KeywordStatsService;
import com.atguigu.gmall.service.ProductStatsService;
import com.atguigu.gmall.service.ProvinceStatsService;
import com.atguigu.gmall.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.swing.text.TabableView;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-25 9:52
 */
@RestController
@RequestMapping("api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;
    @Autowired
    ProvinceStatsService provinceStatsService;
    @Autowired
    VisitorStatsService visitorStatsService;
    @Autowired
    KeywordStatsService keywordStatsService;

    /**
     * {
     *   "status": 0,
     *   "msg": "",
     *   "data": 1201097.8228908153
     * }
     * @param date
     * @return
     */
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date == 0){
            date = genDate();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": " + gmv +
                "}";

        return json;
    }
    private Integer genDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String format = sdf.format(System.currentTimeMillis());
        return Integer.parseInt(format);
    }

    @RequestMapping("/spu")
    public String getProductStatsGroupBySpu(
            @RequestParam(value = "date",defaultValue = "0") Integer date,
            @RequestParam(value = "limit",defaultValue = "10") int limit){

        if (date == 0){
            date = genDate();
        }
        List<ProductStats> statsList = productStatsService.getProductStatsGroupBySpu(date, limit);


        //????????????
        StringBuilder jsonBuilder = new StringBuilder("{" +
                "    \"status\": 0," +
                "    \"data\": {" +
                "        \"columns\": [" +
                "            { \"name\": \"????????????\",  \"id\": \"spu_name\"" +
                "            }," +
                "            { \"name\": \"?????????\", \"id\": \"order_amount\"" +
                "            }," +
                "            { \"name\": \"?????????\", \"id\": \"order_ct\"" +
                "            }" +
                "        ]," +
                "        \"rows\": [");
        //???????????????
        for (int i = 0; i < statsList.size(); i++) {
            ProductStats productStats = statsList.get(i);
            if (i>=1){
                jsonBuilder.append(",");
            }
            jsonBuilder.append("{" +
                    "                \"spu_name\": \"" + productStats.getSpu_name() + "\"," +
                    "                \"order_amount\": \"" + productStats.getOrder_amount() + "\"," +
                    "                \"order_ct\": \"" + productStats.getOrder_ct() + "\"" +
                    "            }");
        }
        jsonBuilder.append("]}}");

        return jsonBuilder.toString();
    }

    @RequestMapping("/category3")
    public String getProductStatsGroupByCategory3(
            @RequestParam(value = "date",defaultValue = "0") Integer date,
            @RequestParam(value = "limit",defaultValue = "4") int limit
    ){
        if (date == 0){
            date = genDate();
        }
        List<ProductStats> category3List = productStatsService.getProductStatsGroupByCategory3(date, limit);

        StringBuilder dataJson = new StringBuilder("{" +
                "    \"status\": 0," +
                "    \"data\": [");
        int i = 0;
        for (ProductStats productStats : category3List) {
            if (i++ > 0){
                dataJson.append(",");
            }
            dataJson.append(" {" +
                    "            \"name\": \""+productStats.getCategory3_name() + "\",");
            dataJson.append(" \"value\": " + productStats.getOrder_amount()+ "" +
                    "        }");

        }
        dataJson.append("]}");
        return dataJson.toString();
    }

    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(
            @RequestParam(value = "date",defaultValue = "0") Integer date,
            @RequestParam(value = "limit",defaultValue = "20") int limit
    ){
        if (date == 0){
            date = genDate();
        }
        List<ProductStats> trademarkList = productStatsService.getProductStatsByTrademark(date, limit);

        ArrayList<String> trademarks = new ArrayList<>();

        ArrayList<BigDecimal> amounts = new ArrayList<>();

        for (ProductStats productStats : trademarkList) {
            trademarks.add(productStats.getTm_name());
            amounts.add(productStats.getOrder_amount());
        }
        String json = "   {" +
                "     \"status\": 0," +
                "     \"data\": {" +
                "         \"categories\": [\"" + StringUtils.join(trademarks,"\",\"") + "\"],"
                + "\"series\": [" +
                "             {" +
                "                 \"data\": [ " + StringUtils.join(amounts,",") + "]}]}}";

        return json;
    }

    @RequestMapping("/province")
    public String getProvinceStats(
            @RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date == 0){
            date = genDate();
        }
        System.out.println(date);
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);

        StringBuilder jsonBuilder = new StringBuilder("{\"status\":0,\"data\":{\"mapData\":[");
        for (int i = 0; i < provinceStatsList.size(); i++) {
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            ProvinceStats provinceStats = provinceStatsList.get(i);
            jsonBuilder.append("{\"name\":\"" + isEmpty(provinceStats.getProvince_name()) + "\",\"value\":" + provinceStats.getOrder_amount() + " }");

        }
        jsonBuilder.append("]}}");
        return jsonBuilder.toString();

    }
    private String isEmpty(String name){
       return  name.length()>1?name:"??????";
    }
    @RequestMapping("/visitor")
    public String getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) date = genDate();
        List<VisitorStats> visitorStatsByNewFlag = visitorStatsService.getVisitorStatsByNewFlag(date);
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();
        //??????????????????????????????????????????????????????????????????
        for (VisitorStats visitorStats : visitorStatsByNewFlag) {
            if (visitorStats.getIs_new().equals("1")) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }
        //???????????????????????????
        String json = "{\"status\":0,\"data\":{\"combineNum\":1,\"columns\":" +
                "[{\"name\":\"??????\",\"id\":\"type\"}," +
                "{\"name\":\"?????????\",\"id\":\"new\"}," +
                "{\"name\":\"?????????\",\"id\":\"old\"}]," +
                "\"rows\":" +
                "[{\"type\":\"?????????(???)\"," +
                "\"new\": " + newVisitorStats.getUv_ct() + "," +
                "\"old\":" + oldVisitorStats.getUv_ct() + "}," +
                "{\"type\":\"???????????????(???)\"," +
                "\"new\":" + newVisitorStats.getPv_ct() + "," +
                "\"old\":" + oldVisitorStats.getPv_ct() + "}," +
                "{\"type\":\"?????????(%)\"," +
                "\"new\":" + newVisitorStats.getUjRate() + "," +
                "\"old\":" + oldVisitorStats.getUjRate() + "}," +
                "{\"type\":\"??????????????????(???)\"," +
                "\"new\":" + newVisitorStats.getDurPerSv() + "," +
                "\"old\":" + oldVisitorStats.getDurPerSv() + "}," +
                "{\"type\":\"?????????????????????(??????)\"," +
                "\"new\":" + newVisitorStats.getPvPerSv() + "," +
                "\"old\":" + oldVisitorStats.getPvPerSv()
                + "}]}}";
        return json;
    }
    @RequestMapping("/hr")
    public String getMidStatsGroupbyHourNewFlag(@RequestParam(value = "date",defaultValue = "0") Integer date ) {
        if(date==0)  date=genDate();
        List<VisitorStats> visitorStatsHrList
                = visitorStatsService.getVisitorStatsByHr(date);

        //??????24?????????
        VisitorStats[] visitorStatsArr=new VisitorStats[24];

        //??????????????????????????????
        for (VisitorStats visitorStats : visitorStatsHrList) {
            visitorStatsArr[visitorStats.getHr()] =visitorStats ;
        }
        List<String> hrList=new ArrayList<>();
        List<Long> uvList=new ArrayList<>();
        List<Long> pvList=new ArrayList<>();
        List<Long> newMidList=new ArrayList<>();

        //??????????????????0-23?????????  ?????????map?????????????????????
        for (int hr = 0; hr <=23 ; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if (visitorStats!=null){
                uvList.add(visitorStats.getUv_ct())   ;
                pvList.add( visitorStats.getPv_ct());
                newMidList.add( visitorStats.getNew_uv());
            }else{ //???????????????????????????
                uvList.add(0L)   ;
                pvList.add( 0L);
                newMidList.add( 0L);
            }
            //???????????????????????????
            hrList.add(String.format("%02d", hr));
        }
        //???????????????
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\""+StringUtils.join(hrList,"\",\"")+ "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":["+ StringUtils.join(uvList,",") +"]}," +
                "{\"name\":\"pv\",\"data\":["+ StringUtils.join(pvList,",") +"]}," +
                "{\"name\":\"?????????\",\"data\":["+ StringUtils.join(newMidList,",") +"]}]}}";
        return  json;
    }
    @RequestMapping("/keyword")
    public String getKeywordStats(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                  @RequestParam(value = "limit", defaultValue = "20") int limit) {
        if (date == 0) {
            date = genDate();
        }
        //????????????
        List<KeywordStats> keywordStatsList
                = keywordStatsService.getKeywordStats(date, limit);
        StringBuilder jsonSb = new StringBuilder("{\"status\":0,\"msg\":\"\",\"data\":[");
        //?????????????????????
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats = keywordStatsList.get(i);
            if (i >= 1) {
                jsonSb.append(",");
            }
            jsonSb.append("{\"name\":\"" + keywordStats.getWord() + "\"," +
                    "\"value\":" + keywordStats.getCt() + "}");
        }
        jsonSb.append("]}");
        return jsonSb.toString();
    }



}
