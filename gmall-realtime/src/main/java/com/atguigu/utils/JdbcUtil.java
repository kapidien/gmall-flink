package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-22 14:46
 */
public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception{
        //创建集合存放最终结果
        ArrayList<T> resultList = new ArrayList<>();

        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        while (resultSet.next()){
           //创建T对象
            T t = clz.newInstance();

            //遍历单行数据中的列，获取列名和值赋值给对象
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);

                if (underScoreToCamel){
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }
                BeanUtils.setProperty(t,columnName,value);
            }
        resultList.add(t);

        }
        resultSet.close();
        preparedStatement.close();

        //返回结果
        return resultList;

    }

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        List<JSONObject> queryList = queryList(connection, "select * from GMALL210927_REALTIME.BASE_TRADEMARK", JSONObject.class, true);

        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }

    }
}
