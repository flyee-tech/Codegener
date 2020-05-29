import os
import re
import shutil
from configparser import ConfigParser

import pandas as pd
import pymysql

project_path = '/Users/Peiel/WorkSpace/oulu/oulu/src/main/java/'
model_path = project_path + 'com/niuban/oulu/automation/model/'
mapper_path = project_path + 'com/niuban/oulu/automation/mapper/'
package_base = 'com.niuban.oulu.automation.'
package_model = package_base + 'model'
package_mapper = package_base + 'mapper'


def getTabelDesc(conn, tableName):
    df = pd.read_sql("desc %s" % tableName, con=conn)
    return tableName, df


def tableNameConvert(table_name, is_tuofeng=0):
    """
    首字母大写
    去掉下划线，并大写后一个字符
    加上TO
    """
    javaClassName = ''
    upIdx = False
    for idx, ch in enumerate(table_name):
        if idx == 0 and is_tuofeng == 0:
            javaClassName += ch.upper()
            continue
        if ch == '_':
            upIdx = True
            continue
        javaClassName += ch if not upIdx else ch.upper()
        upIdx = False
    # if is_tuofeng == 0:
    #     javaClassName += 'TO'
    return javaClassName


def getJavaFiledType(table_type):
    table_type = re.sub(u"\\(.*?\\)|{.*?}|\\[.*?]", "", table_type)
    cfg = ConfigParser()
    cfg.read('conf.ini')
    v = cfg.get('sql2java', table_type)
    return v


def genEntity(table_name, df_desc):
    print(table_name)
    print(df_desc)
    content = ''
    content += 'package %s;\n' % package_model

    content += '''
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
'''
    content += '''
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "%s")
''' % table_name
    content += 'public class %s {\n' % tableNameConvert(table_name)

    for index, row in df_desc.iterrows():
        field = row['Field']
        t = row['Type']
        if field == 'id':
            content += '    @TableId(type = IdType.AUTO)\n'
        content += '    private %s %s;\n' % (getJavaFiledType(t), tableNameConvert(field, is_tuofeng=1))
    content += '}\n'
    return tableNameConvert(table_name), content


def writeFile(f, content):
    with open(f, 'w') as f:
        f.write(content)


def genDao(className):
    content = '''
package %s;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import %s;
import org.springframework.stereotype.Repository;

@Repository
public interface %s extends BaseMapper<%s> {

}
''' % (package_mapper, package_model + '.' + className, className + "Mapper", className)
    print(content)
    return className + "Mapper", content


def main():
    try:
        cfg = ConfigParser()
        cfg.read('mysql_conf.ini')

        host = cfg.get('MYSQL', 'host')
        user = cfg.get('MYSQL', 'user')
        passwd = cfg.get('MYSQL', 'passwd')
        db = cfg.get('MYSQL', 'db')

        conn = pymysql.connect(host=str(host), port=3306, user=str(user), passwd=str(passwd), db=str(db))
        df = pd.read_sql('show tables;', con=conn)
        # df = df[df.Tables_in_oluplaza_dev.str.startswith("user")]

        # df.at['1000'] = ['mobile_send_email']
        # df.loc['1000'] = 'mobile_send_email'

        for index, row in df.iterrows():
            table_name, df_desc = getTabelDesc(conn, row['Tables_in_oluplaza_dev'])
            className, entiry_content = genEntity(table_name, df_desc)
            writeFile(model_path + className + '.java', entiry_content)
            mapperName, daoContent = genDao(className)
            writeFile(mapper_path + mapperName + '.java', daoContent)



    finally:
        conn.close()


if __name__ == '__main__':
    shutil.rmtree(model_path)
    os.mkdir(model_path)
    shutil.rmtree(mapper_path)
    os.mkdir(mapper_path)
    main()
    # print(tableNameConvertJavaClassName("user_car"))
    # print(getJavaFiledType('int(10)'))
    # genDao('User')
