#### datax自动化配置脚本参数如下：

1. mysqlreader

   ```shell
   \--readerPlugin mysqlreader 
   \--ipAddress xxx 
   \--port xxx 
   \--dataBase xxx 
   \--userName xxx 
   \--passWord xxx 
   \--querySql xxx 
   \--splitPk xxx 
   \--separator 
   
   ##########################################解释
   \--readerPlugin ## readerc插件名称（必须）
   --ipAddress     ## ip地址 （必须）
   --port          ## 端口号 （非必须：默认 3306）
   --dataBase      ## 数据库（必须）
   --userName      ## 账号（必须）
   --passWord      ## 密码（必须）
   --querySql      ## 需要查询的sql（必须）
   --splitPk       ## 并行切分的列，一般根据设置为主键id（非必须）
   --separator     ## 用于和writerPlugin进行分割所设置（参数key必须写，参数value不需要写）
   ```

2. hivereader

   ```shell
   \--readerPlugin hivereader 
   \--hiveSql xxx 
   \--defaultFs xxx 
   \--tmpDataBase xxx 
   \--tmpPath xxx 
   \--hiveSetSql XXX
   \--separator 
   ##########################################解释
   \--readerPlugin  ## readerc插件名称（必须）
   --hiveSql        ## 需要读取的sql语句（必须）
   --defaultFs      ## 链接hdfs的文件地址（必须）
   --tmpDataBase    ## 临时中间表的库名（非必须，默认default），最好写
   --tmpPath        ## hive临时表所在数据库的hdfs路径(需写权限)（必须)
   --hiveSetSql     ## 用于 set 一些参数（非必须）
   --separator      ## 用于和writerPlugin进行分割所设置（参数key必须写，参数value不需要写）
   ```

3. clickhousereader

   ```shell
   \--readerPlugin clickhousereader
   \--userName xxx 
   \--passWord xxx 
   \--querySql xxx 
   \--ipAddress xxx 
   \--port xxx 
   \--dataBase xxx 
   \--separator 
   ##########################################解释
   \--readerPlugin   ## readerc插件名称（必须）
   --userName        ## 账号（必须）
   --passWord        ## 密码（必须）
   --querySql        ## 需要查询的sql（必须）
   --ipAddress       ## ip地址 （必须）
   --port            ## 端口号 （非必须：默认 8123）
   --dataBase        ## 数据库（必须）
   ```

   

4. mysqlwriter

   ```shell
   \--writerPlugin mysqlwriter 
   \--column pt,po,py 
   \--ipAddress 008.bg.qkt 
   \--port 3306 
   \--dataBase test 
   \--table ptest 
   \--preSql truncate table ptest 
   \--passWord quicktron123456 
   \--userName root 
   \--writeMode update
   ##########################################解释
   \--writerPlugin   ## writer插件名称（必须）
   \--column         ## 目的表需要写入数据的字段,字段之间用英文逗号分隔(必须)
   \--ipAddress      ## ip地址 （必须）
   \--port           ## 端口号 （非必须：默认 3306）
   \--dataBase       ## 数据库（必须）
   \--table          ## 写入的表名（必须）
   \--preSql         ## 写入前执行的前置sql（非必须）
   \--passWord       ## 密码（必须）
   \--userName       ## 账号（必须）
   \--writeMode      ## insert/replace/update(默认insert)
   ```

5. hivewriter

   ```shell
   \--writerPlugin hivewriter 
   \--dataBase xxx 
   \--table xxx 
   \--defaultFs xxx 
   \--hiveSetSql XXX
   \--writeMode xxx 
   \--preSql xxx 
   \--tmpDataBase xxx 
   \--tmpPath xxx 
   \--partition xxx 
   \--column xxx 
   ##########################################解释
   --writerPlugin   ## writer插件名称（必须）
   --dataBase 		 ## 数据库名称（必须）
   --table          ## 表名（必须）
   --defaultFs      ## 链接hdfs的文件地址（必须）
   --hiveSetSql     ## 用于 set 一些参数（非必须）
   --writeMode      ## 追加或者覆盖数据（非必须，默认insert）(支持insert/overwrite)
   --preSql         ## 写入数据到目的表前，会先执行这里的标准HiveQL语句（非必须）
   --tmpDataBase    ## 临时中间表的库名（非必须，默认与写入的库名一致）
   --tmpPath        ## hive临时表所在数据库的hdfs路径(需写权限)（必须）
   --partition      ## 分区字段,逗号分隔（非必须）示例： dt 或 dt1,dt2
   --column         ## 手动指定hive表列信息,按照建表的字段顺序列出来,包括分区列信息（必须）## 如若遇到 reader字段为时间类型，需在字段后面补 1(date)或者2(timestamp) 例如（column:1）
   
   
   ```

6. clickhousewriter

   ```shell
   \--writerPlugin clickhousewriter 
   \--column pt,po,py 
   \--ipAddress 005.bg.qkt 
   \--port 8123 
   \--dataBase test 
   \--table ptest 
   \--passWord quicktron123456 
   \--userName root 
   \--preSql select * from xxx
   ##########################################解释
   \--writerPlugin   ## writer插件名称（必须）
   \--column         ## 目的表需要写入数据的字段,字段之间用英文逗号分隔(必须)
   \--ipAddress      ## ip地址 （必须）
   \--port           ## 端口号 （非必须：默认 8123）
   \--dataBase       ## 数据库（必须）
   \--table          ## 写入的表名（必须）
   \--passWord       ## 密码（必须）
   \--userName       ## 账号（必须）
   \--preSql        ## 写入数据到目的表前，会先执行这里的标准CKHQL语句（非必须）
   
   ```
   
   

#### 公用参数

```shell
--channel 2          ## 设置内部并发数量,默认值是1（非必须）
```



#### 例子

```shell
start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select * from dim.dim_day_date 
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin hivewriter 
\--dataBase tmp 
\--table dim_day_date_test 
\--defaultFs hdfs://001.bg.qkt:8020
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--column days,year_date, month_date,day_date,quarter_date,week_date,week_year_date,is_month_begin,is_month_end,day_type
\--channel 1" "json文件名"


##########################################解释
start-datax.sh       ## 此脚本为启动json的写的脚本

## 每个 英文 双引号 "" 表示一个参数，每个参数之间用空格分开
第一个参数必须为 json（reader和writer）所需的参数  ## 此参数必须
第二个参数必须为 json文件名称（自己填写）          ## 此参数必须  
第三个参数为 时间（比如 2022-08-25）             ## 此参数非必须（默认为当前时间的前一天） 
```

