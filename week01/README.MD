# 使用说明

## 打包步骤

```shell
cd ../

mvn clean install

```

可以在 week-one/target下看到打好的 week-one-1.0.0-SNAPSHOT.jar

## 使用方法

方法有三个参数

1. jar-path: 上传的week01-1.0.0-SNAPSHOT.jar包路径
2. input-path: 要计算的数据文件所在文件夹
3. midout-path: 计算中间数据的存放文件夹
4. output-path: 最终结果数据的存放文件夹

```shell
hadoop jar jar-path input-path midout-path output-path
```

## 运行结果

```shell
[student3@emr-header-1 ~]$ hadoop fs -cat /user/s20032/week2/output/part-r-00000
22/03/11 16:28:17 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
13925057413	63	11058	11121
18320173382	18	9531	9549
13502468823	102	7335	7437
13660577991	9	6960	6969
84138413	16	4116	4132
15013685858	27	3659	3686
15920133257	20	3156	3176
13922314466	12	3008	3020
13726238888	27	2481	2508
13560439658	24	2034	2058
13602846565	12	1938	1950
15989002119	3	1938	1941
18211575961	12	1527	1539
13560436666	15	1116	1131
13826544101	0	264	264
13719199419	0	240	240
13480253104	3	180	183
13926435656	4	132	136
13760778710	2	120	122
```