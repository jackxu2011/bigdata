# 使用说明

## 打包步骤

```shell
cd ../

mvn clean install

```

可以在 target下看到打好的 week-one-1.0.0-SNAPSHOT.jar

### 使用方法

方法有三个参数

1. jar-path: 上传的week-one-1.0.0-SNAPSHOT.jar包路径
2. input-path: 要计算的数据文件所在文件夹
3. midout-path: 计算中间数据的存放文件夹
4. output-path: 最终结果数据的存放文件夹

```shell
hadoop jar jar-path input-path midout-path output-path
```