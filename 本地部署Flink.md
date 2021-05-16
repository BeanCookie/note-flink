# 本地模式安装

## 步骤1：下载
为了运行Flink，只需提前安装好 Java 8 或者 Java 11。你可以通过以下命令来检查 Java 是否已经安装正确。

```shell
java -version
```

从Flink下载页面https://flink.apache.org/downloads.html，下载最新的安装包（release {{ site.version }}）并解压。
```shell
tar -xzf flink-{{ site.version }}-bin-scala_2.11.tgz
cd flink-{{ site.version }}-bin-scala_2.11
```

## 步骤 2：启动集群
Flink 附带了一个 bash 脚本，可以用于启动本地集群。
```shell
./bin/start-cluster.sh
```

## 步骤 3：提交作业（Job）
Flink 的 Releases 附带了许多的示例作业。你可以任意选择一个，快速部署到已运行的集群上。
```shell
./bin/flink run examples/streaming/WordCount.jar
tail log/flink-*-taskexecutor-*.out
```

输出结果
```
(to,1)
(be,1)
(or,1)
(not,1)
(to,2)
(be,2)
```

### 步骤 4：停止集群 #
完成后，你可以快速停止集群和所有正在运行的组件。
```shell
./bin/stop-cluster.sh
```