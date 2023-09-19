# 用maven编译运行单元测试来运行hash测试

mvn clean install -Dcheckstyle.skip -Dfindbugs.skip -Dlicense.skip -Dmaven.javadoc.skip -Dtest=alluxio.client.file.dora.HashBenchmark1 -f /root/alluxio-zhaozihao/dora/core/client/fs/pom.xml | grep "Tests run"

mvn clean install -Dcheckstyle.skip -Dfindbugs.skip -Dlicense.skip -Dmaven.javadoc.skip -Dtest=alluxio.client.file.dora.HashBenchmark2 -f /root/alluxio-zhaozihao/dora/core/client/fs/pom.xml | grep "Tests run"

mvn clean install -Dcheckstyle.skip -Dfindbugs.skip -Dlicense.skip -Dmaven.javadoc.skip -Dtest=alluxio.client.file.dora.HashBenchmark5 -f /root/alluxio-zhaozihao/dora/core/client/fs/pom.xml | grep "Tests run"

mvn clean install -Dcheckstyle.skip -Dfindbugs.skip -Dlicense.skip -Dmaven.javadoc.skip -Dtest=alluxio.client.file.dora.HashBenchmark10 -f /root/alluxio-zhaozihao/dora/core/client/fs/pom.xml | grep "Tests run"