# blockChainMiner

这是一个小组的project 4, 小组成员名单如下：

计科70 杨景钦 2017011314

计科70 高杰 2017011383

计科70 陈柯润 2017012509



### 如何编译这个项目

git clone到本地后，执行compile.sh即可。 执行完成后，target文件夹下应当有blockdb-1.0-SNAPSHOT.jar



### 如何运行这个项目

在项目根目录下，打开命令行，输入`java -jar ./target/blockdb-1.0-SNAPSHOT.jar --id=?` 即可启动单个服务。

当然，你也可以直接运行`start.sh`(这个是clean start)。

我们把client和server放到了一起，如果需要执行rpc请求，直接`java -jar ./target/blockdb-1.0-SNAPSHOT.jar get test0000` 之类即可。具体的命令格式可以参考client代码。



### 自测数据

我们在`/test`文件夹下准备了自测脚本`test.sh`，不过其运行需要在项目的根目录，于是我们在根目录下也提供了一份`test.sh` （两者完全一致）。自测脚本测试的内容以`echo`的方式输出了出来。

`test.sh` 会会clean start服务，以便多次测试运行。



### 写在最后

本次项目完全在windows环境下开发（linux下只进行了测试）。如果在编译/运行/测试的时候遇到困难，请联系我们。