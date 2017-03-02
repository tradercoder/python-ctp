# python-ctp


CTP Python Adapter . for marketdata/quote datafeed and trade .

开发目的: 提供CTP的python调用接口，方便python开发人员使用API。

设计思路: 通过Bridj开源组件调用c++编写的so。 Java接口完全和c++头文件定义一致，没有做任何自定义转换。

代码说明: 1.保持与原版C++ CTP API名字一致，调用函数一致，写法一致，没有修改自定义数据结构体。

使用步骤: 1. 继承CThostFtdcMdSpi和CThostFtdcTraderSpi类，具体可以参考src/md_2_hbase示例（此example是订阅所有行情，然后保存到hbase数据库中）。

特别说明: 此项目是DataFeed和交易执行模块中的一部分，希望对python程序员有帮助。

免责说明: 交易有风险，入市须谨慎。 不负责由此产生的损失。


