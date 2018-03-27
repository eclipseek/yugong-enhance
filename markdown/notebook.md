## 1. 增量同步实现原理
物化视图：   
http://blog.itpub.net/27243841/viewspace-1149516/       
https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_6003.htm


基于物化视图。

对要同步的表 A，建立物化视图：
```sql
CREATE MATERIALIZED VIEW migrate_A REFRESH ON COMMIT AS SELECT * FROM A;
```
物化视图就是一个物理表，通过：
```sql
select * from user_tables
```
可查询出物化视图。

物化视图会保持和关联表的数据同步（如何同步，可以在创建时制定。何时同步，oracle 保证。）

## 刷新
### 刷新模式
* on demand 
* on commit 


### 刷新方法
* 完全刷新（COMPLETE）：   
会删除表中所有的记录（如果是单表刷新，可能会采用TRUNCATE的方式），然后根据物化视图中查询语句的定义重新生成物化视图。
* 快速刷新（FAST）：   
采用增量刷新的机制，只将自上次刷新以后对基表进行的所有操作刷新到物化视图中去。FAST必须创建基于主表的视图日志。
对于增量刷新选项，如果在子查询中存在分析函数，则物化视图不起作用。
* FORCE方式(默认方式)：
这是默认的数据刷新方式。Oracle会自动判断是否满足快速刷新的条件，如果满足则进行快速刷新，否则进行完全刷新。
 
> 关于快速刷新：Oracle物化视图的快速刷新机制是通过物化视图日志完成的。Oracle通过一个物化视图日志还可以支持多个
物化视图的快速刷新。物化视图日志根据不同物化视图的快速刷新的需要，可以建立为ROWID或PRIMARY KEY类型的。还可以选
择是否包括SEQUENCE、INCLUDING NEW VALUES以及指定列的列表。     
http://blog.csdn.net/yangshangwei/article/details/53328605。

查询物化视图上次同步时间等信息：
```sql
SELECT t.MVIEW_NAME, t.LAST_REFRESH_TYPE, t.LAST_REFRESH_DATE FROM user_mviews t; 
```

物化视图创建之后，就可以通过监控它实现 oracle 到 mysql 库的数据增量同步。

## 整体运行流程
运行流程如下：
* 加载 properties 配置；
* 启动 Controller；
    - 启动 DataSourceFactory；
    - 加载全局配置（ source 数据源和 target 数据源，全局参数等）
    - 初始化告警服务
* 等待 Controller 完成；
* 停止 Controller；

##
- MDC (YuGongController.start)
- DataSourceFactory.dataSources(YuGongController.start)
- 