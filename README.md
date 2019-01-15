
Moon
======

简介
---------------------

Moon是一个以ORM模式实现的Mysql数据库操作组件。


特点
--------

Moon支持以下功能：

* 轻量级的ORM组件，性能好，效率高；
* 面向对象编程，轻松的对数据库进行操作，不需要拼接SQL语句（支持原生SQL，但不推荐）；
* 安全性良好，防SQL注入，模型操作防止全表更新和删除；
* 良好的扩展性，快速实现个性化需求；
* 支持主从模式，内部隐式完成切换；



需求
------------

Moon需要以下内容：

* PHP7.0以上版本
* php-pdo扩展


安装
------------

Moon目前仅有一种安装方式：

1. 通过 [Composer](https://getcomposer.org) 安装


### 通过 Composer 安装

1. 确认[Packagist](https://packagist.org/packages/binsuper/moon)上支持Moon
2. 作为项目的依赖项进行安装

        $ composer require binsuper/moon

之后，您就可以在项目中使用Moon。


使用教程
-------------------

**1. 安装Moon**


**2. 参数配置**

* 单库模式的配置信息
    
        $config = array(
            // 必须配置项
            'database_type' => 'mysql',
            'database_name' => 'mydatabase',
            'server'        => '127.0.0.1',
            'username'      => 'root',
            'password'      => '123456',
            'charset'       => 'utf8',
            // 可选参数
            'port'          => 3306,
            // 可选，定义表的前缀
            'prefix'        => '',
            // 连接参数扩展, 更多参考 http://www.php.net/manual/en/pdo.setattribute.php
            'option'        => [
                PDO::ATTR_CASE       => PDO::CASE_NATURAL,
                PDO::ATTR_PERSISTENT => true,
                PDO::ATTR_ERRMODE    => PDO::ERRMODE_EXCEPTION
            ]
        );

* 主从模式的配置信息
    
        $config = array(
            'database_type' => 'mysql',
            //如果库名一致，则填写一个即可
            'database_name' => ['mydatabase1', 'mydatabase2', 'mydatabase3'], 
            //第一个为主库，其它均为从库
            'server'        => ['192.168.0.100', '192.168.0.101', '192.168.0.102'], 
            //如果账号一致，则填写一个即可
            'username'      => ['root100', 'root101', 'root102'],
            //如果密码一致，则填写一个即可
            'password'      => ['passwd100', 'passwd101','passwd102'],
            'charset'       => 'utf8',
            //主从开关
            'rd_seprate'    => true,
            'port'          => 3306,
            'prefix'        => '',
            'option'        => [
                PDO::ATTR_CASE       => PDO::CASE_NATURAL,
                PDO::ATTR_PERSISTENT => true,
                PDO::ATTR_ERRMODE    => PDO::ERRMODE_EXCEPTION
            ],
        );


**3. 连接数据库**

默认为单例模式，所以需要提前初始化配置信息

    //初始化配置，执行一次即可
    \Moon\Moon::initCfg($config);
    
    //单例对象
    $moon = \Moon\Moon::instance();

*注意*：一般来说不需要显示获取Moon实例。



联系方式
-------


* Github:  [https://github.com/binsuper/Moon](https://github.com/binsuper/Moon)
* E-mail:  [binsuper@126.com](mailto:binsuper@126.com)
