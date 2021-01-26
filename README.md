
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
* [catfan/medoo1.6](https://packagist.org/packages/catfan/medoo)以上版本


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

*注意*：下面的代码或配置只是为了教学编写的简单示例，不应该直接用于生产环境

**1. 安装Moon**

不再赘述

**2. 参数配置**

* 单库模式的配置信息
    ```PHP
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
        ],
        'rec_times'     => 2 // 断线后的重连次数
    );
    ```

* 主从模式的配置信息
    ```PHP
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
    ```

**3. 连接数据库**

默认为单例模式，所以需要提前初始化配置信息。
```php
//初始化配置，执行一次即可
\Moon\Moon::initCfg($config);

//单例对象
$moon = \Moon\Moon::instance();
```

*注意*：一般不需要显示的获取Moon实例，推荐使用模型对数据库进行操作（下面会介绍模型的使用）。


**4. 数据库操作**

一个模型对应数据库的一个表，使用模型可以对数据表进行增删改查等操作。
Moon的模型对数据表结构有一个要求：必须包含一个“主键ID”字段，字段默认名称为“id”，
该名称可以通过修改模型的属性“primary_key”来适配不同的场景。


* 获取模型

    有以下两种方式实例化模型：
    
    ***通用模型***
    ```php
    $moon = \Moon\Moon::instance();
    
    //实例化模型
    $model = $moon->model("表名", "别名"); //“别名”是选填字段
    ```
    
    ***自定义模型***
    ```php
    //定义模型类
    class MyModel extends \Moon\Model{
    
        public $table = 'test'; //映射的表名
        
    }
    
    //实例化模型
    $model = new MyModel();
    ```
    ---

* 单条记录的模型操作

    主要使用到三个函数load()、save()和remove()对数据执行增删改查操作
    ```php
    class User extends \Moon\Model{
    
        public $table = 'user'; //映射的表名
            
    }
    
    //新增
    $user = new User();
    $user->name = '张三';
    $user->gender = '男';
    $user_id = $user1->save(); //新增一个用户，并返回该用户的主键ID
    
    //查询 + 修改
    $user2 = new User();
    $user2->load($user_id); //查找主键ID=$user_id的记录
    $user2->name = '李四'; //将名称修改为李四
    $user2->gender = '女'; //将性别修改为女
    $user2->save(); //当主键ID不为空时，执行更新操作
    
    //删除
    $user2->remove(); //执行删除操作
    ``` 
    ---    

* 查询记录

    - ***where条件***
    
| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| where(key, val) | Model | 等于 |
| whereNot(key, val) | Model | 不等于 |
| whereGT(key, val) | Model | 大于 |
| whereGE(key, val) | Model | 大于等于 |
| whereLT(key, val) | Model | 小于 |
| whereLE(key, val) | Model | 小于等于 |
| whereIn(key, array) | Model | 在列表中 |
| whereNotIn(key, array) | Model | 不在列表中 |
| whereNull(key) | Model | 为空 |
| whereNotNull(key) | Model | 不为空 |
| whereBetween(key, val1, val2) | Model | 介于...之间 |
| whereNotBetween(key, val1, val2) | Model | 不在...之间 |
| whereLike(key, val) | Model | 匹配模糊搜索 |
| whereNotLike(key, val) | Model | 匹配模糊搜索 |
| whereFunc(key, function_name) | Model | 条件为系统函数 |
| whereAnd(callable $func) | Model | 条件的关联关系 - and，<br/>最外层所有的where条件默认使用and连接，不需要显示调用 |
| whereOr(callable $func) | Model | 条件的关联关系 - or |

```
$model->where('id', 10);// sql: where id = 10 

$model->whereNot('id', 10);// sql: where id <> 10

$model->whereGT('id', 10);// sql: where id > 10

$model->whereGE('id', 10);// sql: where id >= 10

$model->whereLT('id', 10);// sql: where id < 10

$model->whereLE('id', 10);// sql: where id <= 10

$model->whereIn('id', [10, 11, 12]);// sql: where id in (10, 11, 12)

$model->whereNotIn('id', [10, 11, 12]);// sql: where id in (10, 11, 12)

$model->whereNull('id');// sql: where id is null

$model->whereNotNull('id');// sql: where id is null

$model->whereBetween('id', 1, 100);// sql: where id between 1 and 100

$model->whereNotBetween('id', 1, 100);// sql: where id not between 1 and 100

$model->whereLike('name', 'zhang%');// sql: where name like 'zhang%'

$model->whereNotLike('name', 'zhang%');// sql: where name not like 'zhang%'

$model->whereFunc('created_time', 'UNIX_TIMESTAMP()');// sql: where created_time = UNIX_TIMESTAMP()

$model->whereAnd(function(\Moon\selector $selector){ 
    $selector->where('name', '张三');
    $selector->where('sex', '男');
}); 
// sql: where ( name = '张三' and 'sex' = '男' )

$model->whereOr(function(\Moon\selector $selector){
    $selector->where('name', '张三');
    $selector->where('sex', '男');
}); 
// sql: where ( name = '张三' or 'sex' = '男' )
```

&nbsp;
* 
    - ***Join联表***

    参数table可以是一个模型，也可以是一个表名字符串
    如果无特殊说明，后续所有table都是模型

| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| join(table, callable func) | Model | 内联查询 - inner join |
| joinLeft(table, callable  func) | Model | 左联查询 - left join |
| joinRight(table, callable  func) | Model | 右联查询 - right join |
| joinFull(table, callable  func) | Model | 外联查询 - outer join |

```
$model->setAlias('u')
    ->join($table, function(\Moon\selector $selector){
    $selector->where('u.class_id', 'id'); //第二个参数表示$table的字段
});
// sql: select * from $model as u inner join $table on u.class_id = $table.id

//其它几个用法同“内联”
```

&nbsp;
*
    - ***Group***


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| groupBy(key) | Model | 组合查询 |


```
$model->where('id'); //sql: group by id
```

&nbsp;
*
    - ***Order***


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| orderBy(key, sort) | Model | 排序 |


```
$model->orderBy('id', 'desc'); //sql: group by id desc
```

&nbsp;
*
    - ***Having***


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| having(callable func) | Model | 组合筛选<br/>通常配合Group使用 |
| havingRaw(raw) | Model | 组合筛选<br/>通常配合Group使用 |


```
$model->groupBy('id')->having(function(Moon\selector $selector) {
    $selector->whereLE('id', 100);
});
 //sql: group by id having id <= 100
 
$model->groupBy('id')->havingRaw(Moon\Moon::raw('count(1) > :cnt', [':cnt' => 5]));
 //sql: group by id having count(1) > 5
```

&nbsp;
*
    - ***limit***


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| limit(len, offset) | Model | 限制获取的数据条目<br/>offset：默认为0 |


```
$model->limit(10)；//sql: limit 10
$model->limit(10, 5)；//sql: limit 10 OFFSET 5, limit 5,10
```

&nbsp;
*
    - ***设置查询字段***


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| select(fieds, alias) | Model | 添加要查询的字段 |
| selectStruct(array) | Model | 自定义数据返回结构 |
| selectInt(fieds, alias) | Model | 添加要查询的字段并输出为int类型 |
| selectBool(fieds, alias) | Model | 添加要查询的字段并输出为bool类型 |
| selectNumber(fieds, alias) | Model | 添加要查询的字段并输出为number类型 |
| selectJson(fieds, alias) | Model | 添加要查询的字段并输出为json类型 |


```
//设置查询字段
$model->select('id')->select('name', 'nickname')->first();
or
$model->select(['id', 'name(nickname)'])->first();
//sql：select `id`, `name` AS `nickname` from table_name limit 1
//返回值: {"id": 1, "nickname": "张三"}


//自定义输出结构
$model->selectStruct(['id', 'info' => ['name(nickname)', 'sex']])->limit(2)->all();
//sql：select `id`,`name` AS `nickname`,`sex` from `mob_user` limit 2
//返回值：
    [{
    	"id": "1000011",
    	"info": {
    		"nickname": "李四",
    		"sex": "女"
    	}
    }, {
    	"id": "1000011",
    	"info": {
    		"nickname": "张三",
    		"sex": "男"
    	}
    }]


//定义字段输出类型
$model->selectInt('id')->selectStruct(['info' => ['name(nickname)', 'sex']])->limit(2)->all();
//sql：select `id`,`name` AS `nickname`,`sex` from `mob_user` limit 2
//返回值：
    [{
    	"id": 1000011, //int类型
    	"info": {
    		"nickname": "张三",
    		"sex": "男"
    	}
    }, {
    	"id": 1000011, //int类型
    	"info": {
    		"nickname": "李四",
    		"sex": "女"
    	}
    }]

```

&nbsp;
*
    - ***执行更新操作查询动作***


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| count() | int | 查询符合条件的数据条目 |
| first() | array | 返回第一条数据 |
| all() | array | 返回所有符合条件的数据 |


```
$model->whereGE('id', 5)->count()；//sql: select COUNT(*) from table_name where `id` >= 5
$model->whereGE('id', 5)->first()；//sql: select * from table_name where `id` >= 5 limit 1
$model->whereGE('id', 5)->all()；//sql: select * from table_name where `id` >= 5
```

&nbsp;

* 新增/更新记录


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| value(key, value) | Model | 新增，修改数据 |
| valueJson(key, array) | Model | 修改数据 - json格式 |
| valueAdd(key, value) | Model | 修改数据 - 自加 |
| valueSub(key, value) | Model | 修改数据 - 自减 |
| valueMul(key, value) | Model | 修改数据 - 自乘 |
| valueDiv(key, value) | Model | 修改数据 - 自除 |
| valueFunc(key, raw) | Model | 修改数据 - 自定义 |
| insert() | Model | 执行新增操作<br/>成功返回自增主键ID，失败返回false|
| update() | Model | 执行更新操作<br/>执行成功后返回修改的数据总条数<br/>***注意***：必须包含查询语句，否则直接返回false |

```
//新增记录
$model->value('name', '张三')->value('sex', '男')->insert();
//sql：INSERT INTO table_name (`name`, `sex`) values ('张三', '男')

//更新记录
$model->where('id', 1)->value('name', '张三')->value('sex', '男')->update();
//sql：UPDATE table_name SET `name` = '张三', `sex` = '男' where `id` = 1
```

&nbsp;
* 删除记录


| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| value(key, value) | Model | 删除记录<br/>***注意***：必须包含查询语句，否则直接返回false |

```
$model->where('id', 1)->delete();
//sql：DELETE from table_name where `id` = 1
```

&nbsp;

* 事务

| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| transaction(callable action) | mixed | 事务处理 |

基于pdo实现的事务处理，可以嵌套使用，只有最外层事务有效。
事务在函数执行完成后会自动提交，只有两种情况会导致事务回滚：

1) action函数的返回值为布尔值false；
2) action函数抛出异常

```
//开启事务的方式有两种
//1. $model->handler()->transaction(function() {});
//2. $moon->transaction(function() {});

$result = $model->handler()->transaction(function() use ($model) {
    $last_id = $model->value('name', '张三')->insert();
    if (rand(0, 1) === 0) { //随机数字，如果为0则回滚，否则提交
        return false;
    }
    return $last_id;
});

var_dump($result);// 值为false或者$last_id的值

```

&nbsp;

* 调试模式

| 函数 | 返回值 | 说明 |
| ---- | ------ | ----------- |
| debug() | Model | 开启DEBUG |

开启了debug模式，所有的数据库操作都不会抵达数据库，而是打印
处将要执行的sql语句，并且操作的返回值统一为布尔值false

```php
//不会真的执行删除操作
//打印sql：DELETE from table_name where `id` = 1
//delete()函数的返回值为false

$model->where('id', 1)->debug()->delete();

```


联系方式
-------


* Github:  [https://github.com/binsuper/Moon](https://github.com/binsuper/Moon)
* E-mail:  [binsuper@126.com](mailto:binsuper@126.com)
