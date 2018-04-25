<?php

/*
 * require PHP version >= 7.0
 */

namespace Moon;

use Medoo\{
    Medoo,
    Raw
};

class MoonMedoo extends Medoo {

    /**
     * 有则更新，无则插入
     * @param string $table
     * @param array $datas
     * @param array $update_datas
     * @return \PDOStatement|booelan
     */
    public function insertUpdate($table, $datas, $update_datas) {
        $stack = [];
        $columns = [];
        $fields = [];
        $update_fields = [];
        $map = [];

        if (!isset($datas[0])) {
            $datas = [$datas];
        }

        foreach ($datas as $data) {
            foreach ($data as $key => $value) {
                $columns[] = $key;
            }
        }

        $columns = array_unique($columns);

        foreach ($datas as $data) {
            $values = [];

            foreach ($columns as $key) {
                if ($raw = $this->buildRaw($data[$key], $map)) {
                    $values[] = $raw;
                    continue;
                }

                $map_key = $this->mapKey();

                $values[] = $map_key;

                if (!isset($data[$key])) {
                    $map[$map_key] = [null, PDO::PARAM_NULL];
                } else {
                    $value = $data[$key];

                    $type = gettype($value);

                    switch ($type) {
                        case 'array':
                            $map[$map_key] = [
                                strpos($key, '[JSON]') === strlen($key) - 6 ?
                                json_encode($value) :
                                serialize($value),
                                PDO::PARAM_STR
                            ];
                            break;

                        case 'object':
                            $value = serialize($value);

                        case 'NULL':
                        case 'resource':
                        case 'boolean':
                        case 'integer':
                        case 'double':
                        case 'string':
                            $map[$map_key] = $this->typeMap($value, $type);
                            break;
                    }
                }
            }

            $stack[] = '(' . implode($values, ', ') . ')';
        }

        foreach ($columns as $key) {
            $fields[] = $this->columnQuote(preg_replace("/(\s*\[JSON\]$)/i", '', $key));
        }

        foreach ($update_datas as $key => $value) {
            $column = $this->columnQuote(preg_replace("/(\s*\[(JSON|\+|\-|\*|\/)\]$)/i", '', $key));

            if ($raw = $this->buildRaw($value, $map)) {
                $update_fields[] = $column . ' = ' . $raw;
                continue;
            }

            $map_key = $this->mapKey();

            preg_match('/(?<column>[a-zA-Z0-9_]+)(\[(?<operator>\+|\-|\*|\/)\])?/i', $key, $match);

            if (isset($match['operator'])) {
                if (is_numeric($value)) {
                    $update_fields[] = $column . ' = ' . $column . ' ' . $match['operator'] . ' ' . $value;
                }
            } else {
                $update_fields[] = $column . ' = ' . $map_key;

                $type = gettype($value);

                switch ($type) {
                    case 'array':
                        $map[$map_key] = [
                            strpos($key, '[JSON]') === strlen($key) - 6 ?
                            json_encode($value) :
                            serialize($value),
                            PDO::PARAM_STR
                        ];
                        break;

                    case 'object':
                        $value = serialize($value);

                    case 'NULL':
                    case 'resource':
                    case 'boolean':
                    case 'integer':
                    case 'double':
                    case 'string':
                        $map[$map_key] = $this->typeMap($value, $type);
                        break;
                }
            }
        }

        return $this->exec('INSERT INTO ' . $this->tableQuote($table) . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $stack) . ' ON DUPLICATE KEY UPDATE ' . implode(', ', $update_fields), $map);
    }

}

/**
 * Description of Moon
 *
 * @author PLS007
 */
class Moon {

    protected static $global_options;
    private static $_instance;
    private static $hash_cnt = 0;
    protected $master_option = [];
    protected $slave_options = [];
    protected $conns = [];
    protected $class_connection;
    //是否处于事务中
    private $bInTrans = false;

    /**
     * 读写分离
     * @var bool 
     */
    protected $rd_separate = false;

    public function __construct(array $options) {
        if (!is_array($options)) {
            return false;
        }

        $this->setConnClass($options['class'] ?? '');

        $this->rd_separate = isset($options['rd_seprate']) ? (bool) $options['rd_seprate'] : false;

        //区分读写分离
        if ($this->rd_separate) {
            //库类型必须一致
            $servers = $options['server'] ?? '';
            !is_array($servers) && ($servers = explode(',', $servers));
            if (count($servers) > 1) {
                $port = $options['port'] ?? 3306;
                $username = $options['username'] ?? '';
                $password = $options['password'] ?? '';
                $dbname = $options['database_name'] ?? '';
                !is_array($port) && ($port = explode(',', $port));
                !is_array($username) && ($username = explode(',', $username));
                !is_array($password) && ($password = explode(',', $password));
                !is_array($dbname) && ($dbname = explode(',', $dbname));

                //主数据库信息
                $master_opt = $options;
                $master_opt['server'] = $servers[0];
                $master_opt['port'] = $port[0];
                $master_opt['username'] = $username[0];
                $master_opt['password'] = $password[0];
                $master_opt['database_name'] = $dbname[0];
                $this->master_option = $master_opt;

                //从数据库信息
                for ($i = 1; $i != count($servers); $i++) {
                    $slave_opt = $options;
                    $slave_opt['server'] = $servers[$i];
                    $slave_opt['port'] = $port[$i] ?? $port[0];
                    $slave_opt['username'] = $username[$i] ?? $username[0];
                    $slave_opt['password'] = $password[$i] ?? $password[0];
                    $slave_opt['database_name'] = $dbname[$i] ?? $dbname[0];
                    $this->slave_options[] = $slave_opt;
                }
            } else {
                $this->master_option = $options;
            }
        } else {
            $this->master_option = $options;
        }
    }

    /**
     * 
     * @param string $string
     * @param array $map
     */
    public static function raw(string $string, array $map = []): Raw {
        return MoonMedoo::raw($string, $map);
    }

    /**
     * 
     * @param string $table
     * @param string $alias
     * @return \Moon\Selector
     */
    public function selector(string $table, string $alias = ''): Selector {
        $obj = new Selector($table, $alias);
        return $obj;
    }

    /**
     * 获取一个连接
     * @param int $i
     * @return \Moon\Connection
     */
    protected function getConn(int $i): Connection {
        if ($i < 0 || $i >= ( 1 + count($this->slave_options))) {
            return false;
        }
        if ($i == 0) {
            $option = $this->master_option;
        } else {
            $option = $this->slave_options[$i - 1];
        }
        $class = $this->getConnClass();
        return new $class($option);
    }

    /**
     * 随机hash值
     * @return int
     */
    public function randHash(): int {
        $seed = (int) ((explode(' ', microtime())[0]) * 10000) + (self::$hash_cnt++);
        $inx = ($seed % count($this->slave_options)) + 1;
        return $inx;
    }

    /**
     * 获取一台从服务器链接
     * @return \Moon\Connection
     */
    public function getReader(): Connection {
        //执行事务时,返回主服务器
        if ($this->inTransaction()) {
            return $this->getWriter();
        }
        //不存在从服务器时，直接返回主服务器
        if (count($this->slave_options) == 0) {
            return $this->getWriter();
        }
        if (isset($this->conns[1])) {
            return $this->conns[1];
        }
        $conn = $this->getConn($this->randHash());
        $this->conns[1] = $conn;
        return $conn;
    }

    /**
     * 获取一台主服务器链接
     * @return \Moon\Connection
     */
    public function getWriter(): Connection {
        if (isset($this->conns[0])) {
            return $this->conns[0];
        }
        $this->conns[0] = $this->getConn(0);
        return $this->conns[0];
    }

    /**
     * 连接类名
     * @return string
     */
    public function getConnClass(): string {
        return empty($this->class_connection) ? MedooConnection::class : $this->class_connection;
    }

    /**
     * 设置连接类名
     * @param string $class
     */
    public function setConnClass(string $class) {
        $this->class_connection = $class;
    }

    /**
     * 事务
     * @param callable $action
     * @return mixed
     */
    public function transaction(callable $action) {
        $this->bInTrans = true;
        $ret = $this->getWriter()->transaction($action);
        $this->bInTrans = false;
        return $ret;
    }

    /**
     * 是否正在执行一个事务
     * @return boolean
     */
    public function inTransaction() {
        return $this->bInTrans;
    }

    /**
     * 初始化配置信息
     * @param array $options
     */
    public static function initCfg(array $options) {
        self::$global_options = $options;
    }

    /**
     * 返回实体
     * @return Moon
     */
    public static function instance() {
        if (empty(self::$global_options)) {
            return false;
        }
        if (self::$_instance == null || self::$_instance == false) {
            self::$_instance = new self(self::$global_options);
        }
        return self::$_instance;
    }

    /**
     * 获得一个模型类
     * @param string $table
     * @param string $alias
     * @return \Moon\Model
     */
    public static function model(string $table, string $alias = ''): Model {
        $obj = new Model(false);
        $obj->table = $table;
        $obj->alias = $alias;
        $obj->moon = Moon::instance();
        return $obj;
    }

}

interface Connection {

    public function error();

    public function isError();

    public function fetch(Selector $selector);

    public function fetchAll(Selector $selector);

    public function rowCount(Selector $selector);

    public function insert(Selector ...$selectors);
    
    public function insertUpdate(Selector $update_selector, Selector ...$selectors);

    public function update(Selector $selector);

    public function delete(Selector $selector);

    public function transaction(callable $action);

    public function query($query, $map = []);

    public function exec($query, $map = []);

    public function quote($string);

    public function tableQuote($table);

    public function log();

    public function last();
}

class Collection extends \ArrayObject {
    
}

class MedooConnection implements Connection {

    protected $option;
    public $medoo;
    protected $pdo;
    public $prefix = '';

    public function __construct(array $options) {
        $this->option = $options;
        $this->medoo = new MoonMedoo($options);
        $this->pdo = $this->medoo->pdo;

        $this->prefix = $options['prefix'] ?? '';
    }

    public function __call($name, $arguments) {
        if (method_exists($this->medoo, $name)) {
            $ret = call_user_func_array([$this->medoo, $name], $arguments);
            if (is_object($ret) && $ret instanceof Medoo) {
                return $this;
            } else {
                return $ret;
            }
        }
        trigger_error('Call to undefined method ' . self::class . '::' . $name . '()', E_USER_ERROR);
    }

    /**
     * 
     * @return array
     */
    public function error() {
        return $this->medoo->error();
    }

    /**
     * @return boolean
     */
    public function isError() {
        $error = $this->error();
        if ($error === null || $error[2] === null) {
            return false;
        }
        return true;
    }

    /**
     * 查询一条记录
     * @param \Moon\Selector $selector
     * @return array
     */
    public function fetch(Selector $selector) {
        $handle = clone $selector;
        $handle->limit(1);
        $joins = $handle->contextJoin();
        if (empty($joins)) {
            $ret = $this->medoo->select($handle->tableName(), $handle->contextColumn(), $handle->contextWhere());
        } else {
            $ret = $this->medoo->select($handle->tableName(), $joins, $handle->contextColumn(), $handle->contextWhere());
        }
        if ($this->isError()) {
            return false;
        }
        if (is_array($ret)) {
            return $ret[0] ?? [];
        }
        return $ret;
    }

    /**
     * 批量查询记录
     * @param \Moon\Selector $selector
     * @return array
     */
    public function fetchAll(Selector $selector) {
        $handle = $selector;
        $joins = $handle->contextJoin();
        if (empty($joins)) {
            $ret = $this->medoo->select($handle->tableName(), $handle->contextColumn(), $handle->contextWhere());
        } else {
            $ret = $this->medoo->select($handle->tableName(), $joins, $handle->contextColumn(), $handle->contextWhere());
        }
        if ($this->isError()) {
            return false;
        }
        if (is_array($ret)) {
            return new Collection($ret);
        }
        return $ret;
    }

    /**
     * 获取行数
     * @param \Moon\Selector $selector
     * @return int
     */
    public function rowCount(Selector $selector) {
        $handle = $selector;
        $joins = $handle->contextJoin();
        if (empty($joins)) {
            $ret = $this->medoo->count($handle->tableName(), $handle->contextWhere());
        } else {
            $ret = $this->medoo->count($handle->tableName(), $joins, $handle->contextColumn(), $handle->contextWhere());
        }
        if ($this->isError()) {
            return false;
        }
        return $ret;
    }

    /**
     * 插入数据
     * @param \Moon\Selector $selector
     */
    public function insert(Selector ...$selectors) {
        if (empty($selectors)) {
            return false;
        }
        $values = [];
        $table = $selectors[0]->tableName(false);
        foreach ($selectors as $selector) {
            $tmp = $selector->contextValue();
            if (count($tmp) > 0) {
                $values[] = $tmp;
            }
        }
        $stmt = $this->medoo->insert($table, $values);
        if ($stmt === false || '00000' !== $stmt->errorCode()) {
            return false;
        }
        return $this->medoo->id();
    }

    /**
     * 插入数据
     * @param \Moon\Selector $selector
     */
    public function insertUpdate(Selector $update_selector, Selector ...$selectors) {
        if (empty($selectors)) {
            return false;
        }
        $values = [];
        $table = $selectors[0]->tableName(false);
        foreach ($selectors as $selector) {
            $tmp = $selector->contextValue();
            if (count($tmp) > 0) {
                $values[] = $tmp;
            }
        }
        $update_values = $update_selector->contextValue();
        $stmt = $this->medoo->insertUpdate($table, $values, $update_values);
        if ($stmt === false || '00000' !== $stmt->errorCode()) {
            return false;
        }
        return $this->medoo->id();
    }
    
    /**
     * 删除数据
     * @param \Moon\Selector $selector
     * @return int 被删除的行数
     */
    public function delete(Selector $selector) {
        $where = $selector->contextWhere();
        if (empty($where)) {  //防止删除全表
            return 0;
        }
        $stmt = $this->medoo->delete($selector->tableName(false), $where);
        if ($stmt === false || '00000' !== $stmt->errorCode()) {
            return false;
        }
        return $stmt->rowCount();
    }

    /**
     * 
     * @param \Moon\Selector $selector
     * @return int 受影响的行数
     */
    public function update(Selector $selector) {
        $value = $selector->contextValue();
        if (empty($value)) {  //没有更新的字段
            return 0;
        }
        $where = $selector->contextWhere();
        if (empty($where)) {  //防止更新全表
            return 0;
        }
        $stmt = $this->medoo->update($selector->tableName(false), $value, $where);
        if ($stmt === false || '00000' !== $stmt->errorCode()) {
            return false;
        }
        return $stmt->rowCount();
    }

    /**
     * 事务嵌套处理
     * @param \Moon\callable $action
     * @return boolean
     * @throws \Moon\Exception
     */
    public function transaction(callable $action) {
        if (is_callable($action)) {
            if ($this->medoo->pdo->inTransaction()) {
                //事务嵌套
                $result = $action($this);
            } else {
                $this->medoo->pdo->beginTransaction();
                try {
                    $result = $action($this);
                    if ($result === false) {
                        $this->medoo->pdo->rollBack();
                    } else {
                        $this->medoo->pdo->commit();
                    }
                } catch (\Exception $e) {
                    $this->medoo->pdo->rollBack();
                    throw $e;
                }
            }
            return $result;
        }

        return false;
    }

    /**
     * 查询sql
     * @param string $query
     * @param array $map
     * @return array|boolean 正确是返回结果数组
     */
    public function query($query, $map = array()) {
        $stmt = $this->medoo->query($query, $map);
        if ($stmt === false || '00000' !== $stmt->errorCode()) {
            return false;
        }
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * 执行sql
     * @param string $query
     * @param array $map
     * @return boolean|int 执行成功返回影响的记录数，否则返回false
     */
    public function exec($query, $map = array()) {
        $stmt = $this->medoo->query($query, $map);
        if ($stmt === false || '00000' !== $stmt->errorCode()) {
            return false;
        }
        return $stmt->rowCount();
    }

    /**
     * Quotes a string for use in a query. 
     * @param string $string
     * @return string
     */
    public function quote($string) {
        return $this->medoo->quote($string);
    }

    /**
     * @param string $table
     * @return string
     */
    public function tableQuote($table) {
        return '"' . $this->prefix . $table . '"';
    }

    /**
     * 返回最后执行的sql
     * @return string
     */
    public function last() {
        return $this->medoo->last();
    }

    /**
     * 返回所有执行的查询
     * @return array
     */
    public function log() {
        return $this->medoo->log();
    }

}

class Selector {

    const JOIN_LEFT = 0;
    const JOIN_RIGHT = 1;
    const JOIN_FULL = 2;
    const JOIN_INNER = 3;

    public $table;  //表名
    public $alias;  //别名
    protected $_columns = [];
    protected $_joins = [];
    protected $_conds = [];
    protected $_order = [];
    protected $_group = [];
    protected $_having = [];
    protected $_limit = [];
    protected $_values = [];    //更新或插入的数据列表

    public function __construct(string $table, string $alias = '') {
        $this->table = $table;
        $this->alias = $alias;
    }

    /**
     * 清空构造器
     * @return \Moon\Selector
     */
    public function clear(): Selector {
        $this->_columns = [];
        $this->_joins = [];
        $this->_conds = [];
        $this->_order = [];
        $this->_group = [];
        $this->_having = [];
        $this->_limit = [];
        $this->_values = [];    //更新或插入的数据列表
        return $this;
    }

    protected function _selectName($col) {
        $prefix = $this->table;
        if (!empty($this->alias)) {
            $prefix = $this->alias;
        }
        if (is_string($col) && false === strpos($col, '.')) {
            return $prefix . '.' . $col;
        } else if (is_array($col)) {
            foreach ($col as $k => $c) {
                $col[$k] = $this->_selectName($c);
            }
        }
        return $col;
    }

    /**
     * 添加要查询的字段
     * @param string|array $c
     * @return Selector
     */
    public function select($c, $alias = null): Selector {
        if (is_array($c)) {
            $c = $this->_selectName($c);
            $this->_columns = array_merge($this->_columns, $c);
        } else {
            $c = explode(',', $c);
            if (count($c) > 1) {
                $c = explode(',', $c);
                return $this->select($c);
            } else {
                $key = $c[0];
                if (!empty($alias)) {
                    $key .= '(' . $alias . ')';
                }
                $key = $this->_selectName($key);
                $this->_columns[] = $key;
            }
        }
        return $this;
    }

    /**
     * 构造获取的结构
     * @param array $struct
     * @return \Moon\Selector
     */
    public function selectStruct(array $struct): Selector {
        return $this->select($struct);
    }

    /**
     * 添加查询条件
     * @param string|array $k
     * @param string|array $v
     * @return \Moon\Selector
     */
    public function where($k, $v = null): Selector {
        if (is_array($k)) {
            $this->_conds = array_merge($this->_conds, $k);
        } else {
            $this->_conds[$k] = $v;
        }
        return $this;
    }

    /**
     * 添加查询条件 - !=
     * @param string|array $k
     * @param string|array $v
     * @return \Moon\Selector
     */
    public function whereNot(string $k, $v): Selector {
        return $this->where($k . '[!]', $v);
    }

    /**
     * 添加查询条件 - 大于
     * @param string $k
     * @param type $v
     * @return \Moon\Selector
     */
    public function whereGT(string $k, $v): Selector {
        return $this->where($k . '[>]', $v);
    }

    /**
     * 添加查询条件 - >大于等于
     * @param string $k
     * @param type $v
     * @return \Moon\Selector
     */
    public function whereGE(string $k, $v): Selector {
        return $this->where($k . '[>=]', $v);
    }

    /**
     * 添加查询条件 - 小于
     * @param string $k
     * @param type $v
     * @return \Moon\Selector
     */
    public function whereLT(string $k, $v): Selector {
        return $this->where($k . '[<]', $v);
    }

    /**
     * 添加查询条件 - 小于等于
     * @param string $k
     * @param type $v
     * @return \Moon\Selector
     */
    public function whereLE(string $k, $v): Selector {
        return $this->where($k . '[<=]', $v);
    }

    /**
     * 查询条件 - in
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function whereIn(string $k, array $v): Selector {
        return $this->where($k, $v);
    }

    /**
     * 查询条件 - not in
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function whereNotIn(string $k, array $v): Selector {
        return $this->whereNot($k, $v);
    }

    /**
     * 查询条件 - is null
     * @param string $k
     * @return \Moon\Selector
     */
    public function whereNull(string $k): Selector {
        return $this->where($k, null);
    }

    /**
     * 查询条件 - is not null
     * @param string $k
     * @return \Moon\Selector
     */
    public function whereNotNull(string $k): Selector {
        return $this->whereNot($k, null);
    }

    /**
     * 查询条件 - 介于两者之间
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function whereBetween(string $k, $v1, $v2): Selector {
        return $this->where($k . '[<>]', [$v, $v2]);
    }

    /**
     * 查询条件 - 在两者之外
     * @param string $k
     * @param type $v1
     * @param type $v2
     * @return \Moon\Selector
     */
    public function whereNotBetween(string $k, $v1, $v2): Selector {
        return $this->where($k . '[><]', [$v1, $v2]);
    }

    /**
     * 查询条件 - or
     * @param callable $func
     * @return \Moon\Selector
     */
    public function WhereOr(callable $func): Selector {
        $new_selector = new self($this->table, $this->alias);
        $func($new_selector);
        $conds = $new_selector->_conds;
        return $this->where('OR', $conds);
    }

    /**
     * 查询条件 - and
     * @param callable $func
     * @return \Moon\Selector
     */
    public function WhereAnd(callable $func): Selector {
        $new_selector = new self($this->table, $this->alias);
        $func($new_selector);
        $conds = $new_selector->_conds;
        return $this->where('AND', $conds);
    }

    /**
     * 查询条件 - like
     * @param string $k
     * @param string $v
     * @return \Moon\Selector
     */
    public function whereLike(string $k, string $v): Selector {
        return $this->where($k . '[~]', $v);
    }

    /**
     * 查询条件 - not like
     * @param string $k
     * @param string $v
     * @return \Moon\Selector
     */
    public function whereNotLike(string $k, string $v): Selector {
        return $this->where($k . '[!~]', $v);
    }

    /**
     * 查询条件 - 系统函数
     * @param string $k
     * @param string $v
     * @return \Moon\Selector
     */
    public function whereFunc(string $k, string $v): Selector {
        return $this->where($k, Moon::raw($v));
    }

    /**
     * 表关联
     * @param int $type
     * @param string $table
     * @param string|array $where
     * @return \Moon\Selector
     */
    protected function _join(int $type, string $table, $where): Selector {
        $all_type = ['[>]' /* left joni */, '[<]' /* right join */, '[<>]' /* full join */, '[><]' /* inner join */];
        $c = $all_type[$type] ?? $all_type[3];
        $this->_joins[$c . $table] = $where;
        return $this;
    }

    protected function _joinF(int $type, $table, callable $func) {
        if (is_object($table) && ($table instanceof Selector)) {
            
        } else if (is_string($table)) {
            $alias = '';
            if (preg_match('/(\w+)\((\w+)\)?/', $table, $info)) {
                $table = $info[1];
                $alias = $info[2] ?? '';
            }
            $table = new self($table, $alias);
        } else {
            return false;
        }
        $new_selector = new self($this->table, $this->alias);
        call_user_func($func, $new_selector);
        $where = $new_selector->_conds;
        return $this->_join($type, $table->tableName(), $where);
    }

    /**
     * 内连接
     * @param string $table
     * @param callable $func
     * @return \Moon\Selector
     */
    public function join($table, callable $func): Selector {
        return $this->_joinF(self::JOIN_INNER, $table, $func);
    }

    /**
     * 左连接
     * @param string|\Moon\Selector $table
     * @param callable $func
     * @return \Moon\Selector
     */
    public function joinLeft($table, callable $func): Selector {
        return $this->_joinF(self::JOIN_LEFT, $table, $func);
    }

    /**
     * 右连接
     * @param string|\Moon\Selector $table
     * @param callable $func
     * @return \Moon\Selector
     */
    public function joinRight($table, callable $func): Selector {
        return $this->_joinF(self::JOIN_RIGHT, $table, $func);
    }

    /**
     * 外连接
     * @param string|\Moon\Selector $table
     * @param callable $func
     * @return \Moon\Selector
     */
    public function joinFull($table, callable $func): Selector {
        return $this->_joinF(self::JOIN_FULL, $table, $func);
    }

    /**
     * 组合
     * @param string|array $groupBy
     * @return \Moon\Selector
     */
    public function groupBy($groupBy): Selector {
        if (is_array($groupBy)) {
            $this->_group = array_merge($this->_group, $groupBy);
        } else {
            $this->_group[] = $groupBy;
        }
        return $this;
    }

    /**
     * HAVING
     * @param \Moon\callable $func
     * @return \Moon\Selector
     */
    public function having(callable $func): Selector {
        $new_selector = new self($this->table, $this->alias);
        $func($new_selector);
        $having = $new_selector->_having;
        $this->_having = array_merge($this->_having, $having);
        return $this;
    }

    /**
     * 排序
     * @param string $k
     * @param string|array $v
     * @return \Moon\Selector
     */
    public function orderBy(string $k, $v = 'ASC'): Selector {
        if (is_array($k)) {
            $this->_order = array_merge($this->_order, $k);
        } else {
            $this->_order[$k] = $v;
        }
        return $this;
    }

    /**
     * limit
     * @param int $offset
     * @param int $len
     * @return \Moon\Selector
     */
    public function limit(int $offset, int $len = 0): Selector {
        $this->_limit = [];
        if ($len > 0) {
            $this->_limit[0] = $len;
            $this->_limit[1] = $offset;
        } else {
            $this->_limit[0] = $offset;
        }
        return $this;
    }

    /**
     * 
     * @param string|array $k
     * @param mixed $v
     * @return \Moon\Selector
     */
    public function value($k, $v = null): Selector {
        if (is_array($k)) {
            $this->_values = array_merge($this->_values, $k);
        } else if ($v !== null) {
            $this->_values[$k] = $v;
        }
        return $this;
    }

    /**
     * 
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function valueJson(string $k, array $v): Selector {
        return $this->value($k . '[JSON]', $v);
    }

    /**
     * 
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function valueAdd(string $k, $v): Selector {
        return $this->value($k . '[+]', $v);
    }

    /**
     * 
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function valueSub(string $k, $v): Selector {
        return $this->value($k . '[-]', $v);
    }

    /**
     * 
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function valueMul(string $k, $v): Selector {
        return $this->value($k . '[*]', $v);
    }

    /**
     * 
     * @param string $k
     * @param array $v
     * @return \Moon\Selector
     */
    public function valueDiv(string $k, $v): Selector {
        return $this->value($k . '[/]', $v);
    }

    /**
     * 
     * @param string $k
     * @param string $v
     * @return \Moon\Selector
     */
    public function valueFunc(string $k, string $v): Selector {
        return $this->value($k, Moon::raw($v));
    }

    /**
     * 构建medoo可用的where数组
     * @return array
     */
    public function contextWhere(): array {
        $where = $this->_conds;
        if (count($this->_group) > 0) {
            $where['GROUP'] = $this->_group;
        }
        if (count($this->_having) > 0) {
            $where['HAVING'] = $this->_having;
        }
        if (count($this->_limit) > 0) {
            if (!isset($this->_limit[1])) {
                $where['LIMIT'] = $this->_limit[0];
            } else {
                $where['LIMIT'] = $this->_limit;
            }
        }
        if (count($this->_order) > 0) {
            $where['ORDER'] = $this->_order;
        }
        return $where;
    }

    /**
     * 
     * @return array
     */
    public function contextColumn() {
        if (empty($this->_columns)) {
            return '*';
        }
        return $this->_columns;
    }

    /**
     * 
     * @return array
     */
    public function contextJoin(): array {
        return $this->_joins;
    }

    /**
     * 
     * @return array
     */
    public function contextValue(): array {
        return $this->_values;
    }

    /**
     * 表名称
     * @return string
     */
    public function tableName(bool $bAlias = true): string {
        $name = $this->table;
        if ($bAlias && !empty($this->alias)) {
            $name .= '(' . $this->alias . ')';
        }
        return $name;
    }

    /**
     * 返回列名
     * @param string $col_name
     * @return string
     */
    public function col(string $col_name) {
        if (empty($this->alias)) {
            return $this->table . '.' . $col_name;
        }
        return $this->alias . '.' . $col_name;
    }

}

abstract class Table {

    public $table;  //表明
    public $alias = '';  //别名
    protected $selector;

    /**
     * @var \Moon\Moon 
     */
    public $moon;
    protected $_debug = false;

    public function __construct(string $table = '', string $alias = '') {
        $this->table = $table;
        $this->alias = $alias;
    }

    public function __call($name, $arguments) {
        if (method_exists($this->needSelector(), $name)) {
            $ret = call_user_func_array([$this->needSelector(), $name], $arguments);
            if (is_object($ret) && $ret instanceof Selector) {
                return $this;
            } else {
                return $ret;
            }
        }
        trigger_error('Call to undefined method ' . self::class . '::' . $name . '()', E_USER_ERROR);
    }

    /**
     * 获取一个查询构造器
     * @return \Moon\Selector
     */
    public function needSelector(bool $cache = true): Selector {
        if (!$cache) {
            return new Selector($this->table, $this->alias);
        }
        if ($this->selector == null) {
            $this->selector = new Selector($this->table, $this->alias);
        }
        return $this->selector;
    }

    /**
     * 测试模式,直接输出sql
     * @return \Moon\Model
     */
    public function debug(): Model {
        $this->_debug = true;
        return $this;
    }

    /**
     * 获取第一条数据
     * @return array
     */
    public function first() {
        $conn = $this->moon->getReader();
        if ($this->_debug) {
            $conn->debug();
            $this->_debug = false;
        }
        $selector = $this->needSelector();
        $ret = $conn->fetch($selector);
        $selector->clear();
        return $ret;
    }

    /**
     * 获取符合条件的数据
     * @return array
     */
    public function all() {
        $conn = $this->moon->getReader();
        if ($this->_debug) {
            $conn->debug();
            $this->_debug = false;
        }
        $selector = $this->needSelector();
        $ret = $conn->fetchAll($selector);
        $selector->clear();
        return $ret;
    }

    /**
     * 插入数据
     * @return int
     */
    public function insert() {
        $conn = $this->moon->getWriter();
        if ($this->_debug) {
            $conn->debug();
            $this->_debug = false;
        }
        $selector = $this->needSelector();
        $ret = $conn->insert($selector);
        $selector->clear();
        return $ret;
    }

    /**
     * 更新数据
     * @return int
     */
    public function update() {
        $conn = $this->moon->getWriter();
        if ($this->_debug) {
            $conn->debug();
            $this->_debug = false;
        }
        $selector = $this->needSelector();
        $ret = $conn->update($selector);
        $selector->clear();
        return $ret;
    }

    /**
     * 删除数据
     * @return int
     */
    public function delete() {
        $conn = $this->moon->getWriter();
        if ($this->_debug) {
            $conn->debug();
            $this->_debug = false;
        }
        $selector = $this->needSelector();
        $ret = $conn->delete($selector);
        $selector->clear();
        return $ret;
    }

    public function count() {
        $conn = $this->moon->getReader();
        if ($this->_debug) {
            $conn->debug();
            $this->_debug = false;
        }
        $selector = $this->needSelector();
        $ret = $conn->rowCount($selector);
        $selector->clear();
        return $ret;
    }

}

/**
 * 模型类
 */
class Model extends Table {

    //主键列名
    public $primary_key = 'id';
    //只搜索数组中的列名
    public $query_columns = '*';
    //只更新数组中的列名
    public $update_columns = '*';
    //插入的时间戳列名
    protected $column_create_time = 'created_time';
    //更新的时间戳列名
    protected $column_update_time = 'updated_time';
    protected $_metadata = [];
    protected $_curdata = [];

    public function __construct(bool $bInit = true) {
        if ($bInit) {
            if (empty($this->table)) {
                trigger_error('initialize Model $table can not be null', E_USER_ERROR);
            }
            parent::__construct($this->table, $this->alias);
            $this->moon = Moon::instance();
        }

        if (is_array($this->query_columns) && !$this->checkQueryColumn($this->primary_key)) {
            $this->query_columns[] = $this->primary_key;
        }
    }

    public function __set($name, $value) {
        if ($this->checkUpdateColumn($name)) {
            $this->setData($name, $value);
        }
    }

    public function __get($name) {
        if ($this->checkQueryColumn($name)) {
            return $this->getData($name);
        }
        return null;
    }

    public function __call($name, $arguments) {
        if (method_exists($this->needSelector(), $name)) {
            if ('value' === substr($name, 0, 5)) {
                $key = $arguments[0] ?? null;
                if (!empty($key) && $this->checkUpdateColumn($key)) {
                    return parent::__call($name, $arguments);
                }
            } else {
                return parent::__call($name, $arguments);
            }
            return true;
        }
        trigger_error('Call to undefined method ' . self::class . '::' . $name . '()', E_USER_ERROR);
    }

    /**
     * 检查列名是否可以更新
     * @param string $column_name
     * @return boolean
     */
    protected function checkUpdateColumn($column_name) {
        if ($this->update_columns === '*') {
            return true;
        } else if (is_array($column_name)) {
            return true;
        } else if (in_array($column_name, $this->update_columns)) {
            return true;
        }
        return false;
    }

    /**
     * 检查列名是否可以获取
     * @param string $column_name
     * @return boolean
     */
    protected function checkQueryColumn(string $column_name) {
        if ($this->query_columns === '*') {
            return true;
        } else if (in_array($column_name, $this->query_columns)) {
            return true;
        }
        return false;
    }

    /**
     * 设置数据
     * @param string $column
     * @param mixed $value
     * @return \Moon\Model
     */
    public function setData(string $column, $value): Model {
        $this->_curdata[$column] = $value;
        if ($this->checkUpdateColumn($column)) {
            $this->value($column, $value);
        }
        return $this;
    }

    /**
     * 获取数据
     * @param string $column
     * @return mixed
     */
    public function getData(string $column = '') {
        if (empty($column)) {
            return $this->_curdata;
        }
        return $this->_curdata[$column] ?? null;
    }

    /**
     * 获取主键
     * @return int
     */
    public function getPrimaryValue() {
        return $this->_curdata[$this->primary_key] ?? null;
    }

    /**
     * 设置主键数据
     * @param mixed $v
     * @return \Moon\Model
     */
    public function setPrimaryValue($v): Model {
        $this->_curdata[$this->primary_key] = $v;
        return $this;
    }

    /**
     * 更新时间戳
     * @return \Moon\Model
     */
    public function updateTimestamp(): Model {
        if (is_null($this->getPrimaryValue()) && !empty($this->column_create_time)) {
            //新增
            $this->needSelector()->value($this->column_create_time, time());
        }
        if (!empty($this->column_update_time)) {
            $this->needSelector()->value($this->column_update_time, time());
        }
        return $this;
    }

    /**
     * @return array
     */
    public function first() {
        if (is_array($this->query_columns)) {
            $this->needSelector()->select($this->query_columns);
        }
        return parent::first();
    }

    /**
     * 获取符合条件的数据
     * @return array
     */
    public function all() {
        if (is_array($this->query_columns)) {
            $this->needSelector()->select($this->query_columns);
        }
        return parent::all();
    }

    /**
     * 加载数据
     * @param mixed $value
     * @param string $key
     * @return \Moon\Model
     */
    public function load($value = null, $key = null) {
        $selector = $this->needSelector();
        if (!empty($value)) {
            //默认匹配主键
            if (is_null($key)) {
                $key = $this->primary_key;
            }
            if (empty($key)) {
                trigger_error('method load() need $key, null given', E_USER_ERROR);
                return false;
            }
            $selector->where($key, $value);
        }
        $data = $this->first();
        if ($data === false || empty($data)) {
            return false;
        }
        $this->_metadata = $data;
        $this->_curdata = $data;
        return $this;
    }

    /**
     * 重载数据
     * \Moon\Model
     */
    public function reload() {
        $primary_value = $this->getPrimaryValue();
        if (empty($primary_value)) {
            return false;
        }
        return $this->load($primary_value);
    }

    /**
     * 保存数据
     * @return boolean
     */
    public function save($reload = false) {
        $primary_value = $this->getPrimaryValue();
        $b_insert = false;
        if (is_null($primary_value)) {
            $b_insert = true;
        }
        if ($b_insert) {
            $ret = $this->insert();
            if (false !== $ret) {
                $this->setPrimaryValue($ret);
            }
        } else {
            $selector = $this->needSelector();
            $selector->where($this->primary_key, $primary_value);
            $ret = $this->update();
        }
        if ($ret != false && $reload) {
            $this->reload();
        }
        return $ret;
    }

    /**
     * 删除当前模型数据
     * @return boolean
     */
    public function remove() {
        $primary_value = $this->getPrimaryValue();
        if (empty($primary_value)) {
            return false;
        }
        $selector = $this->needSelector();
        $selector->where($this->primary_key, $primary_value);
        $ret = $this->delete();
        $this->setPrimaryValue(null);
        return $ret;
    }

}
