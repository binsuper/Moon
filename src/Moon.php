<?php

/*
 * require PHP version >= 7.0
 */

namespace Moon;

use PDO;
use Medoo\{
    Medoo,
    Raw
};

class MoonMedoo extends Medoo {

    /**
     * 有则更新，无则插入
     *
     * @param string $table
     * @param array $datas
     * @param array $update_datas
     *
     * @return \PDOStatement|booelan
     */
    public function insertUpdate($table, $datas, $update_datas) {
        $stack         = [];
        $columns       = [];
        $fields        = [];
        $update_fields = [];
        $map           = [];

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

    /**
     * join的时候，支持alias
     *
     * @override
     */
    protected function selectContext($table, &$map, $join, &$columns = null, $where = null, $column_fn = null) {
        preg_match('/(?<table>[a-zA-Z0-9_]+)\s*\((?<alias>[a-zA-Z0-9_]+)\)/i', $table, $table_match);

        $table_alias = '';

        if (isset($table_match['table'], $table_match['alias'])) {
            $table       = $this->tableQuote($table_match['table']);
            $table_alias = $this->tableQuote($table_match['alias']);
            $table_query = $table . ' AS ' . $this->tableQuote($table_match['alias']);
        } else {
            $table = $this->tableQuote($table);

            $table_query = $table;
        }

        $join_key = is_array($join) ? array_keys($join) : null;

        if (
            isset($join_key[0]) &&
            strpos($join_key[0], '[') === 0
        ) {
            $table_join = [];

            $join_array = [
                '>'  => 'LEFT',
                '<'  => 'RIGHT',
                '<>' => 'FULL',
                '><' => 'INNER'
            ];

            foreach ($join as $sub_table => $relation) {
                preg_match('/(\[(?<join>\<\>?|\>\<?)\])?(?<table>[a-zA-Z0-9_]+)\s?(\((?<alias>[a-zA-Z0-9_]+)\))?/', $sub_table, $match);

                if ($match['join'] !== '' && $match['table'] !== '') {
                    if (is_string($relation)) {
                        $relation = 'USING ("' . $relation . '")';
                    }

                    if (is_array($relation)) {
                        // For ['column1', 'column2']
                        if (isset($relation[0])) {
                            $relation = 'USING ("' . implode($relation, '", "') . '")';
                        } else {
                            $joins = [];

                            foreach ($relation as $key => $value) {
                                $joins[] = (
                                    strpos($key, '.') > 0 ?
                                        // For ['tableB.column' => 'column']
                                        $this->columnQuote($key) :
                                        // For ['column1' => 'column2']
                                        ((empty($table_alias) ? $table : $table_alias) . '."' . $key . '"')
                                    ) .
                                    ' = ' .
                                    $this->tableQuote(isset($match['alias']) ? $match['alias'] : $match['table']) . '."' . $value . '"';
                            }

                            $relation = 'ON ' . implode($joins, ' AND ');
                        }
                    }

                    $table_name = $this->tableQuote($match['table']) . ' ';

                    if (isset($match['alias'])) {
                        $table_name .= 'AS ' . $this->tableQuote($match['alias']) . ' ';
                    }

                    $table_join[] = $join_array[$match['join']] . ' JOIN ' . $table_name . $relation;
                }
            }

            $table_query .= ' ' . implode($table_join, ' ');
        } else {
            if (is_null($columns)) {
                if (
                    !is_null($where) ||
                    (is_array($join) && isset($column_fn))
                ) {
                    $where   = $join;
                    $columns = null;
                } else {
                    $where   = null;
                    $columns = $join;
                }
            } else {
                $where   = $columns;
                $columns = $join;
            }
        }

        if (isset($column_fn)) {
            if ($column_fn === 1) {
                $column = '1';

                if (is_null($where)) {
                    $where = $columns;
                }
            } else {
                if (empty($columns) || $this->isRaw($columns)) {
                    $columns = '*';
                    $where   = $join;
                }
                if ($columns === '1') {
                    $columns = '*';
                }
                $column = $column_fn . '(' . $this->columnPush($columns, $map) . ')';
            }
        } else {
            $column = $this->columnPush($columns, $map);
        }
        return 'SELECT ' . $column . ' FROM ' . $table_query . $this->whereClause($where, $map);
    }

    /**
     * 获取最后执行的一条sql语句
     *
     * @return string
     */
    public function last(bool $getNew = false) {
        static $last_idx = -1;
        $cnt = count($this->logs);
        if ($getNew && $last_idx >= $cnt - 1) {
            return '';
        } else {
            $last_idx = $cnt;
        }
        return parent::last();
    }

    /**
     *
     * @param callable $callback
     * @param string $table
     * @param array $join
     * @param array $columns
     * @param array $where
     *
     * @return boolean|int 返回查询的行数
     */
    public function selectEach($callback, $table, $join, $columns = null, $where = null) {
        $map        = [];
        $column_map = [];
        $index      = 0;
        $query      = $this->exec($this->selectContext($table, $map, $join, $columns, $where), $map);
        $this->columnMap($columns, $column_map);
        if (!$query) {
            return false;
        }
        $use_data_map = false;
        if (is_array($columns)) {
            foreach ($column_map as $colm) {
                if ($colm[0] === '*') {
                    $use_data_map = true;
                    break;
                }
            }
            $use_data_map ^= true;
        }
        while ($data = $query->fetch(PDO::FETCH_ASSOC)) {
            $index++;
            $current_stack = [];
            if ($use_data_map) {
                $this->dataMap($data, $columns, $column_map, $current_stack);
            } else {
                $current_stack = $data;
            }
            if (false === call_user_func($callback, $current_stack)) {
                break;
            }
        }
        if ($query instanceof \PDOStatement) {
            $query->closeCursor();
        }

        return $index;
    }

    protected function columnQuote($string) {
        if (strpos($string, '.') !== false) {
            if (substr($string, -1) === '*') {
                return $this->prefix . $string;
            } else {
                return '"' . $this->prefix . str_replace('.', '"."', $string) . '"';
            }
        }

        return '"' . $string . '"';
    }

    protected function columnPush(&$columns, &$map) {
        if ($columns === '*') {
            return $columns;
        }

        $stack = [];

        if (is_string($columns)) {
            $columns = [$columns];
        }

        foreach ($columns as $key => $value) {
            if (is_array($value)) {
                $stack[] = $this->columnPush($value, $map);
            } elseif (!is_int($key) && $raw = $this->buildRaw($value, $map)) {
                preg_match('/(?<column>[a-zA-Z0-9_\.]+)(\s*\[(?<type>(String|Bool|Int|Number))\])?/i', $key, $match);

                $stack[] = $raw . ' AS ' . $this->columnQuote($match['column']);
            } elseif (is_int($key) && is_string($value)) {
                preg_match('/(?<column>[a-zA-Z0-9_\.\*]+)(?:\s*\((?<alias>[a-zA-Z0-9_]+)\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/i', $value, $match);

                if (!empty($match['alias'])) {
                    $stack[] = $this->columnQuote($match['column']) . ' AS ' . $this->columnQuote($match['alias']);

                    $columns[$key] = $match['alias'];

                    if (!empty($match['type'])) {
                        $columns[$key] .= ' [' . $match['type'] . ']';
                    }
                } else {
                    $stack[] = $this->columnQuote($match['column']);
                }
            }
        }

        return implode($stack, ',');
    }

    protected function columnMap($columns, &$stack) {
        if ($columns === '*') {
            return $stack;
        }

        foreach ($columns as $key => $value) {
            if (is_int($key)) {
                preg_match('/([a-zA-Z0-9_]+\.)?(?<column>[a-zA-Z0-9_\*]+)(?:\s*\((?<alias>[a-zA-Z0-9_]+)\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/i', $value, $key_match);

                $column_key = !empty($key_match['alias']) ?
                    $key_match['alias'] :
                    $key_match['column'];

                if (isset($key_match['type'])) {
                    $stack[$value] = [$column_key, $key_match['type']];
                } else {
                    $stack[$value] = [$column_key, 'String'];
                }
            } elseif ($this->isRaw($value)) {
                preg_match('/([a-zA-Z0-9_]+\.)?(?<column>[a-zA-Z0-9_]+)(\s*\[(?<type>(String|Bool|Int|Number))\])?/i', $key, $key_match);

                $column_key = $key_match['column'];

                if (isset($key_match['type'])) {
                    $stack[$key] = [$column_key, $key_match['type']];
                } else {
                    $stack[$key] = [$column_key, 'String'];
                }
            } elseif (!is_int($key) && is_array($value)) {
                $this->columnMap($value, $stack);
            }
        }

        return $stack;
    }

}

/**
 * Description of Moon
 *
 * @author PLS007
 */
class Moon {

    const ERROR_SERVER_INTERRUPT = " MySQL server has gone away";

    protected static $global_options;
    private static   $_instance;
    private static   $hash_cnt      = 0;
    protected        $master_option = [];
    protected        $slave_options = [];
    protected        $conns         = [];
    protected        $class_connection;
    //是否处于事务中
    private $bInTrans = false;

    /**
     * 读写分离
     *
     * @var bool
     */
    protected $rd_separate = false;

    /**
     * 断线重连次数
     *
     * @var int
     */
    protected $_reconnect_times = 1;

    public function __construct(array $options) {
        if (!is_array($options)) {
            return false;
        }

        $this->setConnClass($options['class'] ?? '');

        $this->rd_separate      = isset($options['rd_seprate']) ? (bool)$options['rd_seprate'] : false;
        $this->_reconnect_times = isset($options['rec_times']) ? $options['rec_times'] : 1;

        //区分读写分离
        if ($this->rd_separate) {
            //库类型必须一致
            $servers = $options['server'] ?? '';
            !is_array($servers) && ($servers = explode(',', $servers));
            if (count($servers) > 1) {
                $port     = $options['port'] ?? 3306;
                $username = $options['username'] ?? '';
                $password = $options['password'] ?? '';
                $dbname   = $options['database_name'] ?? '';
                !is_array($port) && ($port = explode(',', $port));
                !is_array($username) && ($username = explode(',', $username));
                !is_array($password) && ($password = explode(',', $password));
                !is_array($dbname) && ($dbname = explode(',', $dbname));

                //主数据库信息
                $master_opt                  = $options;
                $master_opt['server']        = $servers[0];
                $master_opt['port']          = $port[0];
                $master_opt['username']      = $username[0];
                $master_opt['password']      = $password[0];
                $master_opt['database_name'] = $dbname[0];
                $this->master_option         = $master_opt;

                //从数据库信息
                for ($i = 1; $i != count($servers); $i++) {
                    $slave_opt                  = $options;
                    $slave_opt['server']        = $servers[$i];
                    $slave_opt['port']          = $port[$i] ?? $port[0];
                    $slave_opt['username']      = $username[$i] ?? $username[0];
                    $slave_opt['password']      = $password[$i] ?? $password[0];
                    $slave_opt['database_name'] = $dbname[$i] ?? $dbname[0];
                    $this->slave_options[]      = $slave_opt;
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
     * 执行事务
     * $action函数有且仅有一个入参，类型为(\Moon\Connection)
     *
     * @param callable $action   事务函数
     * @param callable $callback 事务执行后的回调函数, 传入的参数为bool类型，true代表事务执行成功，反之为执行失败
     *
     * @return mixed
     */
    public static function doTrans(callable $action, $callback = null) {
        return static::instance()->transaction($action, $callback);
    }

    /**
     *
     * @param string $table
     * @param string $alias
     *
     * @return \Moon\Selector
     */
    public function selector(string $table, string $alias = ''): Selector {
        $obj = new Selector($table, $alias);
        return $obj;
    }

    /**
     * 获取一个连接
     *
     * @param int $i
     *
     * @return \Moon\Connection
     */
    protected function getConn(int $i): Connection {
        if ($i < 0 || $i >= (1 + count($this->slave_options))) {
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
     *
     * @return int
     */
    public function randHash(): int {
        $seed = (int)((explode(' ', microtime())[0]) * 10000) + (self::$hash_cnt++);
        $inx  = ($seed % count($this->slave_options)) + 1;
        return $inx;
    }

    /**
     * 获取一台从数据库链接
     *
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
        $this->conns[1] = $this->getConn($this->randHash());
        return $this->conns[1];
    }

    /**
     * 获取一台主数据库的链接
     *
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
     * 重置从库连接
     *
     * @return $this
     */
    public function resetReader() {
        //执行事务时,返回主服务器
        if ($this->inTransaction()) {
            return $this->resetWriter();
        }
        //不存在从服务器时，直接返回主服务器
        if (count($this->slave_options) == 0) {
            return $this->resetWriter();
        }
        if (!empty($this->conns[1])) {
            $this->conns[1] = NULL;
        }
        unset($this->conns[1]);
        return $this;
    }

    /**
     * 重置主库连接
     *
     * @return $this
     */
    public function resetWriter() {
        if (!empty($this->conns[0])) {
            $this->conns[0] = NULL;
        }
        unset($this->conns[0]);
        return $this;
    }

    /**
     * 连接类名
     *
     * @return string
     */
    public function getConnClass(): string {
        return empty($this->class_connection) ? MedooConnection::class : $this->class_connection;
    }

    /**
     * 设置连接类名
     *
     * @param string $class
     */
    public function setConnClass(string $class) {
        $this->class_connection = $class;
    }

    /**
     * 执行事务
     * $action函数有且仅有一个入参，类型为(\Moon\Connection)
     *
     * @param callable $action   事务函数
     * @param callable $callback 事务执行后的回调函数, 传入的参数为bool类型，true代表事务执行成功，反之为执行失败
     *
     * @return mixed
     * @throws \Exception
     */
    public function transaction(callable $action, $callback = null) {
        return static::retry(function () use ($action, $callback) {
            $this->bInTrans = true;
            $ret            = $this->getWriter()->transaction($action, $callback);
            $this->bInTrans = false;
            return $ret;
        }, function () {
            $this->resetWriter();
        }, $this->moon->getRecTimes());
    }

    /**
     * 是否正在执行一个事务
     *
     * @return boolean
     */
    public function inTransaction() {
        return $this->bInTrans;
    }

    /**
     * 初始化配置信息
     *
     * @param array $options
     */
    public static function initCfg(array $options) {
        self::$global_options = $options;
    }

    /**
     * 返回实体
     *
     * @return Moon
     */
    public static function instance() {
        if (empty(self::$global_options)) {
            return false;
        }
        if (self::$_instance == null || self::$_instance == false) {
            self::$_instance = new static(self::$global_options);
        }
        return self::$_instance;
    }

    /**
     * 获得一个模型类
     *
     * @param string $table
     * @param string $alias
     *
     * @return \Moon\Model
     */
    public function model(string $table, string $alias = ''): Model {
        $obj        = new Model(false);
        $obj->table = $table;
        $obj->alias = $alias;
        $obj->moon  = $this;
        return $obj;
    }

    /**
     * 重试
     *
     * @param callable $action
     * @param null $retry_action
     * @param int $retry_times
     *
     * @return mixed
     * @throws \Exception
     */
    public static function retry(callable $action, $retry_action = null, int $retry_times = 1) {
        try {
            try {
                return call_user_func($action);
            } catch (\PDOException $ex) {
                if (false !== strstr($ex->getMessage(), static::ERROR_SERVER_INTERRUPT)) {
                    throw new RetryException('', 0, $ex);
                }
                throw $ex;
            }
        } catch (RetryException $ex) {
            if ($retry_times > 0) {
                if (is_callable($retry_action)) {
                    call_user_func($retry_action);
                }
                return static::retry($action, null, $retry_times - 1);
            }
            throw $ex->getPrevious();
        }
        return null;
    }

    /**
     * 断线重连次数
     *
     * @return int
     */
    public function getRecTimes(): int {
        return $this->_reconnect_times;
    }

    /**
     * @param int $times
     *
     * @return $this
     */
    public function setRecTimes(int $times) {
        $this->_reconnect_times = $times;
        return $this;
    }

}

interface Connection {

    /**
     * 获取错误信息
     *
     * @return array
     */
    public function error();

    /**
     * 检测是否发生错误
     *
     * @return boolean
     */
    public function isError();

    /**
     * 查询一条记录
     *
     * @param \Moon\Selector $selector
     *
     * @return array
     */
    public function fetch(Selector $selector);

    /**
     * 批量查询记录
     *
     * @param \Moon\Selector $selector
     *
     * @return \Moon\Collection
     */
    public function fetchAll(Selector $selector);

    /**
     * 每次查询一条记录，并以传参的方式传入到回调函数$callback中
     * 这个操作对于大数据查询是安全有效的
     *
     * @param \Moon\Selector $selector
     * @param callable $callback 回调函数，当且仅当返回false时将结束DB查询
     *
     * @return int 返回已查询的行数
     */
    public function fetchRow(Selector $selector, $callback);

    /**
     * 获取行数
     *
     * @param \Moon\Selector $selector
     *
     * @return int
     */
    public function rowCount(Selector $selector);

    /**
     * 插入数据
     *
     * @param \Moon\Selector $selector
     */
    public function insert(Selector ...$selectors);

    /**
     * 插入数据
     *
     * @param \Moon\Selector $selector
     */
    public function insertUpdate(Selector $update_selector, Selector ...$selectors);

    /**
     *
     * @param \Moon\Selector $selector
     *
     * @return int 受影响的行数
     */
    public function update(Selector $selector);

    /**
     * 删除数据
     *
     * @param \Moon\Selector $selector
     *
     * @return int 被删除的行数
     */
    public function delete(Selector $selector);

    /**
     * 事务嵌套处理
     *
     * @param callable $action   事务处理函数
     * @param callable $callback 事务执行后的回调函数, 传入的参数为bool类型，true代表事务执行成功，反之为执行失败
     *
     * @return boolean
     * @throws \Moon\Exception
     */
    public function transaction(callable $action, $callback = null);

    /**
     * 查询sql
     *
     * @param string $query
     * @param array $map
     *
     * @return array|boolean 正确是返回结果数组
     */
    public function query($query, $map = []);

    /**
     * 执行sql
     *
     * @param string $query
     * @param array $map
     *
     * @return boolean|int 执行成功返回影响的记录数，否则返回false
     */
    public function exec($query, $map = []);

    public function quote($string);

    public function tableQuote($table);

    /**
     * 返回所有执行的查询日志
     *
     * @return array
     */
    public function log();

    /**
     * 返回最后执行的sql日志
     *
     * @return string
     */
    public function last();

}

class Collection extends \ArrayObject {

    /**
     * 转换为数组
     *
     * @return array
     */
    public function toArray() {
        return (array)$this;
    }

}

class MedooConnection implements Connection {

    protected $option;
    public    $medoo;
    protected $pdo;
    public    $prefix                  = '';
    private   $__trans_callback_action = [];

    public function __construct(array $options) {
        $this->option = $options;
        $this->medoo  = new MoonMedoo($options);
        $this->pdo    = $this->medoo->pdo;

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
     * 获取错误信息
     *
     * @return array
     */
    public function error() {
        return $this->medoo->error();
    }

    /**
     * 检测是否发生错误
     *
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
     *
     * @param \Moon\Selector $selector
     *
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
     *
     * @param \Moon\Selector $selector
     *
     * @return \Moon\Collection
     */
    public function fetchAll(Selector $selector) {
        $handle = $selector;
        $joins  = $handle->contextJoin();
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
     * 每次查询一条记录，并以传参的方式传入到回调函数$callback中
     * 这个操作对于大数据查询是安全有效的
     *
     * @param \Moon\Selector $selector
     * @param callable $callback 回调函数，当且仅当返回false时将结束DB查询
     *
     * @return bool|int 返回已查询的行数
     */
    public function fetchRow(Selector $selector, $callback) {
        $handle = $selector;
        $joins  = $handle->contextJoin();
        if (empty($joins)) {
            $ret = $this->medoo->selectEach($callback, $handle->tableName(), $handle->contextColumn(), $handle->contextWhere());
        } else {
            $ret = $this->medoo->selectEach($callback, $handle->tableName(), $joins, $handle->contextColumn(), $handle->contextWhere());
        }
        if ($this->isError()) {
            return false;
        }
        return $ret;
    }

    /**
     * 获取行数
     *
     * @param \Moon\Selector $selector
     *
     * @return int
     */
    public function rowCount(Selector $selector) {
        $handle = $selector;
        $joins  = $handle->contextJoin();
        if (empty($joins)) {
            $ret = $this->medoo->count($handle->tableName(), $handle->contextWhere());
        } else {
            $ret = $this->medoo->count($handle->tableName(), $joins, '1', $handle->contextWhere());
        }
        if ($this->isError()) {
            return false;
        }
        return $ret;
    }

    /**
     * 插入数据
     *
     * @param \Moon\Selector $selector
     */
    public function insert(Selector ...$selectors) {
        if (empty($selectors)) {
            return false;
        }
        $values = [];
        $table  = $selectors[0]->tableName(false);
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
     *
     * @param \Moon\Selector $selector
     */
    public function insertUpdate(Selector $update_selector, Selector ...$selectors) {
        if (empty($selectors)) {
            return false;
        }
        $values = [];
        $table  = $selectors[0]->tableName(false);
        foreach ($selectors as $selector) {
            $tmp = $selector->contextValue();
            if (count($tmp) > 0) {
                $values[] = $tmp;
            }
        }
        $update_values = $update_selector->contextValue();
        $stmt          = $this->medoo->insertUpdate($table, $values, $update_values);
        if ($stmt === false || '00000' !== $stmt->errorCode()) {
            return false;
        }
        return $this->medoo->id();
    }

    /**
     * 删除数据
     *
     * @param \Moon\Selector $selector
     *
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
     *
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
     *
     * @param callable $action   事务处理函数
     * @param callable $callback 事务执行后的回调函数, 传入的参数为bool类型，true代表事务执行成功，反之为执行失败
     *
     * @return boolean
     * @throws \Moon\Exception
     */
    public function transaction(callable $action, $callback = null) {
        if (is_callable($action)) {
            if ($this->medoo->pdo->inTransaction()) {
                //事务嵌套
                $result = call_user_func($action, $this);
                //记录处理函数
                if (is_callable($callback)) {
                    array_unshift($this->__trans_callback_action, $callback);
                }
            } else {
                //开启事务前将处理函数清空
                $this->__trans_callback_action = [];
                //记录处理函数
                if (is_callable($callback)) {
                    array_unshift($this->__trans_callback_action, $callback);
                }

                //开启事务
                $this->medoo->pdo->beginTransaction();
                try {
                    $result = call_user_func($action, $this);
                    if ($result === false) {
                        $this->medoo->pdo->rollBack();
                        //执行函数
                        if (!empty($this->__trans_callback_action)) {
                            foreach ($this->__trans_callback_action as $callable) {
                                try {
                                    call_user_func($callable, false);
                                } catch (\Throwable $ex) {
                                    //丢弃异常
                                }
                            }
                        }
                    } else {
                        $this->medoo->pdo->commit();
                        //执行函数
                        if (!empty($this->__trans_callback_action)) {
                            foreach ($this->__trans_callback_action as $callable) {
                                try {
                                    call_user_func($callable, true);
                                } catch (\Throwable $ex) {
                                    //丢弃异常
                                }
                            }
                        }
                    }
                } catch (\Throwable $e) {
                    $this->medoo->pdo->rollBack();
                    if (!empty($this->__trans_callback_action)) {
                        foreach ($this->__trans_callback_action as $callable) {
                            try {
                                call_user_func($callable, false);
                            } catch (\Throwable $ex) {
                                //丢弃异常
                            }
                        }
                    }
                    throw $e;
                }
            }
            return $result;
        }

        return false;
    }

    /**
     * 查询sql
     *
     * @param string $query
     * @param array $map
     *
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
     *
     * @param string $query
     * @param array $map
     *
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
     *
     * @param string $string
     *
     * @return string
     */
    public function quote($string) {
        return $this->medoo->quote($string);
    }

    /**
     * @param string $table
     *
     * @return string
     */
    public function tableQuote($table) {
        return $this->prefix . $table;
    }

    /**
     * 返回最后执行的sql日志
     *
     * @param bool $getNew 获取最新执行的额
     *
     * @return string
     */
    public function last(bool $getNew = false) {
        return $this->medoo->last($getNew);
    }

    /**
     * 返回所有执行的查询日志
     *
     * @return array
     */
    public function log() {
        return $this->medoo->log();
    }

}

class Selector {

    const JOIN_LEFT  = 0;
    const JOIN_RIGHT = 1;
    const JOIN_FULL  = 2;
    const JOIN_INNER = 3;
    //查询字段的类型
    const FIELD_TYPE_STRING = '[String]'; //默认
    const FIELD_TYPE_INT    = '[Int]';
    const FIELD_TYPE_BOOL   = '[Bool]';
    const FIELD_TYPE_NUMBER = '[Number]';
    const FIELD_TYPE_OBJECT = '[Object]';
    const FIELD_TYPE_JSON   = '[JSON]';

    public    $table;  //表名
    public    $alias;  //别名
    protected $_columns = [];
    protected $_joins   = [];
    protected $_conds   = [];
    protected $_order   = [];
    protected $_group   = [];
    protected $_having  = [];
    protected $_limit   = [];
    protected $_values  = [];    //更新或插入的数据列表

    public function __construct(string $table, string $alias = '') {
        $this->table = $table;
        $this->alias = $alias;
    }

    /**
     * 清空构造器
     *
     * @return \Moon\Selector
     */
    public function clear(): Selector {
        $this->_columns = [];
        $this->_joins   = [];
        $this->_conds   = [];
        $this->_order   = [];
        $this->_group   = [];
        $this->_having  = [];
        $this->_limit   = [];
        $this->_values  = [];    //更新或插入的数据列表
        return $this;
    }

    /**
     * 转还字段名称
     *
     * @param string|array $col
     *
     * @return string
     */
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
     * 搜索的列名加上类型
     *
     * @param string $col
     *
     * @return string
     */
    public function f2Int(string $col): string {
        return $col . self::FIELD_TYPE_INT;
    }

    /**
     * 搜索的列名加上类型
     *
     * @param string $col
     *
     * @return string
     */
    public function f2Num(string $col): string {
        return $col . self::FIELD_TYPE_NUMBER;
    }

    /**
     * 搜索的列名加上类型
     *
     * @param string $col
     *
     * @return string
     */
    public function f2Bool(string $col): string {
        return $col . self::FIELD_TYPE_BOOL;
    }

    /**
     * 搜索的列名加上类型
     *
     * @param string $col
     *
     * @return string
     */
    public function f2Json(string $col): string {
        return $col . self::FIELD_TYPE_JSON;
    }

    /**
     * 添加要查询的字段
     *
     * @param string|array $c
     * @param string $alias
     * @param string $type 数据类型
     *
     * @return \Moon\Selector
     */
    public function select($c, $alias = null, string $type = ''): Selector {
        if (is_array($c)) {
            $c              = $this->_selectName($c);
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
                if (!empty($type)) {
                    $key .= $type;
                }
                $this->_columns[] = $key;
            }
        }
        return $this;
    }

    /**
     * 构造获取的结构
     *
     * @param array $struct
     *
     * @return $this
     */
    public function selectStruct(array $struct): Selector {
        return $this->select($struct);
    }

    /**
     * 查询不同类型字段
     *
     * @param array|string $c
     * @param string $alias
     * @param string $type
     *
     * @return \Moon\Selector
     */
    final private function _selectByType($c, $alias, $type) {
        if (is_array($c)) {
            foreach ($c as $k) {
                $this->select($k, null, $type);
            }
            return $this;
        } else {
            return $this->select($c, $alias, $type);
        }
    }

    /**
     * 查询int类型字段
     *
     * @param array|string $c
     * @param string $alias
     *
     * @return \Moon\Selector
     */
    public function selectInt($c, $alias = null) {
        return $this->_selectByType($c, $alias, self::FIELD_TYPE_INT);
    }

    /**
     * 查询bool类型字段
     *
     * @param array|string $c
     * @param string $alias
     *
     * @return \Moon\Selector
     */
    public function selectBool($c, $alias = null) {
        return $this->_selectByType($c, $alias, self::FIELD_TYPE_BOOL);
    }

    /**
     * 查询number类型字段
     *
     * @param array|string $c
     * @param string $alias
     *
     * @return \Moon\Selector
     */
    public function selectNumber($c, $alias = null) {
        return $this->_selectByType($c, $alias, self::FIELD_TYPE_NUMBER);
    }

    /**
     * 查询json类型字段
     *
     * @param array|string $c
     * @param string $alias
     *
     * @return \Moon\Selector
     */
    public function selectJson($c, $alias = null) {
        return $this->_selectByType($c, $alias, self::FIELD_TYPE_JSON);
    }

    /**
     * 添加查询条件
     *
     * @param string|array $k
     * @param string|array $v
     *
     * @return $this
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
     *
     * @param string|array $k
     * @param string|array $v
     *
     * @return $this
     */
    public function whereNot(string $k, $v): Selector {
        return $this->where($k . '[!]', $v);
    }

    /**
     * 添加查询条件 - 大于
     *
     * @param string $k
     * @param type $v
     *
     * @return $this
     */
    public function whereGT(string $k, $v): Selector {
        return $this->where($k . '[>]', $v);
    }

    /**
     * 添加查询条件 - >大于等于
     *
     * @param string $k
     * @param type $v
     *
     * @return $this
     */
    public function whereGE(string $k, $v): Selector {
        return $this->where($k . '[>=]', $v);
    }

    /**
     * 添加查询条件 - 小于
     *
     * @param string $k
     * @param type $v
     *
     * @return $this
     */
    public function whereLT(string $k, $v): Selector {
        return $this->where($k . '[<]', $v);
    }

    /**
     * 添加查询条件 - 小于等于
     *
     * @param string $k
     * @param type $v
     *
     * @return $this
     */
    public function whereLE(string $k, $v): Selector {
        return $this->where($k . '[<=]', $v);
    }

    /**
     * 查询条件 - in
     *
     * @param string $k
     * @param array $v
     *
     * @return $this
     */
    public function whereIn(string $k, array $v): Selector {
        return $this->where($k, $v);
    }

    /**
     * 查询条件 - not in
     *
     * @param string $k
     * @param array $v
     *
     * @return $this
     */
    public function whereNotIn(string $k, array $v): Selector {
        return $this->whereNot($k, $v);
    }

    /**
     * 查询条件 - is null
     *
     * @param string $k
     *
     * @return $this
     */
    public function whereNull(string $k): Selector {
        return $this->where($k, null);
    }

    /**
     * 查询条件 - is not null
     *
     * @param string $k
     *
     * @return $this
     */
    public function whereNotNull(string $k): Selector {
        return $this->whereNot($k, null);
    }

    /**
     * 查询条件 - 介于两者之间
     *
     * @param string $k
     * @param mixed $v1
     * @param mixed $v2
     *
     * @return $this
     */
    public function whereBetween(string $k, $v1, $v2): Selector {
        return $this->where($k . '[<>]', [$v1, $v2]);
    }

    /**
     * 查询条件 - 在两者之外
     *
     * @param string $k
     * @param mixed $v1
     * @param mixed $v2
     *
     * @return $this
     */
    public function whereNotBetween(string $k, $v1, $v2): Selector {
        return $this->where($k . '[><]', [$v1, $v2]);
    }

    /**
     * 查询条件 - or
     *
     * @param callable $func
     *
     * @return $this
     */
    public function WhereOr(callable $func): Selector {
        static $id = 0;
        $new_selector = new static($this->table, $this->alias);
        $func($new_selector);
        $conds = $new_selector->_conds;
        return $this->where('OR #' . ($id++), $conds);
    }

    /**
     * 查询条件 - and
     *
     * @param callable $func
     *
     * @return $this
     */
    public function WhereAnd(callable $func): Selector {
        static $id = 0;
        $new_selector = new static($this->table, $this->alias);
        $func($new_selector);
        $conds = $new_selector->_conds;
        return $this->where('AND #' . ($id++), $conds);
    }

    /**
     * 查询条件 - like
     *
     * @param string $k
     * @param string $v
     *
     * @return $this
     */
    public function whereLike(string $k, string $v): Selector {
        return $this->where($k . '[~]', $v);
    }

    /**
     * 查询条件 - not like
     *
     * @param string $k
     * @param string $v
     *
     * @return $this
     */
    public function whereNotLike(string $k, string $v): Selector {
        return $this->where($k . '[!~]', $v);
    }

    /**
     * 查询条件 - 系统函数
     *
     * @param string $k
     * @param string $v
     *
     * @return $this
     */
    public function whereFunc(string $k, string $v): Selector {
        return $this->where($k, Moon::raw($v));
    }

    /**
     * 表关联
     *
     * @param int $type
     * @param string $table
     * @param string|array $where
     *
     * @return $this
     */
    protected function _join(int $type, string $table, $where): Selector {
        $all_type                  = ['[>]' /* left joni */, '[<]' /* right join */, '[<>]' /* full join */, '[><]' /* inner join */];
        $c                         = $all_type[$type] ?? $all_type[3];
        $this->_joins[$c . $table] = $where;
        return $this;
    }

    /**
     * 关联查询
     *
     * @param int $type
     * @param string|\Selector $table
     * @param \Moon\callable $func
     *
     * @return boolean|$this
     */
    protected function _joinF(int $type, $table, callable $func) {
        if (is_array($table)) {
            $alias        = $table[1] ?? '';
            $table        = $table[0];
            $table        = $table->needSelector();
            $table->alias = $alias;
        } else if (is_object($table)) {
            if ($table instanceof Selector) {
                //nothing
            } else if ($table instanceof Table) {
                $table = $table->needSelector();
            } else {
                return false;
            }
        } else if (is_string($table)) {
            $alias = '';
            if (preg_match('/(\w+)\((\w+)\)?/', $table, $info)) {
                $table = $info[1];
                $alias = $info[2] ?? '';
            }
            $table = new static($table, $alias);
        } else {
            return false;
        }
        if ($table->alias == 'this') {
            $table->alias = '';
        }
        $new_selector = new static($this->table, $this->alias);
        call_user_func($func, $new_selector);
        $where = $new_selector->_conds;
        return $this->_join($type, $table->tableName(), $where);
    }

    /**
     * 内连接
     *
     * @param string $table
     * @param callable $func
     *
     * @return $this
     */
    public function join($table, callable $func) {
        return $this->_joinF(self::JOIN_INNER, $table, $func);
    }

    /**
     * 左连接
     *
     * @param string|\Moon\Selector $table
     * @param callable $func
     *
     * @return $this
     */
    public function joinLeft($table, callable $func) {
        return $this->_joinF(self::JOIN_LEFT, $table, $func);
    }

    /**
     * 右连接
     *
     * @param string|\Moon\Selector $table
     * @param callable $func
     *
     * @return $this
     */
    public function joinRight($table, callable $func) {
        return $this->_joinF(self::JOIN_RIGHT, $table, $func);
    }

    /**
     * 外连接
     *
     * @param string|\Moon\Selector $table
     * @param callable $func
     *
     * @return $this
     */
    public function joinFull($table, callable $func) {
        return $this->_joinF(self::JOIN_FULL, $table, $func);
    }

    /**
     * 组合
     *
     * @param string|array $groupBy
     *
     * @return $this
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
     *
     * @param \Moon\callable $func
     *
     * @return $this
     */
    public function having(callable $func): Selector {
        $new_selector = new static($this->table, $this->alias);
        $func($new_selector);
        $having        = $new_selector->_conds;
        $this->_having = array_merge($this->_having, $having);
        return $this;
    }

    /**
     * HAVING
     *
     * @param Raw $raw
     *
     * @return $this
     */
    public function havingRaw(Raw $raw) {
        $this->_having = $raw;
        return $this;
    }

    /**
     * 排序
     *
     * @param string|array $k
     * @param string|array $sort
     *
     * @return $this
     */
    public function orderBy($k, $sort = 'ASC'): Selector {
        if (is_array($k)) {
            array_walk($k, function (&$item) {
                $item = strtoupper($item);
            });
            $this->_order = array_merge($this->_order, $k);
        } else {
            $this->_order[$k] = strtoupper($sort);
        }
        return $this;
    }

    /**
     * limit
     *
     * @param int $offset
     * @param int $len
     *
     * @return $this
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
     *
     * @return $this
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
     *
     * @return $this
     */
    public function valueJson(string $k, array $v): Selector {
        return $this->value($k . '[JSON]', $v);
    }

    /**
     *
     * @param string $k
     * @param array $v
     *
     * @return $this
     */
    public function valueAdd(string $k, $v): Selector {
        return $this->value($k . '[+]', $v);
    }

    /**
     *
     * @param string $k
     * @param array $v
     *
     * @return $this
     */
    public function valueSub(string $k, $v): Selector {
        return $this->value($k . '[-]', $v);
    }

    /**
     *
     * @param string $k
     * @param array $v
     *
     * @return $this
     */
    public function valueMul(string $k, $v): Selector {
        return $this->value($k . '[*]', $v);
    }

    /**
     *
     * @param string $k
     * @param array $v
     *
     * @return $this
     */
    public function valueDiv(string $k, $v): Selector {
        return $this->value($k . '[/]', $v);
    }

    /**
     *
     * @param string $k
     * @param string $v
     *
     * @return $this
     */
    public function valueFunc(string $k, string $v): Selector {
        return $this->value($k, Moon::raw($v));
    }

    /**
     * 构建medoo可用的where数组
     *
     * @return array
     */
    public function contextWhere(): array {
        $where = $this->_conds;
        if (count($this->_group) > 0) {
            $where['GROUP'] = $this->_group;
        }
        if (($this->_having instanceof Raw) || count($this->_having) > 0) {
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
     *
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
     *
     * @param string $col_name
     *
     * @return string
     */
    public function col(string $col_name) {
        if (empty($this->alias)) {
            return $this->table . '.' . $col_name;
        }
        return $this->alias . '.' . $col_name;
    }

    /**
     * 设置别名
     *
     * @param string $alias
     *
     * @return $this
     */
    public function setAlias(string $alias) {
        $this->alias = $alias;
        return $this;
    }

}

/**
 * @method $this select(string|array $c, string $alias = null, string $type = '') 添加要查询的字段
 * @method $this selectStruct(array $c) 构造获取的结构
 * @method $this selectInt(string|array $c, string $alias = null) 添加要查询的字段 - Int类型
 * @method $this selectBool(string|array $c, string $alias = null) 添加要查询的字段 - Bool类型
 * @method $this selectNumber(string|array $c, string $alias = null) 添加要查询的字段 - Number类型
 * @method $this selectJson(string|array $c, string $alias = null) 添加要查询的字段 - Json类型
 * @method string f2Int(string $col) 搜索的列名加上类型
 * @method string f2Num(string $col) 搜索的列名加上类型
 * @method string f2Bool(string $col) 搜索的列名加上类型
 * @method string f2Json(string $col) 搜索的列名加上类型
 * @method $this where(string|array $k, string|array $v = null) 添加查询条件
 * @method $this whereNot(string $k, string $v) 添加查询条件 - 不等于
 * @method $this whereGT(string $k, string $v) 添加查询条件 - 大于
 * @method $this whereGE(string $k, string $v) 添加查询条件 - 大于等于
 * @method $this whereLT(string $k, string $v) 添加查询条件 - 小于
 * @method $this whereLE(string $k, string $v) 添加查询条件 - 小于等于
 * @method $this whereIn(string $k, array $v) 添加查询条件 - in
 * @method $this whereNotIn(string $k, array $v) 添加查询条件 - not in
 * @method $this whereNull(string $k) 添加查询条件 - is null
 * @method $this whereNotNull(string $k) 添加查询条件 - is not null
 * @method $this whereBetween(string $k, $v1, $v2) 添加查询条件 - 介于两者之间
 * @method $this whereNotBetween(string $k, $v1, $v2) 添加查询条件 - 不在两者之间
 * @method $this whereOr(callable $func) 添加查询条件 - or
 * @method $this whereAnd(callable $func) 添加查询条件 - and
 * @method $this whereLike(string $k, string $v) 添加查询条件 - like
 * @method $this whereNotLike(string $k, string $v) 添加查询条件 - not like
 * @method $this whereFunc(string $k, string $v) 添加查询条件 - 系统函数(raw形式)
 *
 * @method $this join(string|array|Table $table, callable $func) 联表查询 - inner join
 * @method $this joinLeft(string|array|Table $table, callable $func) 联表查询 - left join
 * @method $this joinRight(string|array|Table $table, callable $func) 联表查询 - right join
 * @method $this joinFull(string|array|Table $table, callable $func) 联表查询 - OUTER join
 *
 * @method $this groupBy(string|array $groupBy) 组合
 * @method $this orderBy(string|array $k, string $sort = 'ASC') 排序
 * @method $this having(callable $func) Having
 * @method $this havingRaw(Raw $raw) Having
 * @method $this limit(int $offset, int $len = 0) limit
 *
 * @method $this value(string|value $k, $v = null) 修改数据
 * @method $this valueJson(string $k, array $v) 修改数据 - json格式
 * @method $this valueAdd(string $k, int $v) 修改数据 - 自加
 * @method $this valueSub(string $k, int $v) 修改数据 - 自减
 * @method $this valueMul(string $k, int $v) 修改数据 - 自乘
 * @method $this valueDiv(string $k, int $v) 修改数据 - 自除
 * @method $this valueFunc(string $k, string $v) 修改数据 - raw
 *
 * @method string tableName(bool $bAlias = true) 表名称
 * @method string col(string $col_name) 返回列名
 * @method $this setAlias(string $alias) 设置别名
 */
abstract class Table {

    const CONN_TYPE_APPOINT = 0; //指定
    const CONN_TYPE_WRITER  = 1; //总是使用writer

    public    $table;  //表明
    public    $alias             = '';  //别名
    protected $selector;
    protected $alwaysUseConnType = self::CONN_TYPE_APPOINT;   //0=自动区分

    /**
     * @var \Moon\Moon
     */
    public    $moon;
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
     *
     * @return \Moon\Moon
     */
    public function handler() {
        return $this->moon;
    }

    /**
     * 获取一个查询构造器
     *
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
     *
     * @return \Moon\Model
     */
    public function debug(): Model {
        $this->_debug = true;
        return $this;
    }

    /**
     * 总是使用writer
     *
     * @return $this
     */
    public function alwaysWriter() {
        $this->alwaysUseConnType = self::CONN_TYPE_WRITER;
        return $this;
    }

    /**
     * 恢复主从连接的使用
     *
     * @return $this
     */
    public function recoverConnType() {
        $this->alwaysUseConnType = self::CONN_TYPE_APPOINT;
        return $this;
    }

    /**
     * @return \Moon\Connection
     */
    protected function _getInternalReader() {
        if ($this->alwaysUseConnType == self::CONN_TYPE_WRITER) {
            $conn = $this->_getInternalWriter();
        } else {
            $conn = $this->moon->getReader();
        }
        return $conn;
    }

    /**
     * @return \Moon\Connection
     */
    protected function _getInternalWriter() {
        $conn = $this->moon->getWriter();
        return $conn;
    }

    /**
     * 重置连接
     *
     * @return $this
     */
    protected function _resetInternalReader() {
        if ($this->alwaysUseConnType == self::CONN_TYPE_WRITER) {
            $this->_resetInternalWriter();
        } else {
            $this->moon->resetReader();
        }
        return $this;
    }

    /**
     * 重置连接
     *
     * @return $this
     */
    protected function _resetInternalWriter() {
        $this->moon->resetWriter();
        return $this;
    }

    /**
     * 获取第一条数据
     *
     * @return array
     * @throws \Exception
     */
    public function first() {
        return Moon::retry(function () {
            $conn = $this->_getInternalReader();
            if ($this->_debug) {
                $conn->debug();
                $this->_debug = false;
            }
            $selector = $this->needSelector();
            $ret      = $conn->fetch($selector);
            $selector->clear();
            return $ret;
        }, function () {
            $this->_resetInternalReader();
        }, $this->moon->getRecTimes());

    }

    /**
     * 获取符合条件的数据
     *
     * @return \Moon\Collection
     * @throws \Exception
     */
    public function all() {
        return Moon::retry(function () {
            $conn = $this->_getInternalReader();
            if ($this->_debug) {
                $conn->debug();
                $this->_debug = false;
            }
            $selector = $this->needSelector();
            $ret      = $conn->fetchAll($selector);
            $selector->clear();
            return $ret;
        }, function () {
            $this->_resetInternalReader();
        }, $this->moon->getRecTimes());
    }

    /**
     * 插入数据
     *
     * @return int
     * @throws \Exception
     */
    public function insert() {
        return Moon::retry(function () {
            $conn = $this->_getInternalWriter();
            if ($this->_debug) {
                $conn->debug();
                $this->_debug = false;
            }
            $selector = $this->needSelector();
            $ret      = $conn->insert($selector);
            $selector->clear();
            return $ret;
        }, function () {
            $this->_resetInternalWriter();
        }, $this->moon->getRecTimes());
    }

    /**
     * 更新数据
     *
     * @return int
     * @throws \Exception
     */
    public function update() {
        return Moon::retry(function () {
            $conn = $this->_getInternalWriter();
            if ($this->_debug) {
                $conn->debug();
                $this->_debug = false;
            }
            $selector = $this->needSelector();
            $ret      = $conn->update($selector);
            $selector->clear();
            return $ret;
        }, function () {
            $this->_resetInternalWriter();
        }, $this->moon->getRecTimes());
    }

    /**
     * 删除数据
     *
     * @return int
     * @throws \Exception
     */
    public function delete() {
        return Moon::retry(function () {
            $conn = $this->_getInternalWriter();
            if ($this->_debug) {
                $conn->debug();
                $this->_debug = false;
            }
            $selector = $this->needSelector();
            $ret      = $conn->delete($selector);
            $selector->clear();
            return $ret;
        }, function () {
            $this->_resetInternalWriter();
        }, $this->moon->getRecTimes());
    }

    /**
     * 返回数据条目
     *
     * @return int
     * @throws \Exception
     */
    public function count() {
        return Moon::retry(function () {
            $conn = $this->_getInternalReader();
            if ($this->_debug) {
                $conn->debug();
                $this->_debug = false;
            }
            $selector = $this->needSelector();
            $ret      = $conn->rowCount($selector);
            $selector->clear();
            return $ret;
        }, function () {
            $this->_resetInternalReader();
        }, $this->moon->getRecTimes());
    }

    /**
     * 每次查询一条记录，并以传参的方式传入到回调函数$callback中
     * 这个操作对于大数据查询是安全有效的
     *
     * @param callable $callback
     *
     * @return bool|int
     * @throws \Exception
     */
    public function each($callback) {
        return Moon::retry(function () use ($callback) {
            $conn = $this->_getInternalReader();
            if ($this->_debug) {
                $conn->debug();
                $this->_debug = false;
            }
            $selector = $this->needSelector();
            $ret      = $conn->fetchRow($selector, $callback);
            $selector->clear();
            return $ret;
        }, function () {
            $this->_resetInternalReader();
        }, $this->moon->getRecTimes());
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
    protected $_metadata          = [];
    protected $_curdata           = [];

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

        $this->__init();
    }

    /**
     * 用户自定义的初始化函数
     */
    public function __init() {

    }

    /**
     * 通过数组参数初始化
     *
     * @param array $data
     *
     * @return self
     */
    public static function initBy(array $data) {
        $model            = new static();
        $model->_metadata = $data;
        $model->_curdata  = $data;
        return $model;
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
     *
     * @param string $column_name
     *
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
     *
     * @param string $column_name
     *
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
     *
     * @param string $column
     * @param mixed $value
     *
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
     *
     * @param string $column
     *
     * @return mixed
     */
    public function getData(string $column = '') {
        if (empty($column)) {
            return $this->_curdata;
        }
        return $this->_curdata[$column] ?? null;
    }

    /**
     * 获取原始数据
     *
     * @param string $column
     * @param mixed $def
     *
     * @return mixed
     */
    public function getMetaData(string $column = '', $def = null) {
        if (empty($column)) {
            return $this->_metadata;
        }
        return $this->_metadata[$column] ?? null;
    }

    /**
     * 获取主键
     *
     * @return int
     */
    public function getPrimaryValue() {
        return $this->_curdata[$this->primary_key] ?? null;
    }

    /**
     * 设置主键数据
     *
     * @param mixed $v
     *
     * @return \Moon\Model
     */
    public function setPrimaryValue($v): Model {
        $this->_curdata[$this->primary_key] = $v;
        return $this;
    }

    /**
     * 更新时间戳
     *
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
     *
     * @return \Moon\Collection
     */
    public function all() {
        if (is_array($this->query_columns)) {
            $this->needSelector()->select($this->query_columns);
        }
        return parent::all();
    }

    /**
     * 每次查询一条记录，并以传参的方式传入到回调函数$callback中
     * 这个操作对于大数据查询是安全有效的
     *
     * @param callable $callback
     */
    public function each($callback) {
        if (is_array($this->query_columns)) {
            $this->needSelector()->select($this->query_columns);
        }
        return parent::each($callback);
    }

    /**
     * 加载数据
     *
     * @param mixed $value
     * @param string $key
     *
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
        $this->_curdata  = $data;
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
     *
     * @return boolean
     */
    public function save($reload = false) {
        $primary_value = $this->getPrimaryValue();
        $b_insert      = false;
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
            //重载时默认使用writer
            $old_type = $this->alwaysUseConnType;
            $this->alwaysWriter();
            $this->reload();
            $this->recoverConnType();
            $this->alwaysUseConnType = $old_type;
        }
        return $ret;
    }

    /**
     * 删除当前模型数据
     *
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

/**
 * Class RetryException
 *
 * @package Moon
 */
class RetryException extends \Exception {

}

