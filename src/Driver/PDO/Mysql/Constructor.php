<?php

namespace Moon\Driver\PDO\Mysql;

use Moon\Core\DB\Raw;
use Moon\Core\DB\Selector;
use Moon\Core\Error\InvalidArgumentException;
use PDO;

class Constructor {

    // 操作符映射
    const OPERATOR_MAP = [
        Selector::COND_EQ     => '=',           // 等于
        Selector::COND_NEQ    => '!=',          // 不等于
        Selector::COND_LT     => '<',           // 小于
        Selector::COND_LE     => '<=',          // 小于等于
        Selector::COND_GT     => '>',           // 大于
        Selector::COND_GE     => '>=',          // 大于等于
        Selector::COND_LK     => 'like',        // 大于等于
        Selector::COND_NLK    => 'not like',    // 大于等于
        Selector::COND_BTW    => 'between',     // 大于等于
        Selector::COND_NBTW   => 'not between', // 大于等于
        Selector::COND_AND    => 'and',         // 且
        Selector::COND_OR     => 'or',          // 或
        Selector::COND_REGEXP => 'REGEXP',      //  正则
    ];

    // php数据类型与PDO数据类型的映射关系
    const VALUE_TYPE_MAP = [
        'NULL'     => PDO::PARAM_NULL,
        'integer'  => PDO::PARAM_INT,
        'double'   => PDO::PARAM_STR,
        'boolean'  => PDO::PARAM_BOOL,
        'string'   => PDO::PARAM_STR,
        'object'   => PDO::PARAM_STR,
        'resource' => PDO::PARAM_LOB,
    ];

    /**
     * 表前缀
     * @var string
     */
    protected string $_prefix;

    /**
     * 选项
     * @var array
     */
    protected array $_options;

    /**
     * @var Selector
     */
    protected Selector $_selector;

    /**
     * @var array
     */
    private array $__data_map = [];

    public function __construct(array $options, Selector $selector) {
        $this->_selector = $selector;
        $this->_options = $options;

        $this->_prefix = $options['prefix'] ?? '';
    }

    /**
     * 为列字段加上引号
     * @param string $col
     * @return string
     */
    public function columnQuote(string $col): string {
        if (false !== strpos($col, '.')) {
            return $this->concat('`', $this->_prefix, str_replace('.', '`.`', $col), '`');
        }
        return $this->concat('`', $col, '`');
    }

    /**
     * 为表名加上引号
     * @param string $table
     * @return string
     */
    public function tableQuote(string $table): string {
        return $this->concat('`', $this->_prefix, $table, '`');
    }

    /**
     * 为Raw对象数据加上引号，并返回字符串数值
     * @param Raw $col
     * @return string
     */
    public function rawQuote(Raw $col): string {
        $val = $col->value();
        if ($val) {
            $val = preg_replace('/([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/', $this->columnQuote('\1.\2'), $val);
        }
        return $val;
    }

    /**
     * @param mixed $obj
     * @return bool
     */
    public function isRaw($obj): bool {
        return $obj instanceof Raw;
    }

    /**
     * @param mixed $obj
     * @return bool
     */
    public function isSelector($obj): bool {
        return $obj instanceof Selector;
    }

    /**
     * 构建select查询语句
     * @param Selector $selector
     * @return string
     */
    protected function _selectContext(Selector $selector) {
        $cols_str = $this->_assembleColumns($selector, $selector->contextColumn());
        $table_str = $this->_assembleTables($selector, $selector->contextJoin());
        $where = $this->_assembleWhere($selector, $selector->contextWhere());
        if (!empty($where)) {
            $where = ' where ' . $where;
        }
        return $this->concat('select ', $cols_str, ' from ', $table_str, $where);
    }

    /**
     * 构建select查询语句
     * @return string
     */
    public function selectContext(): string {
        return $this->_selectContext($this->_selector);
    }

    public function insertContext(): string {
    }

    public function updateContext(): string {
    }

    public function deleteContext(): string {
    }

    /**
     * 构建“字段”语句
     * @param Selector $selector
     * @param array $columns 检索字段信息数组
     * @return string
     */
    protected function _assembleColumns(Selector $selector, array $columns): string {

        if (count($columns) == 0) {
            return '*';
        }

        $stack = [];

        foreach ($columns as $key => $col) {

            // get alias
            $alias = '';
            if ($key && !is_int($key)) { // no number
                $alias = $key;
            }

            if (is_string($col)) {

                // get alias and type in col
                if (false !== strpos($col, '(') || false !== strpos($col, '[')) {
                    preg_match('/(?<column>[a-zA-Z0-9_]+)(?:\((?<alias>[a-zA-Z0-9_]+)\))?(?<type>\[(?:Int|String|Bool|Number|Object|Json)\])?/', $col, $match);
                    if ($match) {
                        $old_col = $col;
                        $col = $match['column'];

                        // set alias
                        if (!$alias) {
                            $alias = $match['alias'] ?? '';
                        }

                        // set type
                        if (isset($match['type'])) {
                            $selector->setColumnType($alias ?: $col, $match['type']);
                        } else if ($selector->getColumnType($old_col)) {
                            $selector->setColumnType($alias ?: $col, $selector->getColumnType($old_col));
                        }

                    }
                }

                if ($alias) {
                    $col = $this->columnQuote($col) . ' as ' . $this->columnQuote($alias);
                } else {
                    $col = $this->columnQuote($col);
                }

                $stack[] = $col;

            } else if (is_object($col) && $this->isRaw($col)) {

                $col_val = $col->value();

                if (is_string($col_val)) {

                    if ($alias) {
                        $stack[] = $this->rawQuote($col) . ' as ' . $this->columnQuote($alias);
                    } else {
                        $stack[] = $this->rawQuote($col);
                    }

                } else if (is_object($col_val) && $this->isSelector($col_val)) {

                    // subquery
                    $stack[] = $this->concat('(', (new static($this->_options, $col_val))->selectContext(), ') as ', $this->columnQuote($alias));

                    // set column type
                    $sub_type_map = $col_val->getColumnType();
                    if (!empty($sub_type_map)) {
                        $selector->setColumnType($alias, array_pop($sub_type_map));
                    }

                }

            } else if (is_array($col)) {

                if (!empty($col)) {
                    $stack[] = $this->_assembleColumns($selector, $col);
                }

            }

        }

        return implode(', ', $stack);

    }

    /**
     * 构建“表”语句
     * @param Selector $selector
     * @param array $joins 连表信息数组
     * @return string
     */
    protected function _assembleTables(Selector $selector, array $joins): string {
        $stack = [];

        // current table
        if (!empty($selector->aliasName())) {
            $stack[] = $this->concat($this->tableQuote($selector->tableName()), ' as ', $this->tableQuote($selector->aliasName()));
        } else {
            $stack[] = $this->tableQuote($selector->tableName());
        }

        // joins


        return implode(' ', $stack);

    }

    /**
     * 构建“检索条件”语句
     * @param Selector $selector
     * @param array $where 条件数组
     * @return string
     */
    protected function _assembleWhere(Selector $selector, array $where): string {

        $stack = [];

        if (!empty($where['where'])) {
            $conditions = $where['where'];
            $stack[] = $this->_whereClause($selector, $conditions);
        }

        return empty($stack) ? '' : implode(' ', $stack);
    }

    /**
     * where条件从句
     * @param Selector $selector
     * @param array $conditions 条件数组
     * @param string|null $glue 连接符，默认为 and
     * @return string
     * @throws InvalidArgumentException
     */
    protected function _whereClause(Selector $selector, array $conditions, ?string $glue = null): string {

        $stack = [];
        if (!$glue) {
            $glue = 'and';
        }

        // scan
        foreach ($conditions as $where) {
            $op = $where[0];
            $key = $where[1];
            $val = $where[2];

            // 无视不支持的操作符
            if (!isset(self::OPERATOR_MAP[$op])) {
                continue;
            }

            // 条件关系
            if (in_array($op, [Selector::COND_AND, Selector::COND_OR])) {
                $stack[] = $this->concat('(', $this->_whereClause($selector, $val, self::OPERATOR_MAP[$op]), ')');
                continue;
            }

            // 解析column字段名
            $column = '';
            if (is_string($column)) {
                $column = $this->columnQuote($key);
            } else if (is_object($column) && $this->isRaw($column)) {
                $column = $this->rawQuote($key);
            }

            // = | != | in | not in
            if (in_array($op, [Selector::COND_EQ, Selector::COND_NEQ])) {

                if (is_null($val)) {

                    $stack[] = $column . ($op === Selector::COND_EQ ? ' is null' : ' is not null');

                    continue;

                } else if (is_array($val)) { // in | not in

                    $key_stack = [];
                    foreach ($val as $item) {
                        $map_key = $this->_addMap($item);
                        $key_stack[] = $map_key;
                    }

                    $stack[] = $this->concat($column, ($op === Selector::COND_EQ ? ' in' : ' not in'), ' (', implode(',', $key_stack), ')');

                    continue;

                }
            } else if (in_array($op, [Selector::COND_BTW, Selector::COND_NBTW])) { // between

                if (is_array($val)) {
                    $stack[] = $this->concat('(', $column, ' ', self::OPERATOR_MAP[$op], ' ', $this->_addMap($val[0]), ' and ', $this->_addMap($val[1]), ')');

                    continue;
                }

            }

            //==============================================
            // apply to all
            //==============================================

            if (is_object($val)) {

                if ($this->isRaw($val)) {

                    $this->_addMap($val);
                    $stack[] = $this->concat('(', $column, ' ', self::OPERATOR_MAP[$op], ' ', $this->rawQuote($val), ')');

                } else if ($this->isSelector($val)) {

                    $stack[] = $this->concat($column, ' ', self::OPERATOR_MAP[$op], ' (', $this->_selectContext($val) . ')');

                }

            } else {

                if (in_array($op, [Selector::COND_LK, Selector::COND_NLK])) {

                    // 若没有手动指定%, 则修改为全匹配
                    if (!preg_match('/(?:%.+|.+%)/', $val)) {
                        $val = $this->concat('%', $val, '%');
                    }

                }

                $map_key = $this->_addMap($val);
                $stack[] = $this->concat($column, ' ', self::OPERATOR_MAP[$op], ' ', $map_key);

            }

        }

        return implode(" {$glue} ", $stack);
    }

    /**
     * 将数据添加到映射表，并返回映射的键名
     * @param mixed $val 绑定到SQL语句的数据
     * @param string|null $map_key 自定义键名
     * @return string
     * @throws InvalidArgumentException
     */
    protected function _addMap($val, ?string $map_key = null): string {
        static $guid = 0;

        if ($this->isRaw($val)) {

            foreach ($val->map() as $key => $item) {
                $this->_addMap($item, $key);
            }

            return '';

        } else {

            $key = $map_key ?: ':MoOn_' . $guid++ . '_NoOm';

            // value type
            $val_type = $this->typeof($val);

            if ($val_type === 'boolean') {
                $val = $val_type ? 1 : 0;
            }

            if (!isset(self::VALUE_TYPE_MAP[$val_type])) {
                throw new InvalidArgumentException('unsupported value type');
            }

            $this->__data_map[$key] = [$val, self::VALUE_TYPE_MAP[$val_type]];

            return $key;

        }
    }

    /**
     * 获取数据映射
     * @return array
     */
    public function getDataMap(): array {
        return $this->__data_map;
    }

    /**
     * 获取数据类型
     * @param mixed $var 变量
     * @return string
     */
    public function typeof($var) {
        if (is_object($var)) {
            return "object";
        }

        if (is_resource($var)) {
            return "resource";
        }

        return (($var === null) ? "NULL" :
            (((bool)$var === $var) ? "boolean" :
                (((float)$var === $var) ? "double" :
                    (((int)$var === $var) ? "integer" :
                        (((string)$var === $var) ? "string" :
                            "unknown"
                        )
                    )
                )
            )
        );
    }

    /**
     * 拼接字符串
     * @param mixed $strs
     * @return string
     */
    public function concat(string ...$strs): string {
        return implode('', $strs);
    }

}