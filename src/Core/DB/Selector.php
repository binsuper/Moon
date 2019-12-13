<?php

namespace Moon\Core\DB;

use Moon\Core\Error\InvalidArgumentException;
use Moon\Helper\Utils;

class Selector {

    // 连表类型
    const JOIN_INNER = 0;
    const JOIN_LEFT  = 1;
    const JOIN_RIGHT = 2;
    const JOIN_FULL  = 3;

    // 查询字段的类型
    const COL_TYPE_STRING = '[String]'; //默认
    const COL_TYPE_INT    = '[Int]';
    const COL_TYPE_BOOL   = '[Bool]';
    const COL_TYPE_NUMBER = '[Number]';
    const COL_TYPE_OBJECT = '[Object]';
    const COL_TYPE_JSON   = '[Json]';

    // 比较符
    const COND_EQ     = '=';    // 等于
    const COND_NEQ    = '!';    // 不等于
    const COND_LT     = '<';    // 小于
    const COND_LE     = '<=';   // 小于等于
    const COND_GT     = '>';    // 大于
    const COND_GE     = '>=';   // 大于等于
    const COND_IN     = '[]';   // 包含在列表中
    const COND_NIN    = '][';   // 不包含在列表中
    const COND_LK     = '~';    // 相似
    const COND_NLK    = '!~';   // 不相似
    const COND_REGEXP = '~~';   // 正则匹配
    const COND_BTW    = '<>';   // 介于两者之间(包含边界)
    const COND_NBTW   = '><';   // 不在两者之间(不包含边界)
    const COND_AND    = '&&';   // 条件之间的关系 - 且
    const COND_OR     = '||';   // 条件之间的关系 - 或

    // 排序
    const ORDER_ASC  = 'asc';   // 升序
    const ORDER_DESC = 'desc';  // 降序

    // 数据操作类型
    const VALUE_TYPE_SET  = '=';    // 设置
    const VALUE_TYPE_ADD  = '+';    // 增加
    const VALUE_TYPE_SUB  = '-';    // 减少
    const VALUE_TYPE_MUL  = '*';    // 乘积
    const VALUE_TYPE_DIV  = '/';    // 相除
    const VALUE_TYPE_JSON = 'JSON'; // JSON

    protected string $_table;  //表名
    protected string $_alias;  //别名

    protected array $_columns = [];
    protected array $_joins = [];
    protected array $_conds = [];
    protected array $_order = [];
    protected array $_group = [];
    protected array $_having = [];
    protected array $_limit = [];
    protected array $_values = [];          //更新或插入的数据列表
    protected array $_multi_values = [];    // 批量插入或更新的数据列表

    /**
     * 检索列与类型的对应关系
     * @var array
     */
    protected array $_column_type_map = [];

    public function __construct(string $table, string $alias = '') {
        $this->_table = $table;
        $this->_alias = $alias;
    }

    /**
     * 表名
     * @return string
     */
    public function tableName(): string {
        return $this->_table;
    }

    /**
     * 别名
     * @return string
     */
    public function aliasName(): string {
        return $this->_alias;
    }

    /**
     * 设置别名
     * @param string $alias
     * @return $this
     */
    public function alias(string $alias): self {
        $this->_alias = $alias;
        return $this;
    }

    /**
     * 查询条件 - 通用
     * @param string|array|Raw $field
     * @param string $op
     * @param mixed|static|Raw $dest
     * @return $this
     */
    protected function _where($field, string $op, $dest = null): self {
        if (is_array($field)) {
            foreach ($field as $key => $val) {
                $this->_conds[] = [$op, $key, $val];
            }
        } else if ($field instanceof Raw) {
            if (is_null($dest)) {
                $this->_conds[] = [$op, null, $field];
            } else {
                $this->_conds[] = [$op, $field, $dest];
            }
        } else {
            $this->_conds[] = [$op, $field, $dest];
        }
        return $this;
    }

    /**
     * 查询条件 - 相等
     * 支持子查询
     * @param string|array|Raw $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function where($col, $val = null): self {
        return $this->_where($col, self::COND_EQ, $val);
    }

    /**
     * 查询条件 - 不相等
     * 支持子查询
     * @param string|array|Raw $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function whereNot($col, $val = null): self {
        return $this->_where($col, self::COND_NEQ, $val);
    }

    /**
     * 查询条件 - 小于
     * 支持子查询
     * @param string|array|Raw $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function whereLT($col, $val = null): self {
        return $this->_where($col, self::COND_LT, $val);
    }

    /**
     * 查询条件 - 小于等于
     * 支持子查询
     * @param string|array|Raw $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function whereLE($col, $val = null): self {
        return $this->_where($col, self::COND_LE, $val);
    }

    /**
     * 查询条件 - 大于
     * 支持子查询
     * @param string|array|Raw $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function whereGT($col, $val = null): self {
        return $this->_where($col, self::COND_GT, $val);
    }

    /**
     * 查询条件 - 大于等于
     * 支持子查询
     * @param string|array|Raw $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function whereGE($col, $val = null): self {
        return $this->_where($col, self::COND_GE, $val);
    }

    /**
     * 查询条件 - 相似
     * 支持子查询
     * @param string|array $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function whereLike($col, $val = null): self {
        return $this->_where($col, self::COND_LK, $val);
    }

    /**
     * 查询条件 - 不相似
     * 支持子查询
     * @param string|array $col
     * @param mixed|static|Raw $val
     * @return $this
     */
    public function whereNotLike($col, $val = null): self {
        return $this->_where($col, self::COND_NLK, $val);
    }

    /**
     * 查询条件 - 在列表中
     * 支持子查询
     * @param string $col
     * @param array|static|Raw $val
     * @return $this
     */
    public function whereIn(string $col, $val): self {
        return $this->_where($col, self::COND_IN, $val);
    }

    /**
     * 查询条件 - 不在列表中
     * 支持子查询
     * @param string $col
     * @param array|static|Raw $val
     * @return $this
     */
    public function whereNotIn(string $col, $val): self {
        return $this->_where($col, self::COND_NIN, $val);
    }

    /**
     * 查询条件 - 在两者之间（包含边界）
     * @param string|Raw $col
     * @param mixed|Raw $val
     * @return $this
     */
    public function whereBetween($col, $val): self {
        return $this->_where($col, self::COND_BTW, $val);
    }

    /**
     * 查询条件 - 不在两者之间（不包含边界）
     * @param string|Raw $col
     * @param mixed|Raw $val
     * @return $this
     */
    public function whereNotBetween($col, $val): self {
        return $this->_where($col, self::COND_NBTW, $val);
    }

    /**
     * 查询条件 - 为空
     * @param string $col
     * @return $this
     */
    public function whereNull(string $col): self {
        return $this->where($col, null);
    }

    /**
     * 查询条件 - 不为空
     * @param string $col
     * @return $this
     */
    public function whereNotNull(string $col): self {
        return $this->whereNot($col, null);
    }

    /**
     * 查询条件关系 - 且
     * @param callable $func 接受一个参数，类型与当前实例一致
     * @return $this
     */
    public function whereAnd(callable $func): self {
        $new_selector = new static($this->_table, $this->_alias);
        call_user_func($func, $new_selector);
        if (empty($new_selector->_conds)) {
            return $this;
        }
        return $this->_where(null, self::COND_AND, $new_selector->_conds);
    }

    /**
     * 查询条件关系 - 或
     * @param callable $func 接受一个参数，类型与当前实例一致
     * @return $this
     */
    public function whereOr(callable $func): self {
        $new_selector = new static($this->_table, $this->_alias);
        call_user_func($func, $new_selector);
        if (empty($new_selector->_conds)) {
            return $this;
        }
        return $this->_where(null, self::COND_OR, $new_selector->_conds);
    }

    /**
     * 查询条件关系 - 正则匹配
     * @param string $col
     * @param string $pattern
     * @return $this
     */
    public function whereRegexp(string $col, string $pattern): self {
        return $this->_where($col, self::COND_REGEXP, $pattern);
    }

    /**
     * 设置检索字段的数据类型
     * @param string $col
     * @param string $type
     * @return $this
     */
    public function setColumnType(string $col, string $type): self {
        $this->_column_type_map[$col] = $type;
        return $this;
    }

    /**
     * 获取检索字段的数据类型
     * @param string|null $col
     * @return array|string|null
     */
    public function getColumnType(?string $col = null) {
        if ($col) {
            return $this->_column_type_map[$col] ?? null;
        }
        return $this->_column_type_map;
    }

    /**
     * 添加检索的字段名
     * @param mixed $col 字段名
     * @param string|null $alias 别名
     * @return $this
     * @throws InvalidArgumentException
     */
    protected function _column($col, ?string $alias = null): self {
        if ($col instanceof Raw || $col instanceof self) {

            if ($alias) {
                $this->_columns[$alias] = $col;
            } else {
                $this->_columns[] = $col;
            }

        } else if (is_string($col)) {

            $cols = explode(',', $col);

            if (count($cols) > 1) {
                array_walk($cols, fn(&$val) => $val = trim($val));
                return $this->_column($cols);
            }

            // if column alias
            if ($alias) {
                $this->_columns[$alias] = $col;
            } else {
                $this->_columns[] = $col;
            }

        } else if (is_array($col)) {

            $this->_columns = array_merge($this->_columns, $col);

        }
        return $this;
    }

    /**
     * 添加检索的字段名 - 通用
     * @param mixed $col
     * @param string|null $alias
     * @return $this
     * @throws InvalidArgumentException
     */
    public function column($col, ?string $alias = null): self {
        return $this->_column($col, $alias);
    }

    /**
     * 添加检索的字段名 - 子查询
     * @param Selector $selector
     * @param string $alias
     * @return $this
     * @throws InvalidArgumentException
     */
    public function columnSubquery(Selector $selector, string $alias): self {
        return $this->_column($selector, $alias);
    }

    /**
     * 添加检索的字段名 - 整型
     * @param string $col
     * @param string|null $alias
     * @return $this
     * @throws InvalidArgumentException
     */
    public function columnInt(string $col, ?string $alias = null): self {
        $this->_column($col, $alias);
        $this->setColumnType($col, self::COL_TYPE_INT);
        return $this;
    }

    /**
     * 表关联查询
     * @param int $type 类型
     * @param string|static $table 表明
     * @param array $on 条件
     * @return $this
     */
    protected function _join(int $type, $table, array $on): self {

        $alias = '';

        if (is_string($table)) {

            if (preg_match('/(?<table>[a-zA-Z0-9_]+)(?:\((?<alias>[a-zA-Z0-9_]+)\))?/', $table, $match)) {
                $alias = $match['alias'] ?? '';
                $table = $match['table'];
            }

        } else if ($table instanceof self) {

            $alias = $table->aliasName();
            $table = $table->tableName();

        } else {
            throw new InvalidArgumentException(Utils::concat('arguments #2 must be string or object instanceof Selector, ', Utils::typeof($table), ' given'));
        }

        $this->_joins[] = [$type, $table, $alias, $on];

        return $this;

    }

    /**
     * 内联查询
     * @param string|static $table 表名
     * @param array $on 条件，格式['join col'=>'other col']
     * @return $this
     */
    public function join($table, array $on): self {
        return $this->_join(self::JOIN_INNER, $table, $on);
    }

    /**
     * 左联查询
     * @param string|static $table 表名
     * @param array $on 条件，格式['join col'=>'other col']
     * @return $this
     */
    public function joinLeft($table, array $on): self {
        return $this->_join(self::JOIN_LEFT, $table, $on);
    }

    /**
     * 右联查询
     * @param string|static $table 表名
     * @param array $on 条件，格式['join col'=>'other col']
     * @return $this
     */
    public function joinRight($table, array $on): self {
        return $this->_join(self::JOIN_RIGHT, $table, $on);
    }

    /**
     * 外联查询
     * @param string|static $table 表名
     * @param array $on 条件，格式['join col'=>'other col']
     * @return $this
     */
    public function joinFull($table, array $on): self {
        return $this->_join(self::JOIN_FULL, $table, $on);
    }

    /**
     * 组合查询
     * @param array|string|Raw $field
     * @return $this
     */
    public function group($field): self {
        if (is_array($field)) {
            $this->_group = array_merge($this->_group, $field);
        } else {
            $this->_group[] = $field;
        }
        return $this;
    }

    /**
     * 聚合查询
     * 内部数据与where保持一致
     * @param Raw|callable $val 函数接受一个参数，类型为Selector或其子类
     * @return $this
     */
    public function having($val): self {

        if (is_callable($val)) {

            $new_selector = new static($this->_table, $this->_alias);
            call_user_func($val, $new_selector);
            if (empty($new_selector->_conds)) {
                return $this;
            }
            $this->_having = array_merge($this->_having, $new_selector->_conds);

        } else if ($val instanceof Raw) {

            $new_selector = new static($this->_table, $this->_alias);
            $new_selector->where($val);
            if (empty($new_selector->_conds)) {
                return $this;
            }
            $this->_having = array_merge($this->_having, $new_selector->_conds);

        }

        return $this;
    }

    /**
     * 排序
     * @param string|array|Raw $col
     * @param string $rule
     * @return $this
     */
    protected function _order($col, string $rule): self {

        if (is_string($col)) {

            $this->_order[$rule][] = $col;

        } else if (is_array($col)) {

            foreach ($col as $item) {
                $this->_order[$rule][] = $item;
            }

        }

        return $this;
    }

    /**
     * 升序排序
     * @param string|array $col
     * @return $this
     */
    public function orderAsc($col): self {
        return $this->_order($col, self::ORDER_ASC);
    }

    /**
     * 降序排序
     * @param string|array $col
     * @return $this
     */
    public function orderDesc($col): self {
        return $this->_order($col, self::ORDER_DESC);
    }

    /**
     * 设置插入或更新数据
     * @param string $col 字段名
     * @param mixed|Raw|static $val 数据
     * @param string|null $type 类型
     * @return $this
     */
    protected function _value(string $col, $val, ?string $type = null): self {
        $this->_values[$col] = [$val, $type];
        return $this;
    }

    /**
     * 数据 - 设置
     * @param string $col
     * @param mixed|Raw|static $val
     * @return $this
     */
    public function value(string $col, $val): self {
        return $this->_value($col, $val, self::VALUE_TYPE_SET);
    }

    /**
     * 数据 - 增加
     * @param string $col
     * @param mixed|Raw|static $val
     * @return $this
     */
    public function valueAdd(string $col, float $val): self {
        return $this->_value($col, $val, self::VALUE_TYPE_ADD);
    }

    /**
     * 数据 - 减少
     * @param string $col
     * @param mixed|Raw|static $val
     * @return $this
     */
    public function valueSub(string $col, float $val): self {
        return $this->_value($col, $val, self::VALUE_TYPE_SUB);
    }

    /**
     * 数据 - 乘积
     * @param string $col
     * @param mixed|Raw|static $val
     * @return $this
     */
    public function valueMul(string $col, float $val): self {
        return $this->_value($col, $val, self::VALUE_TYPE_MUL);
    }

    /**
     * 数据 - 相除
     * @param string $col
     * @param mixed|Raw|static $val
     * @return $this
     */
    public function valueDiv(string $col, float $val): self {
        return $this->_value($col, $val, self::VALUE_TYPE_DIV);
    }

    /**
     * 设置为json格式数据
     * @param string $col
     * @param mixed|Raw|static $val
     * @return $this
     */
    public function valueJson(string $col, $val): self {
        return $this->_value($col, $val, self::VALUE_TYPE_JSON);
    }

    /**
     * 将当前已设置的value数据增加到multi列表中，并置空value.
     * 这将会影响contextValue函数返回的数据结构.
     * @return $this
     */
    public function multiValue(): self {
        if (!empty($this->_values)) {
            $this->_multi_values[] = $this->_values;
            $this->_values = [];
        }
        return $this;
    }

    /**
     * @return bool
     */
    public function isMulti(): bool {
        return !empty($this->_multi_values);
    }

    /**
     * where 格式
     * [
     *      [op, key, val],
     *      [or, null, [
     *          ['=', 'a', 1],
     *          ['=', 'b', 2],
     *          ['and', null, [
     *              ['=', 'c', 1],
     *              ['=', 'b', 2],
     *          ]],
     *      ]]
     * ]
     *
     * join 格式
     * [
     *      [type, table, alias, where]
     * ]
     *
     * @return array
     */
    public function contextWhere(): array {
        $where = [];
        if (!empty($this->_conds)) {
            $where['WHERE'] = $this->_conds;
        }
        if (count($this->_group)) {
            $where['GROUP'] = $this->_group;
        }
        if (count($this->_having)) {
            $where['HAVING'] = $this->_having;
        }
        if (count($this->_order)) {
            $where['ORDER'] = $this->_order;
        }
        /*
        if (count($this->_limit) > 0) {
            if (!isset($this->_limit[1])) {
                $where['LIMIT'] = $this->_limit[0];
            } else {
                $where['LIMIT'] = $this->_limit;
            }
        }
        */
        return $where;
    }

    /**
     * 格式：
     *  [
     *      'A',
     *      'B(alias)',
     *      'alias' => 'C',
     *      'alias' => Object<Raw>,
     *      'base' => ['D', 'E']    //structs
     * ]
     *
     * @return array
     */
    public function contextColumn(): array {
        // ['uername' => 'name']
        // [ ['a' => ['name', '']] ]
        // [ Raw, 'a' => 'b', [], 'c' ]

        return $this->_columns;
    }

    /**
     * 格式：
     * joins:
     * [
     *      ['table', 'alias', join_type, on],
     * ]
     *
     * on: 和 where 一致
     *
     * @return array
     */
    public function contextJoin(): array {
        return $this->_joins;
    }

    /**
     * 格式：
     * [
     *      field => [value, data_type]
     * ]
     *
     * @return array
     */
    public function contextValue(): array {
        if ($this->isMulti()) {
            return $this->_multi_values;
        }
        return $this->_values;
    }

}