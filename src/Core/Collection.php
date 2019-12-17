<?php

namespace Moon\Core;

use Moon\Core\DB\Selector;

class Collection extends \ArrayObject {

    /**
     * 遍历集合中的所有元素
     * @param callable $func 回调函数, 接受一个参数，类型为数组
     * @return Collection
     */
    public function each(callable $func): self {
        foreach ($this as $data) {
            call_user_func($func, $data);
        }
        return $this;
    }

    /**
     * 渲染数据类型
     * @param array $column_type
     * @return Collection
     */
    public function render(array $column_type): self {
        foreach ($this as &$data) {
            array_walk($data, function (&$item, $col) use ($column_type) {
                $data_type = $column_type[$col] ?? null;
                if ($data_type === null) {
                    return;
                }
                switch ($data_type) {
                    case Selector::COL_TYPE_INT:
                        $item = intval($item);
                        break;
                    case Selector::COL_TYPE_STRING:
                        $item = strval($item);
                        break;
                    case Selector::COL_TYPE_BOOL:
                        $item = boolval($item);
                        break;
                    case Selector::COL_TYPE_NUMBER:
                        $item = doubleval($item);
                        break;
                    case Selector::COL_TYPE_OBJECT:
                        $item = unserialize($item);
                        break;
                    case Selector::COL_TYPE_JSON:
                        $item = json_decode($item, true);
                        break;
                }
            });
        }
        return $this;
    }

    /**
     * 获取数据集合中的指定列
     * @param string $col
     * @return array
     */
    public function getColumn(string $col): array {
        return array_column((array)$this, $col);
    }

    /**
     * 将数据总的某一列作为数组的键值
     * 然后返回数组
     * @param string|null $col
     * @return array
     */
    public function toArray(?string $col = null): array {

        $result = (array)$this;

        if (!is_null($col)) {
            $result = array_combine($this->getColumn($col), $result);
        }

        return $result;
    }

}