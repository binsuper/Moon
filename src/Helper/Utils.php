<?php

namespace Moon\Helper;

use Moon\Core\Error\InvalidArgumentException;

class Utils {

    /**
     * 拼接字符串
     * @param mixed $strs
     * @return string
     */
    public static function concat(string ...$strs): string {
        return implode('', $strs);
    }

    /**
     * 获取数据类型
     * @param mixed $var 变量
     * @return string
     */
    public static function typeof($var): string {
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
     * 获取数组中特定键名的数据
     * 如果数据为空或者不存在，则抛出异常
     * @param array $arr 数组
     * @param string|int ...$keys 键名
     * @return mixed
     */
    public static function getNotEmpty(array $arr, ...$keys) {
        if (count($keys) > 1) {

            $stack = [];

            foreach ($keys as $key) {

                if (empty($arr[$key])) {
                    throw new InvalidArgumentException('参数<' . $key . '>不允许为空');
                }
                $stack[$key] = $arr[$key];

            }

            return $stack;

        } else {

            if (empty($arr[$keys[0]])) {
                throw new InvalidArgumentException('参数<' . $keys[0] . '>不允许为空');
            }

            return $arr[$keys[0]];

        }
    }

}