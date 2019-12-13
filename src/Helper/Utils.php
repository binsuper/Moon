<?php

namespace Moon\Helper;

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
    public static function typeof($var) {
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

}