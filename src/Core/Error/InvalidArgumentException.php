<?php


namespace Moon\Core\Error;


class InvalidArgumentException extends Excpetion {

    /**
     * @param $obj
     * @return string
     */
    public static function getType($obj) {
        if (is_object($obj)) {
            return 'object<' . get_class($obj) . '>';
        }
        return getType($obj);
    }

}