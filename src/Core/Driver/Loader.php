<?php

namespace Moon\Core;

use Moon\Core\Error\Excpetion;

class Loader {

    /**
     * @var array default drivers list
     */
    private static array $__drivers = [
        'PDO' => \Moon\Driver\PDO\Driver::class
    ];

    /**
     * 添加自定义的驱动
     * @param string $driver_name
     * @param string $driver_class_name
     * @throws Excpetion
     */
    public static function addDriver(string $driver_name, string $driver_class_name) {
        if (isset(static::$__drivers[$driver_name])) {
            throw new Excpetion("driver<{$driver_name}> is already exists");
        }
        static::$__drivers[$driver_name] = $driver_class_name;
    }

    /**
     * 获取构造器
     */
    public function getConstructor() {
    }

}