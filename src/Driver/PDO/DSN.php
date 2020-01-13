<?php

namespace Moon\Driver\PDO;

use Moon\Core\Constant;
use Moon\Core\Error\Excpetion;
use Moon\Helper\Utils;

class DSN {

    /**
     * 通过名称获取驱动服务的连接信息
     * @param string $dsn_name 驱动服务的名称
     * @param array $options 其他配置项
     * @return string
     */
    public static function get(string $dsn_name, array $options): string {

        switch ($dsn_name) {
            case 'mysql':
                return static::_getMysql($options);
            default:
                throw new Excpetion('尚未支持 ' . $dsn_name . ' 驱动服务');
        }

    }

    /**
     * 获取mysql的连接信息
     * @param array $options
     * @return string
     */
    protected static function _getMysql(array $options): string {
        $dsn = 'mysql:dbname=%s;host=%s';

        $schema = Utils::getNotEmpty($options, Constant::CFG_SCHEMA);
        $host = Utils::getNotEmpty($options, Constant::CFG_HOST);
        $port = $options[Constant::CFG_PORT] ?? '';

        if (!empty($port)) {
            $host .= ':' . $port;
        }

        return sprintf($dsn, $schema, $host);
    }

}