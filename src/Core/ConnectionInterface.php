<?php

namespace Moon\Core\Connection;

interface ConnectionInterface {

    /**
     * 是否已经连接数据库
     * @return bool
     */
    public function isConnected(): bool;

    /**
     * 连接数据库
     * @return bool
     */
    public function connect(): bool;

    /**
     * 执行指令
     * @return mixed
     */
    public function execCommand();

    /**
     * 返回上一次执行命令的错误信息
     * @return string
     */
    public function errorInfo(): string;

    /**
     * 返回上一次执行命令的错误码
     * @return int
     */
    public function errorCode(): string;

}