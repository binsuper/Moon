<?php

namespace Moon\Driver\PDO;

use Moon\Core\Constant;
use Moon\Core\Error\ConnectFailedException;
use Moon\Core\Error\InvalidArgumentException;
use Moon\Core\Connection\ConnectionInterface;
use Moon\Helper\Utils;
use \PDO;
use \PDOException;

class Connection implements ConnectionInterface {

    /**
     * 驱动服务的名称
     * 例如 mysql , mssql 等
     * @var string
     */
    private string $__dsn_name;

    /**
     * 驱动服务使用的用户名
     * @var string
     */
    private string $__user;

    /**
     * 驱动服务使用的密码
     * @var string
     */
    private string $__password;

    /**
     * 驱动服务使用的其他配置信息
     * @var array
     */
    private array $__options;

    /**
     * 驱动句柄
     * @var PDO
     */
    private PDO $__handler;

    /**
     * 上一次执行命令的错误信息
     * @var string
     */
    private string $__error_info;

    /**
     * 上一次执行命令的错误码
     * @var int
     */
    private int $__error_code;

    public function __construct(array $options) {
        $this->__dsn_name = strtolower(Utils::getNotEmpty($options, Constant::CFG_DSN));
        $this->__user = $options[Constant::CFG_USER] ?? '';
        $this->__password = $options[Constant::CFG_PASS] ?? '';
        $this->__options = $options;

        try {
            Utils::getNotEmpty(PDO::getAvailableDrivers(), $this->__dsn_name);
        } catch (InvalidArgumentException $ex) {
            throw new PDOException('当前环境不支持 ' . $this->__dsn_name . ' 驱动服务');
        }

        // 连接数据库
        $this->connect();
    }

    /**
     * @inheritDoc
     */
    public function isConnected(): bool {

        if (is_null($this->__handler)) {
            return false;
        }

        try {
            $this->__handler->getAttribute(PDO::ATTR_SERVER_VERSION);
        } catch (PDOException $ex) {
            if (false !== strpos($ex->getMessage(), 'MySQL server has gone away')) {
                return false;
            }
        }

        return true;
    }

    /**
     * @inheritDoc
     */
    public function connect(): bool {

        $dsn = DSN::get($this->__dsn_name, $this->__options);

        try {
            $handler = new  PDO ($dsn, $this->__user, $this->__password);
        } catch (PDOException $ex) {
            throw new ConnectFailedException($ex->getMessage(), $ex->getCode(), $ex);
        }

        $this->__handler = $handler;

        return true;
    }

    /**
     * @inheritDoc
     */
    public function execCommand() {
        // TODO: Implement execCommand() method.
    }

    /**
     * @inheritDoc
     */
    public function errorInfo(): string {
        return implode('-', $this->__handler->errorInfo());
    }

    /**
     * @inheritDoc
     */
    public function errorCode(): string {
        return $this->__handler->errorCode();
    }
}