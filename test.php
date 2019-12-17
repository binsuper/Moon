<?php

require_once 'vendor/autoload.php';

use Moon\Moon;
use Moon\Core\DB\Raw;
use Moon\Core\DB\Selector;
use Moon\Driver\PDO\Mysql\Constructor;
use Moon\Core\Collection;

$subs = new Selector('class');
$subs->column(['t' => 'time[Int]']);
$subs->column(['p' => 'project[String]'])
    ->where('dd', 1);

$s = new Selector('user', 'u');

$s
    ->column('name[String], c, b(u)[Int]')
//    ->column(Moon::raw('count(:abc)', [':abc' => 1]), 'cnt')
//    ->columnSubquery($subs, 'cls')
//    ->column([
//        'info' => ['class', 'job', 'all' => []]
//    ])
    ->columnInt('sex(ge)')
    ->columnInt('tt[Json]')
//    ->column(Raw::column('user.*'))
;

//$s
//    ->where('a', 'b')
//    ->whereBetween('u.b', Moon::raw('u.b + 1 and u.b + 10'))
//    ->whereNotExists($subs)
//;
//
//$s->group(['name', 'id'])
//    ->having(function (Selector $selector) {
//        $selector->where('a', 1);
//    })
//    ->having(Moon::raw('count(1) > 1'))
//    ->orderAsc('id')
//    ->orderAsc('name')
;

//$s->value('name', '赵四')
//    ->value('class', 1);
//
//$s->multiValue(['name', 'class'], [
//    ['张三', 1],
//    ['李四', 1],
//]);

$c = new Constructor(['prefix' => ''], $s);

print_r($c->selectContext());
//print_r($c->updateContext());
//print_r($s->getColumnType());


$col = new Collection([ ['name' => 123, 'u' => '16', 'tt' => json_encode([1])] ]);
$col->render($s->getColumnType());

//
//list($a,$b) = [1,2];
//var_dump($a,$b);