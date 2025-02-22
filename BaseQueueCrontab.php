<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Carbon;
use ReflectionClass;
use Throwable;


/**
 * redis queue crontab 调度计划任务的基类，模拟实现php长驻守进程
 * 增加了执行的php进程限制，防止出现一个任务出现大量php进程执行
 * 增加了执行的php进程时间限制，1800S后自动退出，防止php内存溢出异常
 *
 * @author  yutu
 * @date    2021-12-09
 */

class HotelQueueCrontab
{
    /**
     * 入库队列
     */
    private $_redisDbList = '';
    /**
     * 入库失败队列
     */
    private $_redisDbFailList = '';
    /**
     *允许运行任务的计数器key
     */
    private $_redisDbListPlanNumKey = '';
    /**
     * 计划任务执行多久后，自己退出，防止内存泄露
     */
    private $_doExpireTime = 86400;

    /**
     * 最多允许多少个任务同时执行
     */
    private $_doPlanNum = 2;

    /**
     * 一个记录更新最大失败次数
     */
    private $_errNum = 3;

    /**
     * 失败超时的时间
     */
    private $_errExpire = 10;

    /**
     * 写入阿里云的日志topic
     */
    private $_logTopic = 'crontab_plan_list';

    /**
     * redis实例
     */
    private $_redis = null;

    /**
     * 当前进程运行的标识id
     */
    private $_planDoTime = 0;

    /**
     * 任务运行的进程是否已经启动，用来执行完减少进程数量
     */
    private $_isIncrPlanNum = 0;

    /**
     * 是否开启数据消费错误重试机制
     *
     */
    private $_doErrorContinue = 0;

    /**
     * 是否要在linux终端输出日志
     */
    private $_outLogToScreen = 1;

    /**
     * 注册执行的应用类单例模式
     */
    private $_doInterion = [];

    /**
     * 保存从队列弹出的数据
     */
    private $_redisData = [];

    /**
     * 执行失败是否写入redis队列，默认是写入
     * CrontabServiceBase constructor.
     * @param array $configArr
     */
    private $_failWriteToRedis = false;

    /**
     * 记录计划任务执行成功次数
     *
     */
    private $_planSuccessDoNum = 0;

    /**
     * 记录计划任务执行失败次数
     *
     */
    private $_planFailDoNum = 0;

    /**
     * 保存执行日志的名称
     */
    private $_logName = 'HotelOtaQueue';

    /**
     * 记录当前的进程
     */
    private $_worker = 0;

    /**
     * 是否批量从redis队列获取
     */
    private $_batchPop = 0;

    /**
     * 多进程是是否能够访问redis list的数据锁
     */
    private $_workerGetPageLockRedisKey = '';

    /**
     * 标识是否已经删除了多进程是否能够访问redis list 的数据锁
     */
    private $_workerGetPageLock = 0;

    /**
     * 标识进程执行的唯一标识
     * 毫秒时间戳
     */
    private $_planCode = '';

    /**
     * 当队列数量堆积严重的时候，是否开启动态调节执行进程数量设置
     * 1、高峰期增加，2、低峰期适当减少进程数（减少系统资源开销）
     */
    private $_dynamic_plan_num = false;


    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct(array $configArr = [])
    {
        //初始配置
        $this->_setConfig($configArr);
    }


    /**
     * 开启多线程执行
     */
    public function multiExecuteWorker(string $className = '', string $methodName = '', array $paramArr = [], string $redisName = 'hotel_ota_cluster') {
        //判断一下参数是否合法
        if (empty($className) || empty($methodName) || empty($redisName)) {
            echo '['.date('Y-m-d H:i:s').']'.$this->_redisDbList." Fork worker stop,param error".PHP_EOL;
            return false;
        }

        $now = time();
        $pids = [];

        //设置redis实例
        //$this->_redis = Redis::connection($redisName);

        declare(ticks = 1);
        pcntl_signal(SIGCHLD, SIG_IGN);

        for ($i = 0; $i < $this->_doPlanNum; $i++) {
            $pid = pcntl_fork();
            if ($pid == -1) {
                $this->error('Fork Failed: ' . $i . '-' . $now);
            } elseif ($pid) {
                $pids[] = $pid;
                pcntl_wait($status,WNOHANG);
            } else {
                try {
                    //防止并发同时启动进程，随机启动
                    usleep(rand(10000, 8000000));
                    echo '['.date('Y-m-d H:i:s').']'.$this->_redisDbList." Fork worker-{$i} success".PHP_EOL;
                    $this->doTask($className, $methodName, $paramArr, $i, $redisName);
                } catch (\Exception $e) {
                    $this->_saveLog($this->_redisDbList."pcntl_fork worker-{$i} exception:".getFullException($e));
                }
                exit();
            }
        }

        echo '['.date('Y-m-d H:i:s').']'.$this->_planCode .' plan executeWorker done, has '.$this->_doPlanNum.' woker.'.PHP_EOL;
        return true;
    }


    /**
     * 执行入口
     * @param   object  $redis       redis对象
     * @param   string  $className   注册执行的类名
     * @param   string  $methodName  注册执行的方法
     * @param   array   $paramArr    传入的参数
     * @return bool
     */
    public function doTask(string $className = '', string $methodName = '', array $paramArr = [], int $i=0, string $redisName = '')
    {
        try {
            //判断参数是否正确
            if (empty($className) || empty($methodName) || empty($redisName)) {
                return false;
            }

            //重置一下数据库连接，因为forck进程，使用单例数据库连接，导致数据库异常报错，掉坑！！
            DB::purge('mongodb');
            DB::reconnect('mongodb');

            //设置redis实例
            $this->_redis = Redis::connection($redisName);

            //判断redis队列名称
            if (empty($this->_redisDbList)) {
                return false;
            }

            set_time_limit(0);   // 设置脚本最大执行时间 为0 永不过期

            //标识当前的worker
            $this->_worker = $i;

            //开始执行任务
            $this->_setPlanDoTime();
            $str = "plan doing...";
            //记录日志
            $this->_saveLog($str, 1);

            //判断当前运行的计划任务进程是否超过退出
            if ($this->_checkPlanNum()) {
                return true;
            }

            $this->_isIncrPlanNum = 1;//标识进程数已经增加1

            //不停轮询redis队列，将数据消费掉
            while(1) {
                //判断执行是否超时，立刻退出计划任务，防止php出现内存溢出异常
                if ($this->_checkDoTime()) {
                    $str = 'plan Overtime Exit';
                    //记录日志
                    $this->_saveLog($str, 1, 'error');
                    break;
                }

                //获取队列数据进行业务处理,左出。（入队列就要右进rpush）,同时也能兼容以后批量获取(lrange/ltrim 命令)
                $dbListStr = $this->_batchPop > 1 ? $this->_multiPopDataBlock($this->_batchPop) : $this->_redis->lpop($this->_redisDbList);
                if ($dbListStr === false || empty($dbListStr)) {
                    //提高计划任务执行的效率，如果队列为空，等待50S后在退出，不然要等1分钟后才执行
                    if (time() - $this->_planDoTime < 57) {
                        usleep(10000);
                        continue;
                    }

                    $str = 'Plan redis list empty';
                    //写日志
                    $this->_saveLog($str, 1);
                    break;
                }

                //设置计划任务要消费的数据
                if (!$this->_setRediData($dbListStr)) {
                    continue;
                }

                //判断数据消费错误的频率，是否再次执行
                if (!$this->_checkDoErrorContinue()) {
                    continue;
                }

                //执行注册消费队列方法，false失败，其他成功
                $result = $this->_doPlan($className, $methodName, $paramArr);

                //当方法返回false为失败，-1的时候要立刻停止任务退出，true 都是成功！！！
                if ($result !== false) {
                    $this->_saveDoSuccess($result);
                    $this->_planSuccessDoNum ++;
                }
                else {
                    $this->_saveDoError($result);
                    $this->_planFailDoNum ++;
                }
                usleep(rand(3000, 10000));
            }

        }
        catch (\Throwable $e) {
            $this->_saveDoException($e);
        }
        finally {
            $this->_saveDoEnd();
        }

        return true;
    }

    /**
     * 初始化全局配置文件
     */
    private function _setConfig(array $configArr = []) {
        if (empty($configArr)) {
            return false;
        }
        //设置需要从那个队列消费
        $this->_redisDbList             =  !empty($configArr['redisDbList']) ? $configArr['redisDbList'] : $this->_redisDbList;
        //消费失败将数据存放到失败队列
        $this->_redisDbFailList         =  $this->_redisDbList . ':fail';
        //允许执行的进程数量
        $this->_redisDbListPlanNumKey   =  'crontab_' . $this->_redisDbList . '_planNum';
        //进程运行的最长时间
        $this->_doExpireTime            =  isset($configArr['doExpireTime']) && $configArr['doExpireTime'] > 0 ? (int)$configArr['doExpireTime'] : $this->_doExpireTime;
        //运行同时运行的进程数量
        $this->_doPlanNum               =  isset($configArr['doPlanNum']) && $configArr['doPlanNum'] > 0 ? (int)$configArr['doPlanNum'] : $this->_doPlanNum;
        //错误重试间隔的时间
        $this->_errExpire               =  isset($configArr['errExpire']) && $configArr['errExpire'] > 0 ? (int)$configArr['errExpire'] : $this->_errExpire;
        //错误重试的次数
        $this->_errNum                  =  isset($configArr['errNum']) && $configArr['errNum'] > 0 ? (int)$configArr['errNum'] : $this->_errNum;
        //日志存放到阿里云topic设置
        $this->_logTopic                =  !empty($configArr['logTopic']) ? $configArr['logTopic'] : $this->_logTopic;
        //消费错误是否重试：1重试，0不重试
        $this->_doErrorContinue         =  isset($configArr['doErrorContinue']) && $configArr['doErrorContinue'] == 0 ? 0 : 1;
        //是否将日志输出到linux 屏幕用于nohup捕获
        $this->_outLogToScreen          =  isset($configArr['outLogToScreen']) && $configArr['outLogToScreen'] == 0 ? 0 : 1;
        //是否将执行失败的信息写会redis 队列
        $this->_failWriteToRedis        =  isset($configArr['failWriteToRedis']) && $configArr['failWriteToRedis'] == true ? true : false;
        //是否要设置计划任务超时时间
        $this->_checkPlanDoTime         =  isset($configArr['checkPlanDoTime']) && $configArr['checkPlanDoTime'] == false ? false : true;
        //日志保存的文件名
        $this->_logName                 =  !empty($configArr['logName']) ? $configArr['logName'] : $this->_logName;
        //批量从redis pop出来
        $this->_batchPop                =  isset($configArr['batchPop']) && $configArr['batchPop'] > 1 ? (int)$configArr['batchPop'] : 1;
        //计划任务的唯一标识
        $this->_planCode = microtime(true);
        //多进程批量获取redis队列数据锁
        $this->_workerGetPageLockRedisKey = 'crontab_' . $this->_redisDbList . '_workBatchPopLock';
        //是否开启进程数量动态调节功能
        $this->_dynamic_plan_num        =  isset($configArr['dynamicPlanNum']) && $configArr['dynamicPlanNum'] === true ? true : false;

        return true;
    }

    /**
     * 阿里云的日志输出
     * @param  string  $message
     * @return bool
     */
    private function _saveLog(string $message = '', $outLog = 1, string $type = 'success', bool $saveLog = true) {
        if (empty($message)) {
            return false;
        }

        if ($this->_outLogToScreen == 1 || $outLog == 1) {
            echo '['.date('Y-m-d H:i:s').']'."[" . $this->_planCode ."][" . $this->_redisDbList ."][worker-".$this->_worker."]" . $message. PHP_EOL;
        }

        //不保存日志
        if (!$saveLog) {
            return true;
        }

        //拼接写日志数组
        $messageArr['planCode'] = $this->_planCode;
        $messageArr['redisListName'] = $this->_redisDbList;
        $messageArr['workerName'] = $this->_worker;
        $messageArr['message'] = $message;

        //开始记录日志，
        $logArr = config("logging.channels.{$this->_logName}");
        if (empty($logArr) || empty($logArr[$this->_logName])) {
            //自己设置默认日志
            $this->_createLog($messageArr, $type);
        } else {
            Log::channel($this->_logName)->info($this->_redisDbList.'_'.$type, $messageArr);
        }

        return true;
    }

    /**
     * 动态的创建log目录和文件
     */
    private function _createLog(array $message = [], string $type = 'success') {
        if (empty($message)) {
            return false;
        }

        $view_log = new Logger($this->_redisDbList);
        // 写入
        $view_log->pushHandler(new StreamHandler(storage_path('logs/'.$this->_logName.'-'.date('Y-m-d').'.log'), Logger::INFO));
        $view_log->info($this->_redisDbList . '_'.$type, $message);

        return true;
    }

    /**
     * 判断计划任务执行时间是否超过
     */
    private function _checkDoTime() {
        if ($this->_checkPlanDoTime == true && (time() - $this->_planDoTime > $this->_doExpireTime)) {
            return true;
        }
        return false;
    }

    /**
     * 判断当前执行的计划任务数量是否超过设置
     * （cron 1分钟跑一次，但是每次跑是在循环里面，需要30分钟结束，30分钟理论上会启动30个进程，限制进程数量，保证服务器资源）
     */
    private function _checkPlanNum() {
        $doNum = $this->_redis->get($this->_redisDbListPlanNumKey);
        $doNum = !empty($doNum) && is_numeric($doNum) ? intval($doNum) : 0;
        if (!empty($doNum)  && $doNum < 0 ) {
            $this->_redis->del($this->_redisDbListPlanNumKey);
        }

        //判断是否开启进程数量动态调节功能
        $dynamicPlanNumArr = $this->_dynamicPlanNum();
        if (intval($doNum) >= $this->_doPlanNum + $dynamicPlanNumArr['planNum']) {
            $str = 'plan doing num over:'.($this->_doPlanNum + $dynamicPlanNumArr['planNum']);
            $str .= ';set doPlannum:'.$this->_doPlanNum.',dynamicPlanNum:'.$dynamicPlanNumArr['planNum'];
            //记录日志
            $this->_saveLog($str);
            return true;
        }
        //设置计划任务数量key过期时间，如果是限定1个进程，是不会有过期时间。
        $this->_redis->incr($this->_redisDbListPlanNumKey);
        //if ($this->_doPlanNum > 1) {
        $this->_redis->expire($this->_redisDbListPlanNumKey, $this->_doExpireTime + 10);
        //}

        return false;
    }

    /**
     * 执行指定的类的方法
     * @param    string    $class   类名
     * @param   string     $method  方法
     * @param   array      $param   消费的数据
     * @return bool
     */
    private function _doPlan(string $class = '', string $method = '', array $paramArr = []) {
        if (empty($class) || empty($method)) {
            return false;
        }

        try {
            if (!empty($this->_doInterion[$class])) {
                $instance = $this->_doInterion[$class];
                //echo 'set class111'.PHP_EOL;
            }
            else {
                $registerClass = new ReflectionClass($class);  // 将类名
                $instance = $registerClass->newInstance();  // 创建类的实例
                $this->_doInterion[$class] = $instance;
                //echo 'set class222'.PHP_EOL;
            }

            if (!is_object($instance)) {
                $str = "plan do error,register $class is not object";
                $this->_saveLog($str, 1, 'error');
                return false;
            }

            if (!is_callable(array($instance, $method))) {
                $str = "plan do register class method no allow do:[$class][$method]";
                $this->_saveLog($str, 1, 'error');
                return false;
            }

            //执行注册消费方法
            $data = $this->_redisData;
            if (isset($data['err_time'])) {
                unset($data['err_time']);
            }
            if (isset($data['err_num'])) {
                unset($data['err_num']);
            }
            if (isset($data['add_redis_list_time'])) {
                unset($data['add_redis_list_time']);
            }
            //return $methodObject->invokeArgs($instance, array($data, $paramArr));
            return $instance->$method($data, $paramArr);

        }
        catch (\Throwable $e) {
            $str = 'plan do register class exception:'.json_encode($this->_getExceptionMessgae($e));
            $this->_saveLog($str, 1, 'error');
            return false;
        }
    }

    /**
     * 设置redis实例
     * @param   object  $redis   redis实例
     */
    private function _setRedis(&$redis = null) {
        if (is_object($redis)) {
            $this->_redis = $redis;
        }
    }

    /**
     * 设置执行的开始时间
     */
    private function _setPlanDoTime() {
        $this->_planDoTime = time();
    }

    /**
     * 判断数据消费的错误次数,
     * 判断数据消费执行错误的时间间隔
     * 进行重试，保证数据可以被消费
     */
    private function _checkDoErrorContinue() {
        if ($this->_doErrorContinue == 1 && !empty($this->_redisData)) {
            //入库错误次数限制
            if (!empty($this->_redisData['err_num']) && $this->_redisData['err_num'] >= $this->_errNum) {
                $this->_redis->rpush($this->_redisDbFailList, json_encode($this->_redisData));
                $str = 'Plan fail over max num:'.$this->_ArrToJsonString($this->_redisData);
                $this->_saveLog($str);
                return false;
            }
            //入库错误时间限制
            if (!empty($this->_redisData['err_time']) && time() - $this->_redisData['err_time'] <= $this->_errExpire) {
                $this->_redis->rpush($this->_redisDbList, json_encode($this->_redisData));
                $str = "Plan fail err_time in {$this->_errExpire}S:".$this->_ArrToJsonString($this->_redisData);
                //$this->_saveLog($str);
                return false;
            }
        }

        return true;
    }

    /**
     * 数据消费失败后，重新放回去队列
     * 记录数据消费的错误次数和时间
     */
    private function _saveDoError() {
        if ($this->_doErrorContinue == 1 && !empty($this->_redisData)) {
            if (isset($this->_redisData['err_num'])) {
                $this->_redisData['err_num'] ++;
            }
            else {
                $this->_redisData['err_time'] = time();
                $this->_redisData['err_num'] = 1;
            }

            //把错误的信息重新打入该队列，继续消费
            if ($this->_failWriteToRedis == true) {
                $this->_redis->rpush($this->_redisDbList, json_encode($this->_redisData));
            }
        }
        else {
            //把错误的信息重新打入另外失败队列，后续在处理
            if ($this->_failWriteToRedis == true) {
                $this->_redis->rpush($this->_redisDbFailList, json_encode($this->_redisData));
            }
        }

        $str = 'plan fail done: ' . $this->_ArrToJsonString($this->_redisData);
        //写日志
        $this->_saveLog($str, 1, 'error');
    }

    /**
     * 数据消费成功后处理
     */
    private function _saveDoSuccess($result) {
        $resultMessage = $result === -1 ? 'parma error' : 'success done';
        $str = "plan {$resultMessage}.";
        $str .= "json_data:".json_encode($this->_redisData);
        //线上成功的日志就不输出，太多了影响性能
        $this->_saveLog($str, 1, 'success', false);
    }

    /**
     * 计划任务执行异常处理
     * @param  object  $e   异常的$e
     *
     */
    private function _saveDoException(&$e = null) {
        $json_data = '';
        if ($this->_failWriteToRedis == true && !empty($this->_redisData)) {
            $this->_redis->rpush($this->_redisDbFailList, json_encode($this->_redisData));
            $json_data =  $this->_ArrToJsonString($this->_redisData);
        }
        $str = 'Plan exception :' . $this->_getExceptionMessgae($e) . ',json_data:' .$json_data;
        //写阿里云日志
        $this->_saveLog($str, 1, 'exception');

        if ($this->_isIncrPlanNum == 1) {
            $this->_redis->decr($this->_redisDbListPlanNumKey);
            $this->_isIncrPlanNum = 0;
        }

        //判断进程是不是都已经退出了,自动清理启用批量获取redis 队列相关key
        if ($this->_batchPop > 1 && $this->_workerGetPageLock == 1) {
            $this->_redis->del($this->_workerGetPageLockRedisKey);
        }
    }

    /**
     * 计划任务执行结束处理
     */
    private function _saveDoEnd() {
        //进程减少
        $str = "plan end,success_num:{$this->_planSuccessDoNum},error_num:{$this->_planFailDoNum}.";
        //写阿里云日志
        $this->_saveLog($str, 1);
        if ($this->_isIncrPlanNum == 1) {
            $this->_redis->decr($this->_redisDbListPlanNumKey);
            $this->_isIncrPlanNum = 0;
        }
        //判断进程是不是都已经退出了,自动清理启用批量获取redis 队列相关key
        if ($this->_batchPop > 1 && $this->_workerGetPageLock == 1) {
            $this->_redis->del($this->_workerGetPageLockRedisKey);
        }
    }

    /**
     * 设置while循环要执行的数据
     * @param   $dbListStr  从redis队列弹出数据 (批量弹出是数组，单个是字符串)
     * @return  bool
     */
    private function _setRediData($dbListStr) {
        try {
            $this->_redisData = [];
            if ($this->_batchPop <= 1) {
                $this->_redisData = !empty($dbListStr) ? json_decode($dbListStr, true) : [];
            } else {
                //批量是数组需要重新处理
                foreach ($dbListStr as $key=>$val) {
                    $tempArr = json_decode($val, true);
                    if (!empty($tempArr) && is_array($tempArr)) {
                        $this->_redisData[] = $tempArr;
                    }
                }
            }

            //队列弹出数据为空处理
            if (empty($this->_redisData) || !is_array($this->_redisData)) {
                $str = 'Plan redis list data type error:' .(is_array($dbListStr) ? json_encode($dbListStr) : $dbListStr);
                $this->_saveLog($str,1, 'error');
                return false;
            }
        }
        catch (\Throwable $e) {
            $str = 'Plan redis list data type Exception:' .$this->_getExceptionMessgae($e).';data:'.$dbListStr;
            $this->_saveLog($str,1, 'exception');
            return false;
        }

        return true;
    }

    /**
     * 获取异常的信息
     * @param  object  $e  Exception中的$e
     */
    private function _getExceptionMessgae($e) {
        $errMessage = [
            'code'    => $e->getCode(),
            'message'     => $e->getMessage(),
            'file'    => $e->getFile(),
            'line'    => $e->getLine(),
        ];

        return json_encode($errMessage);
    }

    /**
     * 将数组转为json字符串
     * @param $data  array
     * @return string
     */
    private function _ArrToJsonString(array $data = []) {
        if (empty($data)) {
            return '';
        }

        return json_encode($data);
    }

    /**
     * 禁止clone类
     */
    private function __clone() {

    }

    /**
     * 批量从redis list获取数据，多进程阻塞
     */
    private function _multiPopDataBlock(int $limit = 10) {
        $data = [];
        $this->_workerGetPageLock = 1;
        while(1) {
            if ($this->_redis->set($this->_workerGetPageLockRedisKey, date('YmdHis'),'ex', 60,'nx')) {
                $data = $this->_redis->lrange($this->_redisDbList, 0, $limit - 1);
                if (!empty($data)) {
                    //批量删除已经取出的数据
                    $this->_redis->ltrim($this->_redisDbList, count($data), -1);
                    //echo $this->_worker.':ltrim '.count($data).' -1'.PHP_EOL;
                }
                $this->_redis->del($this->_workerGetPageLockRedisKey);
                $this->_workerGetPageLock = 0;
                break;
            } else {
                //echo $this->_worker.':wait get page_lock ...'.PHP_EOL;
                usleep(50000);
                continue;
            }
        }

        return $data;
    }

    /**
     * 动态调节进程数量
     * 根据当前队列数量，来适当增加或者减少进程数量
     * @return  int   返回允许运行的进程数量
     */
    private function _dynamicPlanNum() {
        $dynamicResult = [
            'type'    => 0,//0是不需要动态调节，1是需要增加(繁忙)，-1是需要减少(空闲)
            'planNum' => 0,//动态调节的进程数量
        ];
        if (!$this->_dynamic_plan_num) {
            //不干预原来的设置进程数量
            return $dynamicResult;
        }

        //先获取当前执行队列的总数量，用来计算当前运行进程数量需要数量
        $listLen = $this->_redis->llen($this->_redisDbList);

        if ($listLen == 0) {
            //如果队列为空，就强制减少启动进程数量，减少空闲时候系统的压力，有些初始化配置10多个进程
            $this->_doPlanNum = $this->_doPlanNum > 3 ? 3 : $this->_doPlanNum;
        } elseif ($listLen > 0 && $listLen < 300) {
            $dynamicResult = [
                'type'    => 0,
                'planNum' => 0,
            ];
        } elseif ($listLen >= 300 && $listLen < 1500) {
            $dynamicResult = [
                'type'    => 1,
                'planNum' => 2,
            ];
        }elseif ($listLen >= 1500 && $listLen < 3000) {
            $dynamicResult = [
                'type'    => 2,
                'planNum' => 4,
            ];
        }elseif ($listLen >= 3000 && $listLen < 5000) {
            $dynamicResult = [
                'type'    => 3,
                'planNum' => 6,
            ];
        } elseif ($listLen >= 5000 && $listLen < 10000) {
            $dynamicResult = [
                'type'    => 4,
                'planNum' => 8,
            ];
        } else{
            $dynamicResult = [
                'type'    => 5,
                'planNum' => 10,
            ];
        }

        //输出日志，可以方便观察动态调节进程是否有生效
        if ($dynamicResult['type'] != 0) {
            $str = 'dynamic plan num is doing:' .json_encode($dynamicResult);
            $this->_saveLog($str,1, 'success');
        }

        return $dynamicResult;
    }


    /**
     * 类销毁触发
     */
    public function __destruct()
    {
        //在次清除计数器，防止进程锁异常，主要针对单个的进程
        if ($this->_isIncrPlanNum == 1) {
            //echo $this->_redisDbListPlanNumKey . ' decr' . PHP_EOL;
            $this->_redis->decr($this->_redisDbListPlanNumKey);
        }
        //判断进程是不是都已经退出了,自动清理启用批量获取redis 队列相关key
        if ($this->_batchPop > 1 && $this->_workerGetPageLock == 1) {
            $this->_redis->del($this->_workerGetPageLockRedisKey);
        }
    }

}
