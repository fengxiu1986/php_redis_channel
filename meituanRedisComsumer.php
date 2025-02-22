<?php

/**
 * 美团供应商推送的数据消费（用新的redis集群）
 * 1、用redis原生队列，不在使用之前的 horizon 队列（出现性能问题）
 */

namespace App\Console\Commands\Meituan;

use App\Business\MeituantmcSupplierPushData;
use App\Console\Commands\HotelQueueCrontab;
use App\Helpers\CacheKey;
use App\Models\BaseHotelSource;
use App\Services\Hotels\HotelSupplierRedisQueueService;
use Illuminate\Console\Command;
use App\Business\MeituanSupplierPushData;

class RedisQueueConsume extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'redisQueue:meituan-push-consume {consumeType=0}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = '美团供应商推送的数据消费(价格和房态)';


    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $consumeType = $this->argument('consumeType');
        switch ($consumeType) {
            case 'price':
                $this->priceConsume();
                break;
            case 'roomStatus':
                $this->roomStatusConsume();
                break;
            case 'hotel':
                $this->hotelConsume();
                break;
            case 'roomPriceStatus':
                $this->roomPriceStatusConsume();
                break;
            case 'hotelInfo':
                $this->hotelInfoConsume();
                break;
            default:
                echo '['.date('Y-m-d H:i:s').'] no found plan.'.PHP_EOL;
                break;
        }

        return true;
    }


    /**
     * 推送价格消费方法
     */
    public function priceConsume() {
        //初始配置
        $configArr = [
            'redisDbList' => CacheKey::MEITUAN_PRICE_LIST,//队列名称
            'doPlanNum' => in_array(intval(date('H')), [0,1,12,17,18,19,20]) ?  10: 8,//最多两个进程运行
            'doErrorContinue' => 0,//消费出错进行重试:1重试，0不重试
            'errNum' => 0,//失败重试的次数
            'logName' => 'redis_queue_meituan_price_consume',//保存的日志名称18
            'batchPop' => 1,//批量获取数量
        ];
        //初始化crontab模式执行的基类
        $crontabServiceBase = new HotelQueueCrontab($configArr);
        //执行注册的方法(batchPop = 1时使用rpChange、否则使用rpChangeMultiDo)
        if ($configArr['batchPop'] == 1) {
            $crontabServiceBase->multiExecuteWorker(MeituanSupplierPushData::class, 'rpChange');
        } else {
            $crontabServiceBase->multiExecuteWorker(MeituanSupplierPushData::class, 'rpChangeMultiDo');
        }
    }

    /**
     * 房态变更推送
     */
    public function roomStatusConsume() {
        //初始配置
        $configArr = [
            'redisDbList' => CacheKey::MEITUAN_ROOM_STATUS_LIST,//队列名称
            'doPlanNum' => in_array(intval(date('H')), [0,1,12,17,18,19,20]) ? 10 : 8,//最多两个进程运行
            'doErrorContinue' => 0,//消费出错进行重试:1重试，0不重试
            'errNum' => 0,//失败重试的次数
            'logName' => 'redis_queue_meituan_roomstatus_consume',//保存的日志名称
            'batchPop' => 1,//批量获取数量
            'dynamicPlanNum'=>true,//是否开启高峰期进程数动态调节(高峰期进程不够用会动态增加)
        ];
        //初始化crontab模式执行的基类
        $crontabServiceBase = new HotelQueueCrontab($configArr);
        //执行注册的方法(batchPop = 1时使用rsChange、否则使用rsChangeMultiDo)
        if ($configArr['batchPop'] == 1) {
            $crontabServiceBase->multiExecuteWorker(MeituanSupplierPushData::class, 'rsChange');
        } else {
            $crontabServiceBase->multiExecuteWorker(MeituanSupplierPushData::class, 'rsChangeMultiDo');
        }
    }

    /**
     * 增量酒店推送
     */
    public function hotelConsume() {
        //初始配置
        $configArr = [
            'redisDbList' => CacheKey::MEITUAN_HOTEL_LIST,//队列名称
            'doPlanNum' => 2,//最多两个进程运行
            'doErrorContinue' => 0,//消费出错进行重试:1重试，0不重试
            'errNum' => 0,//失败重试的次数
            'logName' => 'redis_queue_meituan_hotel_consume',//保存的日志名称
            'batchPop' => 1,//批量获取数量
            'dynamicPlanNum'=>true,//是否开启高峰期进程数动态调节(高峰期进程不够用会动态增加)
        ];
        //初始化crontab模式执行的基类
        $crontabServiceBase = new HotelQueueCrontab($configArr);
        //执行注册的方法
        $crontabServiceBase->multiExecuteWorker(MeituanSupplierPushData::class, 'hotelChange');
    }

    /**
     * 增量房型、价格、房态
     */
    public function roomPriceStatusConsume()
    {
        //初始配置
        $configArr = [
            'redisDbList' => CacheKey::MEITUAN_PRODUCE_LIST,// 队列名称
            'doPlanNum' => in_array(intval(date('H')), [0,1,12,17,18,19,20]) ? 10 : 8,//最多两个进程运行
            'doErrorContinue' => 0,//消费出错进行重试:1重试，0不重试
            'errNum' => 0,//失败重试的次数
            'logName' => 'redis_queue_meituan_room_price_status_consume',//保存的日志名称
            'batchPop' => 30,//批量获取数量
            'dynamicPlanNum'=>true,//是否开启高峰期进程数动态调节(高峰期进程不够用会动态增加)
        ];
        //初始化crontab模式执行的基类
        $crontabServiceBase = new HotelQueueCrontab($configArr);
        //执行注册的方法(batchPop = 1时使用rsChange、否则使用rsChangeMultiDo)
        if ($configArr['batchPop'] == 1) {
            $crontabServiceBase->multiExecuteWorker(MeituantmcSupplierPushData::class, 'productChange', ['source_from' => BaseHotelSource::SOURCE_MEITUAN]);
        } else {
            $crontabServiceBase->multiExecuteWorker(MeituantmcSupplierPushData::class, 'productChangeMultiDo', ['source_from' => BaseHotelSource::SOURCE_MEITUAN]);
        }
    }


    /**
     * 增量酒店推送
     */
    public function hotelInfoConsume() {
        //初始配置
        $configArr = [
            'redisDbList' => CacheKey::MEITUAN_HOTEL_INFO_LIST,//队列名称
            'doPlanNum' => 2,//最多两个进程运行
            'doErrorContinue' => 0,//消费出错进行重试:1重试，0不重试
            'errNum' => 0,//失败重试的次数
            'logName' => 'redis_queue_meituan_hotel_consume',//保存的日志名称
            'batchPop' => 1,//批量获取数量
        ];
        //初始化crontab模式执行的基类
        $crontabServiceBase = new HotelQueueCrontab($configArr);
        //执行注册的方法
        $crontabServiceBase->multiExecuteWorker(MeituantmcSupplierPushData::class, 'hotelInfoChange', ['source_from' => BaseHotelSource::SOURCE_MEITUAN]);
    }
}
