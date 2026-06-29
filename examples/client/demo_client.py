import time
import datetime
from progressbar import ProgressBar
from pathlib import Path
import random
from gpmq import use_spawn, ProcessStatus, GPMQProgress
from gpmq.config import set_config_manager, get_client
from gpmq.log import get_logger
from gpconfig import GPConfigManager

example_path = Path(__file__).parent.parent
config_path = example_path / 'configs'
log_path = example_path / 'logs'


class MyProgressBar(GPMQProgress):
    def __init__(self):
        super().__init__()
        self._progbar = ProgressBar()

    def start(self):
        super().start()
        self._progbar.start(max_value=self.max_value, init=True)

    def finish(self):
        super().finish()
        self._progbar.finish()

    def update(self, value):
        super().update(value)
        self._progbar.update(self.current_value)


# ── demo functions ──────────────────────────────────────────────────────────

def demo_sync_message(client, logger):
    """发布单条同步消息，1 message，1 result"""
    logger.info("send sync message: 'load_user_profile' for user 'user_0'")
    result = client.publish("load_user_profile", {"user_name": "user_0"})
    for sub_name, pr in result.items():
        logger.info(f"message 'load_user_profile' got result from subscriber '{sub_name}', "
                    f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                    f"result: '{pr.data}'")


def demo_async_message(client, logger):
    """发布单条异步消息，1 message，1 result"""
    logger.info("send async message: 'load_user_posts' for user 'user_1'")
    handler = client.publish_async("load_user_posts", {"user_name": "user_1"})
    results = handler.wait()
    for sub_name, pr in results.items():
        logger.info(f"message 'load_user_posts' got result from subscriber '{sub_name}', "
                    f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                    f"result: '{pr.data}'")


def demo_batch_async(client, logger):
    """批量发布异步消息，12 messages，12 results"""
    result_handlers = list()
    for i in range(2, 14):
        result_handlers.append(client.publish_async("load_user_posts", {"user_name": f"user_{i}"}))
    all_results = client.wait_all(result_handlers)
    for one_result in all_results:
        for sub_name, pr in one_result.items():
            logger.info(f"message 'load_user_posts' got result from subscriber '{sub_name}', "
                        f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                        f"result: '{pr.data}'")


def demo_worker_exception(client, logger):
    """Worker 异常处理，1 message，1 result"""
    logger.info("send sync message: 'load_user_posts' for user 'test_exception_user'")
    result = client.publish("load_user_posts", {"user_name": "test_exception_user"})
    for sub_name, pr in result.items():
        logger.info(f"message 'load_user_posts' got result from subscriber '{sub_name}', "
                    f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                    f"result: '{pr.data}', \n"
                    f"error_msg: '{pr.error_message}', \n"
                    f"error_traceback: '{pr.error_traceback}'")


def demo_multiple_subscriber(client, logger):
    """多订阅者场景，2 messages，4 results"""
    result = client.publish("report_subscriber_status",
                            {"publisher": client.publisher_name})
    for sub_name, pr in result.items():
        logger.info(f"message 'report_subscriber_status' got result from subscriber '{sub_name}', "
                    f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                    f"result: '{pr.data}', \n"
                    f"error_msg: '{pr.error_message}', \n"
                    f"error_traceback: '{pr.error_traceback}'")

    result = client.publish("report_subscriber_status",
                            {"publisher": client.publisher_name, "raise_exception": "notifier"})
    for sub_name, pr in result.items():
        logger.info(f"message 'report_subscriber_status' got result from subscriber '{sub_name}', "
                    f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                    f"result: '{pr.data}', \n"
                    f"error_msg: '{pr.error_message}', \n"
                    f"error_traceback: '{pr.error_traceback}'")


def demo_sync_timeout(client, logger):
    """同步消息超时，1 message，1 result"""
    result = client.publish("notify_system_event",
                            {"user_name": "heavy_workload_user", "heavy_workload": 10},
                            timeout=5)
    for sub_name, pr in result.items():
        logger.info(f"message 'notify_system_event' got result from subscriber '{sub_name}', "
                    f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                    f"result: '{pr.data}', \n"
                    f"error_msg: '{pr.error_message}', \n"
                    f"error_traceback: '{pr.error_traceback}'")


def demo_async_timeout(client, logger):
    """异步消息超时与重试"""
    handler = client.publish_async("notify_system_event",
                            {"user_name": "heavy_workload_user", "heavy_workload": 8})
    for i in range(1, 10):
        result = handler.wait(timeout=1.0, allow_timeout_retry=True)
        count = 0
        for sub_name, pr in result.items():
            logger.info(f"message 'notify_system_event' got result from subscriber '{sub_name}', "
                        f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                        f"result: '{pr.data}', \n"
                        f"error_msg: '{pr.error_message}', \n"
                        f"error_traceback: '{pr.error_traceback}'")
            if pr.status == ProcessStatus.SUCCESS:
                count += 1
        if count == len(result):
            break


def demo_batch_with_progress(client, logger):
    """批量异步消息 + 进度条"""
    with client.async_batch(MyProgressBar()) as batch:
        for i in range(20, 30):
            batch.send_message("load_user_activities", {"user_name": f"user_{i}"})

    results = batch.get_all_result()
    for _r in results:
        for sub_name, pr in _r.items():
            logger.info(f"message 'load_user_activities' got result from subscriber '{sub_name}', "
                        f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                        f"result: '{pr.data}', \n"
                        f"error_msg: '{pr.error_message}', \n"
                        f"error_traceback: '{pr.error_traceback}'")


def demo_batch_with_progress_timeout(client, logger):
    """批量异步消息 + 进度条 + 超时"""
    with client.async_batch(MyProgressBar(), timeout=15) as batch:
        for i in range(30, 40):
            batch.send_message("load_user_activities", {"user_name": f"user_{i}"})

    results = batch.get_all_result()
    for _r in results:
        for sub_name, pr in _r.items():
            logger.info(f"message 'load_user_activities' got result from subscriber '{sub_name}', "
                        f"status: '{pr.status}', time_consumed: '{round(pr.processing_time, 4)}', "
                        f"result: '{pr.data}', \n"
                        f"error_msg: '{pr.error_message}', \n"
                        f"error_traceback: '{pr.error_traceback}'")


def demo_stress_test_heavy(client, logger):
    """压力测试：大量重负荷消息"""
    with client.async_batch(MyProgressBar()) as batch:
        for i in range(0, 400):
            batch.send_message("notify_system_event",
                            {"user_name": "stress_test_user", "heavy_workload": 2+random.random()})

    results = batch.get_all_result()
    timeout_count = 0
    success_count = 0
    failed_count = 0
    for _r in results:
        for sub_name, pr in _r.items():
            if pr.status == ProcessStatus.SUCCESS:
                success_count += 1
            elif pr.status == ProcessStatus.FAILURE:
                failed_count += 1
            elif pr.status == ProcessStatus.TIMEOUT:
                timeout_count += 1

    logger.info(f"stress_test_heavy completed with total messages: {len(results)}, "
                f"success: {success_count}, failed: {failed_count}, timeout: {timeout_count}")


def demo_stress_test_light(client, logger):
    """压力测试：大量轻负荷消息"""
    with client.async_batch(MyProgressBar()) as batch:
        for i in range(0, 1000):
            batch.send_message("notify_system_event",
                            {"user_name": "stress_test_user", "heavy_workload": random.random()})

    results = batch.get_all_result()
    timeout_count = 0
    success_count = 0
    failed_count = 0
    for _r in results:
        for sub_name, pr in _r.items():
            if pr.status == ProcessStatus.SUCCESS:
                success_count += 1
            elif pr.status == ProcessStatus.FAILURE:
                failed_count += 1
            elif pr.status == ProcessStatus.TIMEOUT:
                timeout_count += 1

    logger.info(f"stress_test_light completed with total messages: {len(results)}, "
                f"success: {success_count}, failed: {failed_count}, timeout: {timeout_count}")


def demo_stress_test_fire_and_forget(client, logger):
    """压力测试：大量轻负荷消息发送后不等待返回结果"""
    start_time = time.monotonic()
    logger.info(f"stress_test_fire_and_forget completed with total messages: 5000, "
                f"start: {time.monotonic()}, now: {datetime.datetime.now()}")
    for i in range(0, 5000):
        client.send("notify_system_event",
                    {"user_name": "stress_test_user", "heavy_workload": random.random()/50})
    end_time = time.monotonic()
    elapsed_time = end_time - start_time
    logger.info(f"stress_test_fire_and_forget completed with total messages: 5000, "
                f"end: {time.monotonic()}, elapsed: {elapsed_time}, now: {datetime.datetime.now()}")


def demo_pingpong_message_relay(client, logger):
    client.send("msg_ping", {"ttl": 10})
    logger.info(f"pingpong_message_relay started with 'msg_ping' sent to message queue")


# ── menu & main ─────────────────────────────────────────────────────────────

DEMOS = [
    ("发布单条同步消息", demo_sync_message),
    ("发布单条异步消息", demo_async_message),
    ("批量发布异步消息", demo_batch_async),
    ("Worker 异常处理", demo_worker_exception),
    ("多订阅者场景", demo_multiple_subscriber),
    ("同步消息超时", demo_sync_timeout),
    ("异步消息超时与重试", demo_async_timeout),
    ("批量异步 + 进度条", demo_batch_with_progress),
    ("批量异步 + 进度条 + 超时", demo_batch_with_progress_timeout),
    ("压力测试（大量重负荷消息等待结果）", demo_stress_test_heavy),
    ("压力测试（大量轻负荷消息等待结果）", demo_stress_test_light),
    ("压力测试（大量轻负荷消息发送后不等待结果）", demo_stress_test_fire_and_forget),
    ("消息转发测试（Subscriber内再次发送消息）", demo_pingpong_message_relay),
]


def show_menu():
    print("\n" + "=" * 50)
    print("  GPMQ Demo Client - 请选择要运行的示例")
    print("=" * 50)
    for idx, (name, _) in enumerate(DEMOS, 1):
        print(f"  {idx:>2}. {name}")
    print(f"  {'0':>2}. 退出")
    print("=" * 50)


def main():
    use_spawn()
    config = GPConfigManager("gpmq_demo", config_path)
    set_config_manager(config)
    logger = get_logger("gpmq_client")

    with get_client("gpmq.publisher.main") as client:

        logger.info("demo_client started")

        while True:
            show_menu()
            choice = input("请输入编号: ").strip()
            if choice == '0':
                print("再见！")
                break
            if not choice.isdigit() or not (1 <= int(choice) <= len(DEMOS)):
                print(f"无效输入，请输入 0-{len(DEMOS)} 之间的编号")
                continue

            idx = int(choice) - 1
            name, func = DEMOS[idx]
            print(f"\n>>> 运行示例: {name}\n")
            try:
                func(client, logger)
            except Exception as e:
                logger.error(f"示例运行出错: {e}")
            print(f"\n<<< 示例 '{name}' 运行结束\n")


if __name__ == '__main__':
    main()
