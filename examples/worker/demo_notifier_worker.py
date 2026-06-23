from pathlib import Path
import time
from gpmq import use_spawn
from gpmq.config import set_config_manager, get_subscriber
from gpconfig import GPConfigManager

example_path = Path(__file__).parent.parent
config_path = example_path / 'configs'
log_path = example_path / 'logs'


def main():
    use_spawn()
    config = GPConfigManager("gpmq_demo", config_path)
    set_config_manager(config)

    sub_notifier = get_subscriber("gpmq.subscribers.notifier")

    sub_notifier.run_forever()


if __name__ == '__main__':
    main()
