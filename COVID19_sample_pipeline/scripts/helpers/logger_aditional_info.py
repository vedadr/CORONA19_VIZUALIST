from loguru import logger


def show_step_info(func):
    def deco(path):
        logger.info(f' Running {func.__name__}! '.center(40, '🐍'))
        func(path)

    return deco
