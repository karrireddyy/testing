import logging, inspect

def get_logger(name):
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
	ch.setFormatter(formatter)
	logger.addHandler(ch)

	return logger

def debug(logger, message):
	func = inspect.currentframe().f_back.f_code
	logger.debug("{%s:%s} - %s" % (
        func.co_filename, 
        func.co_firstlineno,
        message
    ))

def info(logger, message):
	func = inspect.currentframe().f_back.f_code
	logger.info("{%s:%s} - %s" % (
        func.co_filename, 
        func.co_firstlineno,
        message
    ))

def warn(logger, message):
	func = inspect.currentframe().f_back.f_code
	logger.warn("{%s:%s} - %s" % (
        func.co_filename, 
        func.co_firstlineno,
        message
    ))

def error(logger, message):
	func = inspect.currentframe().f_back.f_code
	logger.error("{%s:%s} - %s" % (
        func.co_filename, 
        func.co_firstlineno,
        message
    ),exc_info=True)