import logging
import pycassa

comedy_logger = pycassa.PycassaLogger()
comedy_logger.set_logger_name('comedy')
comedy_logger.set_logger_level('debug')

#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
