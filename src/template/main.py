from template import extract
from loguru import logger as log

if __name__ == '__main__':

    df = extract.read_raw_csv()



    log.debug('breakpoint')