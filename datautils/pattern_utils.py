import logging

def show_pair_patterns(sid, patterns):
    logger = logging.getLogger('MainLogger')
    if len(patterns) > 0:
        logger.info("processing: {}".format(sid))
        for name, dates in patterns.items():
            for date in dates:
                logger.info("pattern {} range from {} to {}".format(name, date[0], date[1]))

def show_single_patterns(sid, patterns):
    logger = logging.getLogger('MainLogger')
    if len(patterns) > 0:
        logger.info("processing: {}".format(sid))
        for name, dates in patterns.items():
            logger.info("pattern {} at {}".format(name, dates))

def compute_growth(cal_df, field):
    # return (cal_df[field] - cal_df[field].shift(1)) / cal_df[field].shift(1) * 100
    return cal_df[field].diff() / cal_df[field].abs().shift()

def value_to_float(x):
    if type(x) == float or type(x) == int:
        return x
    if 'k' in x or 'K' in x:
        if len(x) > 1:
            return float(x.replace('k', '').replace('K', '')) * 1000
        return 1000.0
    if 'm' in x or 'M' in x:
        if len(x) > 1:
            return float(x.replace('m', '').replace('M', '')) * 1_000_000
        return 1_000_000.0
    if 'b' in x or 'B' in x:
        return float(x.replace('b', '').replace('B', '')) * 1_000_000_000
    return x