def show_pair_patterns(sid, patterns):
    if len(patterns) > 0:
        print("processing: {}".format(sid))
        for name, dates in patterns.items():
            for date in dates:
                print("pattern {} range from {} to {}".format(name, date[0], date[1]))

def show_single_patterns(sid, patterns):
    if len(patterns) > 0:
        print("processing: {}".format(sid))
        for name, dates in patterns.items():
            print("pattern {} at {}".format(name, dates))