def show_pair_patterns(patterns):
    if len(patterns) > 0:
        for k, v in patterns.items():
            for pattern in v:
                print('pattern {} range from {} to {}'.format(k, pattern[0], pattern[1]))

def show_single_patterns(patterns):
    if len(patterns) > 0:
        for k, v in patterns.items():
            print('pattern {} at {}'.format(k, v))