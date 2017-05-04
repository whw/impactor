import sys

def show(count, total, suffix=''):
    '''
    Display a progress bar
    '''
    bar_len = 40
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush() 

if __name__ == '__main__':
    show(3, 10)
