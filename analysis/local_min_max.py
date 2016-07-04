import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt


def find_local_min_max(series, x):
    state = {
        'pmin': None,
        'pmin_ind': None,
        'pmax': None,
        'pmax_ind': None,
        'lmin': None,
        'lmax': None
    }
    state['pmin_ind'] = series.index.tolist()[0]
    state['pmin'] = series.iat[0]
    state['pmax_ind'] = series.index.tolist()[0]
    state['pmax'] = series.iat[0]
    local_min_ind = []
    local_min = []
    local_max_ind = []
    local_max = []
    for ind, value in series.iteritems():
        if state['pmin'] is not None and state['pmax'] is not None:
            if value <= state['pmin']:
                state['pmin'] = value
                state['pmin_ind'] = ind
                if value <= state['pmax'] * (1 - x):
                    state['lmax'] = state['pmax']
                    local_max_ind.append(state['pmax_ind'])
                    local_max.append(state['pmax'])
                    state['pmax'] = None
                    state['pmax_ind'] = None
            elif value >= state['pmax']:
                state['pmax'] = value
                state['pmax_ind'] = ind
                if value > state['pmin'] * (1 + x):
                    state['lmin'] = state['pmin']
                    local_min_ind.append(state['pmin_ind'])
                    local_min.append(state['pmin'])
                    state['pmin'] = None
                    state['pmin_ind'] = None
            else:
                pass
        elif state['pmax'] is not None and state['lmin'] is not None:
            if value >= state['pmax']:
                state['pmax'] = value
                state['pmax_ind'] = ind
            elif value <= state['lmin']:
                state['lmax'] = state['pmax']
                local_max_ind.append(state['pmax_ind'])
                local_max.append(state['pmax'])
                state['pmax'] = None
                state['pmax_ind'] = None
                state['pmin'] = value
                state['pmin_ind'] = ind
            else:
                state['lmin'] = None
                state['pmin'] = value
                state['pmin_ind'] = ind
        elif state['pmin'] is not None and state['lmax'] is not None:
            if value <= state['pmin']:
                state['pmin'] = value
                state['pmin_ind'] = ind
            elif value >= state['lmax']:
                state['lmin'] = state['pmin']
                local_min_ind.append(state['pmin_ind'])
                local_min.append(state['pmin'])
                state['pmin'] = None
                state['pmin_ind'] = None
                state['pmax'] = value
                state['pmax_ind'] = ind
            else:
                state['lmax'] = None
                state['pmax'] = value
                state['pmin_ind'] = ind
        else:
            print('strange')
    return local_min_ind, local_min, local_max_ind, local_max


if __name__ == '__main__':
    close_price = pd.read_csv('close_price.csv', header=None, index_col=[0])
    close_price = close_price.iloc[:, 0]
    local_min_ind, local_min, local_max_ind, local_max = find_local_min_max(close_price, .25)
