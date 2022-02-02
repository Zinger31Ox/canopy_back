# from datetime import date
from pyxirr import xirr

def xirr_calc(dataframe, fin_column_name):
    return xirr( dataframe["Date"] , dataframe[fin_column_name] )

def mult_calc(dataframe, fin_column_name):
    df = dataframe[fin_column_name]
    pos_mask = df>0
    neg_mask = df<0
    # pos_mask = dataframe[fin_column_name]>0
    # neg_mask = dataframe[fin_column_name]<0
    
    pos_val = sum( df[pos_mask] )
    neg_val = sum( df[neg_mask] )
    return - pos_val/neg_val

def coc_calc_quarterly(dataframe, fin_column_name):
    # temporary fix: remove last positive CF
    # date value to exclude the sale of the asset from the yield calc
    # on the basis of the sum of neg values (another proximation)

    df = dataframe[fin_column_name]

    neg_mask = df<0
    neg_val = sum( df[neg_mask] )

    pos_mask = df >0
    pos_val = sum( df[pos_mask] )
    # find last positive CF Date
    count = sum(pos_mask)
    maxdate = max(dataframe[pos_mask]["Date"])
    sub_val = dataframe.loc[dataframe['Date'] == maxdate, fin_column_name].sum()


    return - (pos_val - sub_val)*4/neg_val / (count - 1)



