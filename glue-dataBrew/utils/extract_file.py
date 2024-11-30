import pandas as pd

#Defining function to extract file in csv format to outputs folder
def extract_to_csv(df, 
                    filename:str, 
                    path:str):
    """
    Parameters
    ----------
    df : name of the dataframe
    filename :  name of the output file
    path: path the output files are stored
    """
    df.to_csv(path+filename+'.csv',sep=',',index=False)

#Defining function to extract file in excel format to outputs folder
def extract_to_excel(df, 
                    filename:str, 
                    path:str):
    """
    Parameters
    ----------
    df: name of the dataframe
    filename: name of the output file
    path: path the output files are stored
    """
    df.to_excel(path+filename+'.xlsx',index=False)