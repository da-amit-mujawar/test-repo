import datetime
import glob
import io
import os

import numpy as np

import usp_GetTextJobFunction1


def ProcessData(ps_filename):
    ls_ary = np.genfromtxt(ps_filename, dtype=str, delimiter='|', encoding='UTF-8', comments=None)
    col1 = ls_ary[:, 0]
    col2 = ls_ary[:, 1]
    lvr_func = np.vectorize(usp_GetTextJobFunction1.usp_GetTextJobFunction1)
    ret_val = lvr_func(col1)
    with io.open(ps_filename.replace("\\", "/mismatch_") + '.txt', 'w',
                 encoding='ISO-8859-1') as lf_exception:
        for in_rec in range(len(ls_ary)):
            if col2[in_rec].upper() == ret_val[in_rec].upper():
                pass
            else:
                lf_exception.write(col1[in_rec] + "|" + col2[in_rec] + "|" + ret_val[in_rec] + "\n")
    lf_exception.close()


if __name__ == "__main__":
    ##arg = sys.argv[1]
    Fn = "text_titles_dw_jobfunc1"
    DirName = 'C:/Users/ei11066/PycharmProjects/pythonProject/Data/' + Fn + '*'
    arr = glob.glob(DirName)
    for rec in arr:
        startTime = datetime.datetime.now()
        ls_filename = rec
        ProcessData(ls_filename)
        os.rename(ls_filename,
                  ls_filename.replace(Fn, Fn + "/Processed").replace("\\", "/processed_"))
        endTime = datetime.datetime.now()
        executionTime = (endTime - startTime)
        print('{0} -  {1} - {2} - {3}'.format(startTime.strftime("%Y-%m-%d %H:%M:%S"),
                                              endTime.strftime("%Y-%m-%d %H:%M:%S"),
                                              str(executionTime), ls_filename))
    print("Process Completed")
