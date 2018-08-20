import errno
import os
from datetime import datetime

import numpy as np
import pandas as pd
from halo import Halo
from termcolor import colored

spinner = Halo(text='Running RFS Report', spinner="simpleDotsScrolling")

spinner.start()


try:
    oekb_rfs_file = pd.read_excel('data/oekb_rfs.xlsx')
    oekb_melde_file = pd.read_excel('data/oekb_melde.xlsx')
    report_file = pd.read_csv('data/report.csv')
except FileNotFoundError:
    spinner.fail(colored('Report failed!', 'red'))
    print("""Input files not found! Please save files to compare in `data` directory as follows:
OeKB RFS List:          ./data/oekb_rfs.xlsx
OeKB Meldefonds List:   ./data/oekb_melde.xlsx
Lipper RFS Report:      ./data/report.csv""")
    os.system('pause')
else:
    oekb_rfs = set(oekb_rfs_file['ISIN'])

    oekb_melde_all = set(
        oekb_melde_file.loc[oekb_melde_file['ISIN'].str[:2] != 'AT']['ISIN'])

    criteria_1 = report_file['Domicile'] != 'Austria'
    criteria_2 = report_file['Umbrella Flag'] == 'No'
    criteria_all = criteria_1 & criteria_2
    report_file_filtered = report_file[criteria_all].dropna(subset=['ISIN'])

    lipper_all = set(report_file_filtered['ISIN'])

    lipper_rfs = set(
        report_file_filtered[report_file_filtered['Notified for Sale in Austria'] == 'Yes']['ISIN'])

    melde_criteria_1 = report_file_filtered['Austrian Registration Status'] == 'Meldefonds'
    melde_criteria_2 = report_file_filtered['Austrian Registration Status'] == 'Meldefonds / Zugelassen'
    melde_criteria_3 = report_file_filtered['Austrian Registration Status'] == 'Zugelassen'
    melde_criteria_all = melde_criteria_1 | melde_criteria_2 | melde_criteria_3

    lipper_melde_all = set(report_file_filtered[melde_criteria_all]['ISIN'])

    lipper_melde = set(report_file_filtered[melde_criteria_1]['ISIN'])
    lipper_melde_zugelassen = set(
        report_file_filtered[melde_criteria_2]['ISIN'])
    lipper_zugelassen = set(report_file_filtered[melde_criteria_3]['ISIN'])

    oekb_melde = oekb_melde_all - oekb_rfs
    oekb_zugelassen = oekb_rfs - oekb_melde_all
    oekb_melde_zugelassen = oekb_melde_all & oekb_rfs

    melde_to_delete = lipper_melde_all - (oekb_melde_all | oekb_rfs)

    melde_to_add = ((oekb_melde_all | oekb_rfs) -
                    lipper_melde_all) & lipper_all

    rfs_to_delete = lipper_rfs - oekb_rfs

    rfs_to_add = (oekb_rfs - lipper_rfs) & lipper_all

    def filter_melde(x):
        if (x in oekb_melde and x in lipper_melde):
            return False
        if (x in oekb_zugelassen and x in lipper_zugelassen):
            return False
        if(x in oekb_melde_zugelassen and x in lipper_melde_zugelassen):
            return False
        return True

    melde_to_filter = ((oekb_melde | oekb_melde_zugelassen |
                        oekb_zugelassen) & lipper_all) - melde_to_add
    melde_to_change = set(filter(filter_melde, melde_to_filter))

    def melde_status(x):
        if (x in oekb_melde):
            return 'Meldefonds'
        if (x in oekb_zugelassen):
            return 'Zugelassen'
        if (x in oekb_melde_zugelassen):
            return 'Meldefonds / Zugelassen'

    df_rfs_to_add = pd.DataFrame()
    df_rfs_to_add['ISIN Code (Xref)'] = pd.Series(list(rfs_to_add))
    df_rfs_to_add['Registered For Sale Country (Add RFS)'] = 'Austria'

    df_rfs_to_delete = pd.DataFrame()
    df_rfs_to_delete['ISIN Code (Xref)'] = pd.Series(list(rfs_to_delete))
    df_rfs_to_delete['Registered For Sale Country (Remove RFS)'] = 'Austria'

    df1 = pd.DataFrame()
    df1['ISIN Code (Xref)'] = pd.Series(list(melde_to_delete))
    df1['Austrian Registration Status'] = 'NULL'
    df1['Attribute Start Date'] = '01/01/1800'
    df1['Attribute Overwrite Historical Flag'] = 'Yes'

    df2 = pd.DataFrame()
    df2['ISIN Code (Xref)'] = pd.Series(list(melde_to_change | melde_to_add))
    df2['Austrian Registration Status'] = df2['ISIN Code (Xref)'].apply(
        melde_status)
    df2['Attribute Start Date'] = '01/01/1800'
    df2['Attribute Overwrite Historical Flag'] = 'Yes'

    df_status = pd.concat([df1, df2])

    time = datetime.now().strftime("%Y%m%d_%H%M%S")
    dirname = '/output/{}'.format(time)
    if not os.path.exists(os.getcwd() + dirname):
        try:
            os.makedirs(os.getcwd() + dirname)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    df_rfs_to_add.to_csv('output/{0}/RFS_TO_ADD_{0}.csv'.format(time))
    df_rfs_to_delete.to_csv('output/{0}/RFS_TO_REMOVE_{0}.csv'.format(time))
    df_status.to_csv('output/{0}/STATUS_TO_UPDATE_{0}.csv'.format(time))

    log_message = """RFS deleted:\t\t{}
RFS added:\t\t{}
Status deleted:\t\t{}
Status added:\t\t{}
Status updated: \t{}
""".format(len(rfs_to_delete), len(rfs_to_add), len(melde_to_delete), len(melde_to_add), len(melde_to_change))

    with open("log.txt", "a") as f:
        f.write('-' * 10)
        f.write('\n')
        f.write('-' * 10)
        f.write('\n')
        f.write(str(datetime.now()))
        f.write('\n\n')
        f.write(log_message)
        f.write('\n')

    spinner.succeed(colored('Report finished!', 'green'))
    print(log_message)
    os.system('pause')
