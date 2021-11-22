# -*- coding: utf-8 -*-

# import pandas as pd
import wrds

conn = wrds.Connection(wrds_username='lyzhou')

# get familiar with datasets
conn.list_libraries()
conn.list_tables('tr_sdc_ma')
# conn.list_tables('tr_ds')
conn.describe_table('tr_ds_equities', 'ds2fxrate')
conn.describe_table('tr_ds_equities', 'ds2mktval')

conn.describe_table('tr_ds_equities', 'ds2icbchg')


conn.list_tables('tr_ibes')

# get datasets
ds2mktval = conn.get_table('tr_ds_equities', 'ds2mktval', obs=5)
# exchange rate
ds2exrate = conn.raw_sql("""select a.exratedate, a.midrate, a.bidrate, a.offerrate,
                            b.fromcurrcode, b.tocurrcode

                            from tr_ds_equities.ds2fxrate a
                            inner join tr_ds_equities.ds2fxcode b

                            on a.exrateintcode = b.exrateintcode

                            where b.tocurrcode='USD'
                            and a.exratedate>='01/01/2000'
                            """, date_cols=['exratedate'])
ds2exrate.to_stata('./data/datastream/ds2exrate.dta', write_index=False)


# mkt value and outstandings
ds2mktval = conn.raw_sql("""
                         select infocode,valdate,consolnumshrs,consolmktval
                         from tr_ds_equities.ds2mktval
                         where valdate>='01/01/2000'
                         """, date_cols=['valdate'])
ds2mktval.to_stata('./data/datastream/ds2mktval4.dta', write_index=False)


# ds2 reference tables
ref_tables = ['ds2ctryqtinfo','ds2security','ds2company','ds2icbchg','ds2isinchg','ds2region','ds2sedolchg','ds2statuschg']
for t in ref_tables:
    temp = conn.get_table('tr_ds_equities', t)
    to_path = './data/datastream/'+t+'.csv'
    temp.to_csv(to_path)