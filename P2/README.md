Hi
I decided to redo Ingest(e,D) using the input as json files and a database for the output.

I have created two directories (P1 and P2) and put files for each of the way ways i coded in their own directory:
P1 D were pandas
P2 D were DB Tables

This file is for P2
My source program is MyProject.py
I use each input json files to create pandas which were loaded as tables into a Postgresql DB
(I exported my four output tables:  CUSTOMER, SITE_VISIT, IMAGE, ORDER into csv files and copied them into the output director)

My 8 input json files are in the input directory:
(I don't know how to read the json records from a single and convert them into pandas)
sfi_1.json  CUSTOMER NEW ... original input_sample
sfi_2.json  CUSTOMER NEW
sfi_3.json  CUSTOMER NEW
sfi_4.json  SITE_VISIT NEW
sfi_5.json  IMAGE UPLOAD
sfi_6.json  ORDER NEW
sfi_7.json  CUSTOMER UPDATE
sfi_8.json  ORDER UPDATE

The output were 4 tables which I exported as csv files and they can be found in the output directory.

Things yet to do:
1 I'm using 8 files for json input.  They need to put into and processed from  a single file.
2 I think I Wasn't able to process the array in SITE_VISIT correctly?
3 Need to set up sqlKeysConstraints:  primary keys and foreign keys for the 4 tables
4 Should look into python parallelization.
5 Should look into database parallelization.
6 Should look into auditing and error logging.

Let me know if you have any questions either by email or call me.

Thank You,
Bob Wenzel
Wenzel.robert.p@gmail.com    or 650.233.1903


