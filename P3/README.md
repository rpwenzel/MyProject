P3 Has both parts now along with a bunch of data that I used. 
   I used Postgres as my database. It can be set up on windows followin the comments below: 
   I used postgresql for windows for my Data warehouse: postgresql-13.0-1-windows-x64.exe

After installing postgress with: password for user postgress password:

1.  createdb -h localhost -p 5432 -U postgres bobtest password: password (was specified when prompted)

2.  createuser -P -U postgres bob

3.  C:\Program Files\PostgreSQL\13\bin>psql bobtest postgres Password for user postgres: password --(entered)

4.  bobtest=# \du --(\du shows users) results: Role name | Attributes | Member of -----------+------------------------------------------------------------+----------- bob | | {} postgres | Superuser, Create role, Create DB, Replication, Bypass RLS | {} 

5. bobtest=# exit

Please contact me if you need any help or have any questions re postgres install

--------------------------------------------------------------------------------


There are 4 directories and a README.md file  (your reading it)
creating data and checking the data took more time than I thought it would

input  directory
    file named: 1file_input.json
       first record is the x for top SLTV records
       other records the json input
           has data covering data over 10 years
           orders with keys 100000000003 - 100000000345 has 6 weeks with multiple days(2) with orders

output directory
    file named: MyProject_output.txt
    results from MyProject.py

src directory
    file named: MyProject.py
    python program

 sample_input direcotry
    file named: events.txt
    example give of the input