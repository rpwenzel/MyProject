# MyProjectsee file: README.md.docx
There are two Projects:  
P1  first attemp using a single file as input and creating pandas and then exporting the pandas into .csv files

P2  Halfway thru, I think I should have the json pandas creation and put the output into Postgresql tables, which were exported.

P3  Has both parts now along with a bunch of data that I used.
    I used Postgres as my database.  It can be set up on windows followin the comments below:
    I used postgresql for windows for my Data warehouse: postgresql-13.0-1-windows-x64.exe

After installing postgress with: password    for user postgress password:

1.
createdb -h localhost -p 5432 -U postgres bobtest
password: password  (was specified when prompted)

2.
createuser -P -U postgres bob 

3.
C:\Program Files\PostgreSQL\13\bin>psql bobtest postgres
Password for user postgres: password  --(entered)

4.
bobtest=# \du                         --(\du shows users)
results:
 Role name |                         Attributes                         | Member of
-----------+------------------------------------------------------------+-----------
 bob       |                                                            | {}
 postgres  | Superuser, Create role, Create DB, Replication, Bypass RLS | {}
5.
bobtest=# exit

Please contact me if you need any help or have any questions re postgres install

'''
