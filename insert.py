import psycopg2
try:
    connection = psycopg2.connect(user = "intern",
                                  password = "anchainaiintern",
                                  host = "beidb.cehxu5roxwmo.us-west-2.rds.amazonaws.com",
                                  port = "5432",
                                  database = "beidb")
    cursor = connection.cursor()

    #create_table_query = '''CREATE TABLE BEI_Address
    #  (ID serial PRIMARY KEY,
    #  address VARCHAR (50) UNIQUE NOT NULL,
    #  entity_name VARCHAR,
    #  chain_name VARCHAR,
    #  owner_type VARCHAR,
    #  data_source VARCHAR); '''

    #cursor.execute(create_table_query)
    #connection.commit()
    #print("Table created in PostgreSQL")

    i = 0
    num = int(input("Next num in table: "))
    eName = input("Exchange name: ")
    chain = "BTC"
    oType = input("Type: ")
    source = "https://www.walletexplorer.com"
    f = open(eName + ".txt", "r")
    fileOne = f.readlines()
    for x in fileOne:
        if len(x)<30:
            continue
        postgres_insert_query = """ INSERT INTO BEI_Address (ID, address, entity_name, chain_name, owner_type, data_source) VALUES (%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (str(num), x, eName, chain, oType, source )
        cursor.execute(postgres_insert_query, record_to_insert)
        count = cursor.rowcount
        connection.commit()
        num+=1
        if(num%10,000 == 0):
            print(num)
    print ("File inserted successfully into table")
    f.close()



    

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)


cursor.close()
connection.close()
print("Connection Closed")