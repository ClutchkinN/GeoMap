import pandas as pd
import psycopg2


def create_data_frame(path, columns, print_results):
    data = pd.read_csv(path, sep=';', low_memory=False)
    cdf = pd.DataFrame(data, columns=columns)
    if print_results:
        print(cdf)
    return cdf


if __name__ == '__main__':
    '''
    Connect to postgresql database as variable (conn) and set variable (cur) to cursor.
        DB name: test
        User: postgres
        Table: acled_africa
    '''
    conn = psycopg2.connect("dbname=test user=postgres")
    cur = conn.cursor()

    '''
    Create dataframe from csv with same collum names.
    Using create_data_frame function with passing (path(string), columns(list), print_results(boolean))
    Data in dataframe are formatted in tuples.
    '''
    df = create_data_frame(r'C:\Python\PycharmProjects\pythonProject\Acled\data\Africa_1997-2021_Jul30.csv',
                           ['ISO', 'EVENT_ID_CNTY', 'EVENT_ID_NO_CNTY', 'EVENT_DATE', 'YEAR', 'TIME_PRECISION',
                            'EVENT_TYPE', 'SUB_EVENT_TYPE', 'ACTOR1', 'ASSOC_ACTOR_1', 'INTER1', 'ACTOR2',
                            'ASSOC_ACTOR_2', 'INTER2', 'INTERACTION', 'REGION', 'COUNTRY', 'ADMIN1', 'ADMIN2',
                            'ADMIN3', 'LOCATION', 'LATITUDE', 'LONGITUDE', 'GEO_PRECISION', 'SOURCE',
                            'SOURCE_SCALE', 'NOTES', 'FATALITIES', 'TIMESTAMP'],
                           False)


    '''
        Drop current database and Create new database with same columns as csv file and dataframe.
    '''
    cur.execute("DROP TABLE IF EXISTS acled_africa;"
                "CREATE TABLE acled_africa (ISO integer, EVENT_ID_CNTY varchar, EVENT_ID_NO_CNTY integer, "
                "EVENT_DATE date, YEAR integer, TIME_PRECISION integer, EVENT_TYPE varchar, "
                "SUB_EVENT_TYPE varchar, ACTOR1 varchar, ASSOC_ACTOR_1 varchar, INTER1 integer, "
                "ACTOR2 varchar, ASSOC_ACTOR_2 varchar, INTER2 integer, INTERACTION integer, "
                "REGION varchar, COUNTRY varchar, ADMIN1 varchar, ADMIN2 varchar, "
                "ADMIN3 varchar, LOCATION varchar, LATITUDE decimal, LONGITUDE 	decimal, "
                "GEO_PRECISION integer, SOURCE varchar, SOURCE_SCALE varchar, NOTES varchar, "
                "FATALITIES integer, TIMESTAMP integer)")
    conn.commit()

    '''
        Process of looping through dataframe, changing data format and inserting dataframe tuples into DB.
    '''
    for row in df.itertuples():
        print(row)
        '''
            Changing wrong data format in dataframe where months are set as roman numbers.
        '''
        date_is = row.EVENT_DATE
        if str(row.EVENT_DATE).find('XII') != -1:
            date_is = str(row.EVENT_DATE).replace('XII', '12')
        elif str(row.EVENT_DATE).find('XI') != -1:
            date_is = str(row.EVENT_DATE).replace('XI', '11')
        elif str(row.EVENT_DATE).find('IX') != -1:
            date_is = str(row.EVENT_DATE).replace('IX', '9')
        elif str(row.EVENT_DATE).find('X') != -1:
            date_is = str(row.EVENT_DATE).replace('X', '10')
        elif str(row.EVENT_DATE).find('VIII') != -1:
            date_is = str(row.EVENT_DATE).replace('VIII', '8')
        elif str(row.EVENT_DATE).find('VII') != -1:
            date_is = str(row.EVENT_DATE).replace('VII', '7')
        elif str(row.EVENT_DATE).find('VI') != -1:
            date_is = str(row.EVENT_DATE).replace('VI', '3')
        elif str(row.EVENT_DATE).find('IV') != -1:
            date_is = str(row.EVENT_DATE).replace('IV', '4')
        elif str(row.EVENT_DATE).find('III') != -1:
            date_is = str(row.EVENT_DATE).replace('III', '3')
        elif str(row.EVENT_DATE).find('II') != -1:
            date_is = str(row.EVENT_DATE).replace('II', '2')
        elif str(row.EVENT_DATE).find('I') != -1:
            date_is = str(row.EVENT_DATE).replace('I', '1')
        elif str(row.EVENT_DATE).find('V') != -1:
            date_is = str(row.EVENT_DATE).replace('V', '5')

        '''
            Inserting data from dataframe to DB.
        '''
        cur.execute('''
                    INSERT INTO acled_africa (ISO, EVENT_ID_CNTY, EVENT_ID_NO_CNTY, EVENT_DATE, YEAR, TIME_PRECISION,
                            EVENT_TYPE, SUB_EVENT_TYPE, ACTOR1, ASSOC_ACTOR_1, INTER1, ACTOR2,
                            ASSOC_ACTOR_2, INTER2, INTERACTION, REGION, COUNTRY, ADMIN1, ADMIN2, 
                            ADMIN3, LOCATION, LATITUDE, LONGITUDE, GEO_PRECISION, SOURCE,SOURCE_SCALE, NOTES, 
                            FATALITIES, TIMESTAMP)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s)
                    ''',
                    (row.ISO, row.EVENT_ID_CNTY, row.EVENT_ID_NO_CNTY, date_is, row.YEAR, row.TIME_PRECISION,
                     row.EVENT_TYPE, row.SUB_EVENT_TYPE, row.ACTOR1, row.ASSOC_ACTOR_1,  row.INTER1, row.ACTOR2,
                     row.ASSOC_ACTOR_2, row.INTER2, row.INTERACTION, row.REGION, row.COUNTRY, row.ADMIN1, row.ADMIN2,
                     row.ADMIN3, row.LOCATION, str(row.LATITUDE).replace(',','.'), str(row.LONGITUDE).replace(',','.'),
                     row.GEO_PRECISION, row.SOURCE, row.SOURCE_SCALE, row.NOTES, row.FATALITIES, row.TIMESTAMP,)
                    )
        conn.commit()
