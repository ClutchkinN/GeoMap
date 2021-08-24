import pandas as pd
import psycopg2
import plotly.express as px

if __name__ == '__main__':
    '''
    Connect to postgresql database as variable (conn) and set variable (cur) to cursor.
        DB name: test
        User: postgres
        Table: acled_africa
    '''
    conn = psycopg2.connect("dbname=test user=postgres")
    cur = conn.cursor()

    # PostgreSQL cursor execute
    cur.execute(
        '''
        SELECT * FROM acled_africa WHERE event_date > '2021-05-20' AND event_date < '2021-07-25' AND COUNTRY = 'Mali' 
        ORDER BY EVENT_DATE
        '''
    )
    postgresql_data = cur.fetchall()

    # Creating dataframe from cursor fetchall
    df = pd.DataFrame(postgresql_data,
                      columns=['ISO', 'EVENT_ID_CNTY', 'EVENT_ID_NO_CNTY', 'EVENT_DATE', 'YEAR', 'TIME_PRECISION',
                               'EVENT_TYPE', 'SUB_EVENT_TYPE', 'ACTOR1', 'ASSOC_ACTOR_1', 'INTER1', 'ACTOR2',
                               'ASSOC_ACTOR_2', 'INTER2', 'INTERACTION', 'REGION', 'COUNTRY', 'ADMIN1', 'ADMIN2',
                               'ADMIN3', 'LOCATION', 'LATITUDE', 'LONGITUDE', 'GEO_PRECISION', 'SOURCE',
                               'SOURCE_SCALE', 'NOTES', 'FATALITIES', 'TIMESTAMP'])

    #Setting date as string because Plotly cant handle base format from data frame.
    for i in df.index:
        date_as_string = str(df['EVENT_DATE'][i])
        df.at[i,'EVENT_DATE'] = date_as_string

    # Map generating
    '''
    'ISO', 'EVENT_ID_CNTY', 'EVENT_ID_NO_CNTY', 'EVENT_DATE', 'YEAR', 'TIME_PRECISION', 'EVENT_TYPE', 'SUB_EVENT_TYPE', 
    'ACTOR1', 'ASSOC_ACTOR_1', 'INTER1', 'ACTOR2', 'ASSOC_ACTOR_2', 'INTER2', 'INTERACTION', 'REGION', 'COUNTRY', 
    'ADMIN1', 'ADMIN2', 'ADMIN3', 'LOCATION', 'LATITUDE', 'LONGITUDE', 'GEO_PRECISION', 'SOURCE', 'SOURCE_SCALE', 
    'NOTES', 'FATALITIES', 'TIMESTAMP'
    '''
    fig = px.scatter_mapbox(df, lat="LATITUDE", lon="LONGITUDE", color="EVENT_TYPE", hover_name="SUB_EVENT_TYPE",
                            hover_data=["EVENT_DATE"], zoom=6, height=800, animation_frame="EVENT_DATE")
    fig.update_traces(marker_size=10)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 400, "t": 50, "l": 50, "b": 50})
    fig.show()
    fig.write_html("C:\Python\PycharmProjects\pythonProject\Acled\priklad.html")
