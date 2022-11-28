cursor = createCursor(databaseConfig, section)
    schemas = cursor.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
    print(schemas)
    cursor.execute("SET search_path TO dvdrental, public;")
    
    from pprint import pprint
    actors = cursor.execute("SELECT * FROM actor;").fetchall()
    actors_df = pd.DataFrame(actors, columns=['actor_id', 'first_name', 'last_name', 'last_update'])
    print(actors_df.head())
    
    cursor.close()


