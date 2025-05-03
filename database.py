import csv
import pymongo
from neo4j import GraphDatabase
import psycopg2

# Setup PostgreSQL connection
postgre_conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    user="postgres",
    password="root",
    database="university_db"
)
postgre_cursor = postgre_conn.cursor()

print("Connected to postgre successfully!")

# Setup MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["university_db"]

programs_collection = mongo_db['program_requirements']
courses_collection = mongo_db['course_details']

print("Connected to MongoDB successfully!")


# Setup Neo4j connection
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
neo4j_session = neo4j_driver.session()

print("Connected to neo4j successfully!")


# Parse the CSV file
def parse_csv(file_path):
    
    print("Reading csv data...")
    
    with open(file_path, mode='r', encoding='utf-8', errors='ignore') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    print("Finished parsing csv data!")
    return data

def create_postgre_table():
    print("Creating table in postgresql...")
    create_table_query = """
        CREATE TABLE IF NOT EXISTS courses(
            ProgID VARCHAR(50),
            ProgramName VARCHAR(100),
            Degree VARCHAR(100),
            TotalCredits INT
        )
    """
    
    postgre_cursor.execute(create_table_query)
    postgre_conn.commit()
    


# Insert into PostgreSQL
def insert_into_postgre(data):
    
    create_postgre_table()
    for row in data:
        query = """
        INSERT INTO courses (
            ProgID, ProgramName, Degree, TotalCredits
        )
        VALUES (%s, %s, %s, %s)
        """
        cursor_data = (
            row['ProgID'], row['ProgramName'], row['Degree'], row['TotalCredits']
        )
        postgre_cursor.execute(query, cursor_data)
        postgre_conn.commit()
    print("Added data into PostgreSQL courses table")

# Insert into MongoDB
def insert_into_mongodb(data):
    for row in data:
        program_data = {
            "ProgID": row['ProgID'],
            "Group_CategoryTitle": row['Group_CategoryTitle'],
            "Group_CategoryNotes": row['Group_CategoryNotes'],
            "EmphasisName": row['EmphasisName'],
            "SeriesHeading": row['SeriesHeading'],
            "GroupCredits": row['GroupCredits']
        }
        programs_collection.update_one(
            {"ProgID": row['ProgID']}, 
            {"$set": program_data},
            upsert=True
        )
        
        course_data = {
            "CourseNumber": row['CourseNumber'],
            "Course": row['Course'],
            "Title": row['Title'],
            "Credits": row['Credits'],
            "PreReq": row['PreReq'],
            "CoReq": row['CoReq'],
            "DiverseCultures": row['DiverseCultures'],
        }
        
        courses_collection.update_one(
            {"CourseNumber": row['CourseNumber']},
            {"$set": course_data},
            upsert=True
        )
    print("Added data into MongoDB!")

# Insert into Neo4j
def insert_into_neo4j(data):
    for row in data:
        query = """
        MERGE (p:Program {ProgID: $progID, ProgramName: $programName, Degree: $degree})
        MERGE (c:Course {CourseNumber: $courseNumber, Title: $title, Credits: $credits})
        MERGE (p)-[:REQUIRES]->(c)
        """
        main_params = {
            "progID": row['ProgID'],
            "programName": row['ProgramName'],
            "degree": row['Degree'],
            "courseNumber": row['CourseNumber'],
            "title": row['Title'],
            "credits": row['Credits']
        }
        
        neo4j_session.run(query, main_params)
        
        if row['PreReq']:
            query = """
                MATCH (c1:Course {CourseNumber: $courseNumber}), (c2:Course {CourseNumber: $preReq})
                MERGE (c1)-[:PRE_REQUISITE]->(c2)
            """
            
            params = {
                "courseNumber": row['CourseNumber'],
                "preReq": row['PreReq'],
            }
            neo4j_session.run(query, params)
        
        if row['CoReq']:
            query = """
                MATCH (c1:Course {CourseNumber: $courseNumber}), (c2:Course {CourseNumber: $coReq})
                MERGE (c1)-[:CO_REQUISITE]->(c2)
            """
            
            params = {
                "courseNumber": row['CourseNumber'],
                "coReq": row['CoReq'],
            }
            neo4j_session.run(query, params)
        
    print("Added data into Neo4j!")

# Main function to execute the inserts
def main():
    csv_file_path = './data/course.csv'  # Update this path to your actual CSV file location
    data = parse_csv(csv_file_path)
    
    # Insert into all databases
    print("Starting to add data into PostgreSQL...")
    insert_into_postgre(data)
    
    print("Starting to add data into MongoDB...")
    insert_into_mongodb(data)
    
    print("Starting to add data into Neo4j...")
    insert_into_neo4j(data)

    print("Data successfully inserted into PostgreSQL, MongoDB, and Neo4j.")

if __name__ == "__main__":
    main()

# Close connections
postgre_cursor.close()
postgre_conn.close()
neo4j_session.close()
neo4j_driver.close()
