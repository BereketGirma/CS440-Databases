import psycopg2
import pymongo
from neo4j import GraphDatabase

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


def run_neo4j_query(query, params=None):
    if params:
        result = neo4j_session.run(query, params)
    else:
        result = neo4j_session.run(query)
    return result

def run_postgresql_query(query, params=None):
    if params and params[0]:  # Ensure that the tuple is not empty
        postgre_cursor.execute(query, params)
        return postgre_cursor.fetchall()
    else:
        return [] 

def run_mongo_query(query):
    return list(courses_collection.find(query))

def query_1(prog_id):
    print("\n--- Query 1: Course details ---")
    print(f"\nRunning Unified Query 1 for ProgID: {prog_id}")

    # Step 1. PostgreSQL: Get Program Information
    pg_query = "SELECT ProgramName, Degree FROM courses WHERE ProgID = %s"
    postgre_cursor.execute(pg_query, (prog_id,))
    pg_result = postgre_cursor.fetchone()
    
    if not pg_result:
        print(f"No program found in PostgreSQL for ProgID: {prog_id}")
        return

    program_name, degree = pg_result
    print(f"\nPostgreSQL - Program Info:\nProgram Name: {program_name}, Degree: {degree}")

    # Step 2. MongoDB: Get course numbers associated with the program
    mongo_courses = list(courses_collection.find({"ProgID": prog_id}, {
        "_id": 0, "CourseNumber": 1, "Title": 1, "Credits": 1
    }))

    if not mongo_courses:
        print("\nMongoDB - No course data found for this ProgID.")
    else:
        print("\nMongoDB - Courses:")
        for course in mongo_courses:
            print(course)

    # Step 3. Neo4j: Validate or extend course information from relationships
    neo4j_query = """
    MATCH (p:Program {ProgID: $prog_id})-[:REQUIRES]->(c:Course)
    RETURN c.CourseNumber AS CourseNumber, c.Title AS Title, c.Credits AS Credits
    """
    neo4j_result = neo4j_session.run(neo4j_query, {"prog_id": prog_id})
    neo4j_courses = [record.data() for record in neo4j_result]

    if not neo4j_courses:
        print("\nNeo4j - No course relationships found.")
    else:
        print("\nNeo4j - Courses connected via REQUIRES:")
        for course in neo4j_courses:
            print(course)

    # Final output
    return {
        "program": {
            "ProgID": prog_id,
            "ProgramName": program_name,
            "Degree": degree
        },
        "mongo_courses": mongo_courses,
        "neo4j_courses": neo4j_courses
    }
    
def query_2():
    print("\n--- Query 2: Courses with Prerequisites (from Neo4j node properties) ---")

    # Step 1: Find courses with PreReqText set
    neo4j_query = """
    MATCH (c:Course)
    WHERE c.PreReqText IS NOT NULL AND trim(c.PreReqText) <> ""
    RETURN c.CourseNumber AS CourseNumber
    LIMIT 10
    """
    result = neo4j_session.run(neo4j_query)
    course_numbers = [record["CourseNumber"] for record in result]

    if not course_numbers:
        print("No courses with prerequisites found.")
        return

    print(f"Found the first {len(course_numbers)} courses with prerequisites.")

    # Step 2: Show course details from MongoDB
    print("\nMongoDB course details:")
    for number in course_numbers:
        course = courses_collection.find_one({"CourseNumber": number})
        if course:
            print(f"Course: {course.get('Course')} - {course.get('Title')}")
            print(f"  Credits: {course.get('Credits')}")
            print(f"  PreReq: {course.get('PreReq')}")
            print(f"  CoReq: {course.get('CoReq')}\n")
        else:
            print(f"MongoDB: No course found with CourseNumber {number}")

def query_3():
    print("\n--- Query 3: Summary of Prereqs/Coreqs for a Program ---")
    prog_id = "202052921"

    # Step 1: Get 10 CourseNumbers from PostgreSQL
    pg_query = """
    SELECT DISTINCT CourseNumber FROM courses
    WHERE ProgID = %s
    LIMIT 10
    """
    postgre_cursor.execute(pg_query, (prog_id,))
    course_numbers = [row[0] for row in postgre_cursor.fetchall()]

    # Step 2: MongoDB lookup and summary
    summary = {
        "total_courses": 0,
        "with_prereqs": 0,
        "with_coreqs": 0,
        "with_both": 0
    }

    for course_number in course_numbers:
        course_doc = courses_collection.find_one({"CourseNumber": course_number})
        if not course_doc:
            continue

        has_prereq = bool(course_doc.get("PreReq", "").strip())
        has_coreq = bool(course_doc.get("CoReq", "").strip())

        summary["total_courses"] += 1
        if has_prereq:
            summary["with_prereqs"] += 1
        if has_coreq:
            summary["with_coreqs"] += 1
        if has_prereq and has_coreq:
            summary["with_both"] += 1

    # Print results
    print(f"\nSummary for Program ID: {prog_id}")
    print(f"  Total Courses Checked: {summary['total_courses']}")
    print(f"  Courses with Prerequisites: {summary['with_prereqs']}")
    print(f"  Courses with Corequisites: {summary['with_coreqs']}")
    print(f"  Courses with Both: {summary['with_both']}")


def query_4():
    print("\n--- Query 4: Integrate PostgreSQL, MongoDB, and Neo4j ---")
    prog_id = "201749219"

    # Step 1. PostgreSQL: get program name
    postgre_cursor.execute("SELECT ProgramName FROM courses WHERE ProgID = %s LIMIT 1", (prog_id,))
    result = postgre_cursor.fetchone()
    program_name = result[0] if result else "Unknown Program"
    print(f"Program: {program_name} (ProgID: {prog_id})")

    # Step 2. MongoDB: get up to 10 course documents for the program
    course_docs = list(courses_collection.find(
        {"ProgID": prog_id},
        {"CourseNumber": 1, "Title": 1, "Credits": 1}
    ).limit(10))

    if not course_docs:
        print(f"No courses found for ProgID: {prog_id}")
        return

    total_credits = 0
    print("\nCourses:")

    for course in course_docs:
        course_number = course.get("CourseNumber", "N/A")
        title = course.get("Title", "No Title")
        credits = float(course.get("Credits", 0))

        # Step 3. Neo4j: fetch prereq/coreq text by CourseNumber
        neo4j_result = neo4j_session.run(
            "MATCH (c:Course {CourseNumber: $courseNumber}) RETURN c.PreReqText AS prereq, c.CoReqText AS coreq",
            {"courseNumber": course_number}
        ).single()

        prereq = neo4j_result["prereq"] if neo4j_result else "None"
        coreq = neo4j_result["coreq"] if neo4j_result else "None"

        print(f"  {course_number}: {title} ({credits} credits)")
        print(f"    PreReq: {prereq}")
        print(f"    CoReq:  {coreq}")

        total_credits += credits

    print(f"\nTotal Credits from listed courses: {total_credits}")

from collections import Counter

def query_5():
    print("\n--- Query 5: Top 10 Most Required Courses Across Programs ---")

    # Step 1: MongoDB — Get all program-course references
    required_courses = programs_collection.aggregate([
        {"$group": {"_id": "$ProgID", "count": {"$sum": 1}}},
    ])
    program_ids = [entry["_id"] for entry in required_courses]

    # Step 2: MongoDB — Count course frequencies from course_details
    all_courses = list(courses_collection.find({}, {"CourseNumber": 1}))
    course_counts = Counter([c["CourseNumber"] for c in all_courses if "CourseNumber" in c])
    top_10 = course_counts.most_common(10)

    print("\nTop 10 Most Required Courses:")
    for course_num, count in top_10:
        # Step 3: Neo4j — Check if course exists in Neo4j
        result = neo4j_session.run(
            "MATCH (c:Course {CourseNumber: $num}) RETURN c.Title AS title LIMIT 1",
            {"num": course_num}
        ).single()
        title = result["title"] if result else "Title Not Found"

        # Step 4: PostgreSQL — Ensure at least one valid program exists for this course's ProgID
        postgre_cursor.execute(
            "SELECT COUNT(*) FROM courses WHERE ProgID = %s",
            (course_num[:10],)  # Assuming some match via substring
        )
        pg_count = postgre_cursor.fetchone()[0]

        print(f"  {course_num}: {title} | Occurrences: {count} | PG Valid: {'Yes' if pg_count else 'No'}")

def query_6():
    print("\n-- Query 6: Courses with prerequisites or corequisites, program names, and Neo4j link status. --")

    # Step 1: Fetch from MongoDB
    mongo_courses = list(courses_collection.find({
        "$or": [{"PreReq": {"$ne": ""}}, {"CoReq": {"$ne": ""}}]
    }))

    results = []

    for course in mongo_courses[:10]:  # Limit to first 10 results
        course_number = course.get('CourseNumber')
        prog_id = course.get('ProgID')

        # Step 2: Get Program Name from PostgreSQL
        postgre_cursor.execute("SELECT ProgramName FROM courses WHERE ProgID = %s", (prog_id,))
        pg_result = postgre_cursor.fetchone()
        program_name = pg_result[0] if pg_result else "Unknown"

        # Step 3: Check Neo4j for REQUIRES relationship
        neo4j_result = neo4j_session.run("""
            MATCH (p:Program)-[:REQUIRES]->(c:Course {CourseNumber: $course_number})
            RETURN COUNT(c) AS count
        """, course_number=course_number)

        has_neo4j_relation = neo4j_result.single()["count"] > 0

        results.append({
            "CourseNumber": course_number,
            "Title": course.get('Title'),
            "PreReq": course.get('PreReq'),
            "CoReq": course.get('CoReq'),
            "ProgramName": program_name,
            "InNeo4j": has_neo4j_relation
        })

    # Output
    for r in results:
        print(f"{r['CourseNumber']} - {r['Title']}")
        print(f"  Program: {r['ProgramName']}")
        print(f"  PreReq: {r['PreReq'] or 'None'} | CoReq: {r['CoReq'] or 'None'}")
        print(f"  In Neo4j: {'Yes' if r['InNeo4j'] else 'No'}")
        print("----")


def placeholder(query_number):
    print(f"Query #{query_number} is not yet implemented.\n")

def main():
    while True:
        print("\n--- Multi-DB Query CLI ---")
        print("1. Programs that include a course with prerequisites")
        print("2. Programs where all courses have no prerequisites (Neo4j + PostgreSQL)")
        print("3. Summary of Prereqs/Coreqs for a Program")
        print("4. Integrate PostgreSQL, MongoDB, and Neo4j ")
        print("5. Top 10 Most Required Courses Across Programs")
        print("6. Courses with prerequisites or corequisites, program names, and Neo4j link status")
        print("0. Exit")
        
        choice = input("Select a query (0-6): ")
        
        if choice == "1":
            print(query_1("201749219"))
        elif choice == "2":
            query_2()
        elif choice == "3":
            query_3()
        elif choice == "4":
            query_4()
        elif choice == "5":
            query_5()
        elif choice == "6":
            query_6()
        elif choice == "0":
            break
        else:
            print("Invalid choice. Try again.")
if __name__ == "__main__":
    main();        