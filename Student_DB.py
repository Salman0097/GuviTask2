# Run the below command to install pymongo library to connect to the NoSql DB and import it
# pip install pymongo

# Run the below command to install pandas library and import it
# pip install pandas

from pymongo import MongoClient
import json
import pandas as pd

# Establishing a new connection with MongoDB client
client = MongoClient("mongodb://localhost:27017/")

# Creating a DataBase
my_db = client["Students"]

# Creating a Collection
my_collection = my_db["Scores"]

# Load JSON file using pandas library
data = pd.read_json("students.json", lines = True)
data_dict = data.to_dict("records")

# Insert the file to the collection
if isinstance(data_dict, list):
    my_collection.insert_many(data_dict)
else:
    my_collection.insert_one(data_dict)

def max_result(type):
  max_result=my_collection.aggregate([
      {"$unwind" : "$scores" },
    {"$match": {"scores.type": type}},
    {
        "$group": {
            "_id" : "null",
            "name": {"$first": "$name"},
            "max_marks" : {"$max":"$scores.score"},
        }
    }
  ])
  return max_result

max_marks_exam = [i for i in max_result("exam")]
max_marks_quiz = [i for i in max_result("quiz")]
max_marks_homework = [i for i in max_result("homework")]

print("Find the student name who scored maximum scores in all (exam, quiz and homework)?")
print(max_marks_exam[0])
print(max_marks_quiz[0])
print(max_marks_homework[0])

print("Find students who scored below average in the exam and pass mark is 40%")