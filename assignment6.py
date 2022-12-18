import sqlite3

import numpy as np
import pandas as pd
from faker import Faker


def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


conn = create_connection('non_normalized.db')
sql_statement = "select * from Students;"
df = pd.read_sql_query(sql_statement, conn)
print(df)


def create_df_degrees(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_degrees' dataframe that contains only
    the degrees. See screenshot below. 
    """

    # BEGIN SOLUTION
    conn = create_connection(non_normalized_db_filename)
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)
    hasho = {"Degree": df["Degree"].unique()}
    df_2 = pd.DataFrame(hasho)
    return df_2
    # END SOLUTION


def create_df_exams(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_exams' dataframe that contains only
    the exams. See screenshot below. Sort by exam!
    hints:
    # https://stackoverflow.com/a/16476974
    # https://stackoverflow.com/a/36108422
    """

    # BEGIN SOLUTION

    exam_list = []
    unique_exam_and_date = []

    conn = create_connection(non_normalized_db_filename)
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)
    exam_list = df["Exams"].unique()

    exam_hasho = {"Exam": [], "Year": []}
    for i in range(len(exam_list)):
        exam_and_date = exam_list[i].split(",")
        for i in range(len(exam_and_date)):
            if exam_and_date[i].strip() not in unique_exam_and_date:
                unique_exam_and_date.append(exam_and_date[i].strip())
    unique_exam_and_date.sort()

    for exam in unique_exam_and_date:
        exam_name, year = exam.split(" (")
        exam_hasho["Exam"].append(exam_name)
        exam_hasho["Year"].append(int(year[:-1]))
    df_exams = pd.DataFrame.from_dict(exam_hasho)

    return df_exams
    # END SOLUTION


def create_df_students(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_students' dataframe that contains the student
    first name, last name, and degree. You will need to add another StudentID column to do pandas merge.
    See screenshot below. 
    You can use the original StudentID from the table. 
    hint: use .split on the column name!
    """

    # BEGIN SOLUTION
    conn = create_connection("non_normalized.db")
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)

    student_names = df["Name"]
    student_degrees = df["Degree"]

    students_hasho = {"StudentID": [],
                      "First_Name": [], "Last_Name": [], "Degree": []}

    for i in range(len(student_names)):
        students_hasho["StudentID"].append(i+1)
        students_hasho["First_Name"].append(student_names[i].split(", ")[1])
        students_hasho["Last_Name"].append(student_names[i].split(", ")[0])
        students_hasho["Degree"].append(student_degrees[i])

    df_students = pd.DataFrame.from_dict(students_hasho)
    return df_students
    # END SOLUTION


def create_df_studentexamscores(non_normalized_db_filename, df_students):
    """
    Open connection to the non-normalized database and generate a 'df_studentexamscores' dataframe that 
    contains StudentID, exam and score
    See screenshot below. 
    """

    # BEGIN SOLUTION
    conn = create_connection("non_normalized.db")
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)

    student_exams = df["Exams"]
    student_scores = df["Scores"]
    student_ids = df["StudentID"]

    exam_scores_hasho = {
        "StudentID": [],
        "Exam": [],
        "Score": []
    }

    for i in range(len(student_exams)):
        ith_student_exams = student_exams[i].split(" ")[::2]
        ith_student_scores = student_scores[i].split(", ")
        for j in range(len(ith_student_exams)):
            exam_scores_hasho["StudentID"].append(i+1)
            exam_scores_hasho['Exam'].append(ith_student_exams[j])
            exam_scores_hasho["Score"].append(int(ith_student_scores[j]))

    df_studentexamscores = pd.DataFrame.from_dict(exam_scores_hasho)
    return df_studentexamscores

    # END SOLUTION


def ex1(df_exams):
    """
    return df_exams sorted by year
    """
    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df_exams


def ex2(df_students):
    """
    return a df frame with the degree count
    # NOTE -- rename name the degree column to Count!!!
    """
    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df


def ex3(df_studentexamscores, df_exams):
    """
    return a datafram that merges df_studentexamscores and df_exams and finds the average of the exams. Sort
    the average in descending order. See screenshot below of the output. You have to fix up the column/index names.
    Hints:
    # https://stackoverflow.com/a/45451905
    # https://stackoverflow.com/a/11346337
    # round to two decimal places
    """

    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df


def ex4(df_studentexamscores, df_students):
    """
    return a datafram that merges df_studentexamscores and df_exams and finds the average of the degrees. Sort
    the average in descending order. See screenshot below of the output. You have to fix up the column/index names.
    Hints:
    # https://stackoverflow.com/a/45451905
    # https://stackoverflow.com/a/11346337
    # round to two decimal places
    """

    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df


def ex5(df_studentexamscores, df_students):
    """
    merge df_studentexamscores and df_students to produce the output below. The output shows the average of the top 
    10 students in descending order. 
    Hint: https://stackoverflow.com/a/20491748
    round to two decimal places

    """

    # BEGIN SOLUTION
    pass
    # END SOLUTION


# DO NOT MODIFY THIS CELL OR THE SEED

# THIS CELL IMPORTS ALL THE LIBRARIES YOU NEED!!!


np.random.seed(0)
fake = Faker()
Faker.seed(0)


def part2_step1():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    fake = Faker()
    Faker.seed(0)
    # ---- DO NOT CHANGE

    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step2():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    # ---- DO NOT CHANGE

    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step3(df2_scores):
    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step4(df2_students, df2_scores, ):
    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step5():
    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step6():
    # BEGIN SOLUTION
    pass
    # END SOLUTION
