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

    conn = create_connection(non_normalized_db_filename)
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)
    hasho = {"Degree": df["Degree"].unique()}

    return pd.DataFrame(hasho)


def create_df_exams(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_exams' dataframe that contains only
    the exams. See screenshot below. Sort by exam!
    hints:
    # https://stackoverflow.com/a/16476974
    # https://stackoverflow.com/a/36108422
    """

    conn = create_connection(non_normalized_db_filename)
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)

    exam_year_list_of_tuples = []
    raw_exam_date_list = []

    for index, entire_row_values in df.iterrows():
        for exam_and_year_str in entire_row_values[3].split(', '):
            raw_exam_date_list.append(tuple(exam_and_year_str.split(" ")))

        for exam, year in raw_exam_date_list:
            exam_year_list_of_tuples.extend([(exam, int(year[1:-1]))])

    df2 = pd.DataFrame(exam_year_list_of_tuples, columns=['Exam', 'Year'])
    df2 = df2.sort_values(by=['Exam']).reset_index(drop=True)

    df_exams = df2.drop_duplicates().reset_index(drop=True)

    return df_exams


def create_df_students(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_students' dataframe that contains the student
    first name, last name, and degree. You will need to add another StudentID column to do pandas merge.
    See screenshot below. 
    You can use the original StudentID from the table. 
    hint: use .split on the column name!
    """

    conn = create_connection(non_normalized_db_filename)
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)

    df[['Last_Name', 'First_Name']] = df['Name'].str.split(', ', expand=True)
    df['StudentID'] = df['StudentID'].astype(int)
    return df[['StudentID', 'First_Name', 'Last_Name', 'Degree']]


def create_df_studentexamscores(non_normalized_db_filename, df_students):
    """
    Open connection to the non-normalized database and generate a 'df_studentexamscores' dataframe that 
    contains StudentID, exam and score
    See screenshot below. 
    """
    conn = create_connection(non_normalized_db_filename)
    sql_statement = "select * from Students;"
    df = pd.read_sql_query(sql_statement, conn)

    student_exam_scores = []
    for index, row in df.iterrows():

        list_of_ith_exams = list(
            map(lambda x: (x.split(" ")[0]), row[3].split(', ')))
        list_of_ith_scores = list(
            map(lambda x: int(x.split(" ")[0]), row[4].split(', ')))

        for i in range(len(list_of_ith_exams)):
            student_exam_scores.append(
                [int(row[0]), list_of_ith_exams[i], list_of_ith_scores[i]])

    student_id_list = []
    exam_list = []
    score_list = []

    for id_exam_score in student_exam_scores:
        student_id_list.append(id_exam_score[0])
        exam_list.append(id_exam_score[1])
        score_list.append(id_exam_score[2])

    exam_score_hasho = {'StudentID': student_id_list,
                        'Exam': exam_list, 'Score': score_list}

    return pd.DataFrame.from_dict(exam_score_hasho)


def ex1(df_exams):
    """
    return df_exams sorted by year
    """
    return df_exams.sort_values(by='Year')


def ex2(df_students):
    """
    return a df frame with the degree count
    # NOTE -- rename name the degree column to Count!!!
    """
    df = df_students["Degree"].value_counts().to_frame()
    df.columns = [['Count']]
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

    df = pd.merge(df_studentexamscores, df_exams, on='Exam')

    # Group by student and exam, and compute the mean score
    df_averages = df.groupby(['Exam', 'Year'])[
        'Score'].mean().round(2).reset_index()
    # Sort the dataframe in descending order by the average score
    df_averages.sort_values(by='Score', ascending=False, inplace=True)
    df_averages["Score"].astype("int32")
    df_averages["Year"] = df_averages["Year"].astype("int32")

    df_averages = df_averages.rename(columns={'Score': 'average'})
    df_averages.set_index('Exam', inplace=True)

    return df_averages


def ex4(df_studentexamscores, df_students):
    """
    return a datafram that merges df_studentexamscores and df_exams and finds the average of the degrees. Sort
    the average in descending order. See screenshot below of the output. You have to fix up the column/index names.
    Hints:
    # https://stackoverflow.com/a/45451905
    # https://stackoverflow.com/a/11346337
    # round to two decimal places
    """

    df = (
        pd.merge(df_studentexamscores, df_students, on='StudentID')
        .groupby(['Degree'])
        .mean()
        .round(2)
        .rename(columns={'Score': 'Average'})
    )

    return df['Average'].to_frame()


def ex5(df_studentexamscores, df_students):
    """
    merge df_studentexamscores and df_students to produce the output below. The output shows the average of the top 
    10 students in descending order. 
    Hint: https://stackoverflow.com/a/20491748
    round to two decimal places

    """

    temp_df = (
        pd.merge(df_studentexamscores, df_students, on='StudentID')
        .groupby(['StudentID'])
        .mean()
        .round(2)
        .reset_index()
        .sort_values(by='Score', ascending=False)
        .rename(columns={'Score': 'average'})
    )

    df = pd.merge(temp_df, df_students, on='StudentID')
    return df[['First_Name', 'Last_Name', 'Degree', 'average']].head(10)
    


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

    pass


def part2_step2():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    # ---- DO NOT CHANGE

    pass


def part2_step3(df2_scores):
    pass


def part2_step4(df2_students, df2_scores, ):
    pass


def part2_step5():
    pass


def part2_step6():
    pass
