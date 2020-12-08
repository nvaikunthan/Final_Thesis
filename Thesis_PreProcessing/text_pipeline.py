import psycopg2
import numpy as np
from trials_note import connect
from trials_note import close


def text_dict_intermediate(cur, n=1):
    if cur.closed:
        print("Error: cursor is closed")
    if n == 1:
        cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, COUNT(DISTINCT TEXT) \
        FROM noteevents INNER JOIN admissions ON noteevents.hadm_id = admissions.hadm_id \
        WHERE (category='Physician ' OR category='Nursing' OR category='Nursing/other') \
        AND (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) = 1))) \
        GROUP BY ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID ORDER BY ADMISSIONS.SUBJECT_ID LIMIT 7000")

    else:
        cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, COUNT(DISTINCT TEXT) \
            FROM noteevents INNER JOIN admissions ON noteevents.hadm_id = admissions.hadm_id \
            WHERE (category='Physician ' OR category='Nursing' OR category='Nursing/other') \
            AND (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) >= 2))) \
            GROUP BY ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID ORDER BY ADMISSIONS.SUBJECT_ID")

    cur_rows = cur.fetchall()
    num_of_visitors = cur.rowcount
    print(num_of_visitors)

    hadm_ids_big = []
    raw_text_dict = {}
    for row in cur_rows:
        key = row[0]
        val = row[1]


        if key not in raw_text_dict:
            raw_text_dict[key] = {val: [row[2], 0, int(row[2])/50]}
            if int(row[2]) > 100:
                hadm_ids_big.append(row[1])
        else:
            raw_text_dict[key][val] = row[2]
    return raw_text_dict, hadm_ids_big


def text_dict_train(cur, other_dict):

    length = 0
    if cur.closed:
        print("Error: cursor is closed")
    cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, TEXT \
        FROM noteevents INNER JOIN admissions ON noteevents.hadm_id = admissions.hadm_id \
        WHERE (category='Physician ' OR category='Nursing' OR category='Nursing/other') \
        AND (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) = 1))) \
        GROUP BY ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, TEXT ORDER BY ADMISSIONS.SUBJECT_ID")
    cur_rows = cur.fetchall()
    num_of_texts = cur.rowcount

    text_indicator, idx = text_dict_intermediate(cur)
    raw_text_dict = {}
    for row in cur_rows:
        subj = row[0]
        visit = row[1]
        if subj not in raw_text_dict and subj in text_indicator:
            raw_text_dict[subj] = {visit: []}

        if subj in text_indicator:
            if visit not in raw_text_dict[subj]:
                raw_text_dict[subj][visit] = []
            temp = text_indicator[subj]
            if visit in temp:

                val = temp[visit]
                if visit in idx:


                    vec = raw_text_dict[subj][visit]

                    if (val[1] % val[2]) == 0 and len(vec) < 50:
                        vec.append(row[2])
                        length += 1
                        raw_text_dict[subj][visit] = vec

                    val[1] += 1
                    text_indicator[subj][visit] = val

                else:
                    vec = raw_text_dict[subj][visit]
                    vec.append(row[2])
                    length += 1

                    raw_text_dict[subj][visit] = vec




    return raw_text_dict


def text_dict_test(cur):
    if cur.closed:
        print("Error: cursor is closed")
    cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, TEXT \
        FROM noteevents INNER JOIN admissions ON noteevents.hadm_id = admissions.hadm_id \
        WHERE (category='Physician ' OR category='Nursing' OR category='Nursing/other') \
        AND (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) >= 2))) \
        GROUP BY ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, TEXT ORDER BY ADMISSIONS.SUBJECT_ID")
    cur_rows = cur.fetchall()
    num_of_texts = cur.rowcount


    length = 0
    text_indicator, idx = text_dict_intermediate(cur, n=2)
    raw_text_dict = {}
    for row in cur_rows:
        subj = row[0]
        visit = row[1]
        if subj not in raw_text_dict and subj in text_indicator:
            raw_text_dict[subj] = {visit: []}
        if visit not in raw_text_dict[subj]:
            raw_text_dict[subj][visit] = []
        if subj in text_indicator:
            temp = text_indicator[subj]
            if visit in temp:
                val = temp[visit]
                if visit in idx:


                    vec = raw_text_dict[subj][visit]
                    if (val[1] % val[2]) == 0 and len(vec) < 50:
                        vec.append(row[2])
                        length += 1
                        raw_text_dict[subj][visit] = vec

                    val[1] += 1
                    text_indicator[subj][visit] = val

                else:
                    vec = raw_text_dict[subj][visit]
                    vec.append(row[2])
                    length += 1

                    raw_text_dict[subj][visit] = vec

    return raw_text_dict



def main():
    current_connection = connect()
    current_cur = current_connection[1]
    oth_dict = text_dict_intermediate(current_cur)
    close(current_connection[0], current_connection[1])
