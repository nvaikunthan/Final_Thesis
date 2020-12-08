import psycopg2
import numpy as np
from trials_note import connect
from trials_note import close


def ico_code_retriever(cur):
    if cur.closed:
        print("Error: cursor is closed")
    cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, COUNT(DISTINCT TEXT) \
    FROM noteevents INNER JOIN admissions ON noteevents.hadm_id = admissions.hadm_id \
    WHERE (category='Physician ' OR category='Nursing' OR category='Nursing/other') \
    AND (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) >= 2))) \
    GROUP BY ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID ORDER BY ADMISSIONS.SUBJECT_ID")

    cur_rows = cur.fetchall()
    num_of_visitors = cur.rowcount

    code_dict = {}

    for row in cur_rows:
        key = row[0]
        val = row[1]

        many_hot_vec = np.zeros([943,])
        if key not in code_dict:
            code_dict[key] = {val: many_hot_vec}
        else:
            code_dict[key][val] = many_hot_vec
    print(len(code_dict.keys()))
    return code_dict

def ico_map(cur):
    if cur.closed:
        print("Error: cursor is closed")

    cur.execute("SELECT DISTINCT ICD9_CODE \
    FROM diagnoses_icd INNER JOIN admissions ON diagnoses_icd.hadm_id = admissions.hadm_id \
    WHERE (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) >= 2))) \
    ORDER BY ICD9_CODE LIMIT 4893")

    cur_rows = cur.fetchall()
    num_of_visitors = cur.rowcount
    list_of_codes = {}
    counter = 0

    for row in cur_rows:
        code_ex = row[0][0:3]
        if code_ex not in list_of_codes:
            list_of_codes[code_ex] = counter
            counter += 1
        else:
            pass


    cur.execute("SELECT DISTINCT ICD9_CODE \
         FROM diagnoses_icd INNER JOIN admissions ON diagnoses_icd.hadm_id = admissions.hadm_id \
         WHERE (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) = 1))) \
         ORDER BY ICD9_CODE LIMIT 6427")
    cur_rows = cur.fetchall()
    for row in cur_rows:
        code_ex = row[0][0:3]
        if code_ex not in list_of_codes:
            list_of_codes[code_ex] = counter
            counter += 1
        else:
            pass

    return list_of_codes

def ico_dict_populator(cur):
    if cur.closed:
        print("Error: cursor is closed")



    list_of_codes = ico_map(cur)
    cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, ICD9_CODE \
    FROM diagnoses_icd INNER JOIN admissions ON diagnoses_icd.hadm_id = admissions.hadm_id \
    WHERE (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) >= 2))) \
    ORDER BY ADMISSIONS.SUBJECT_ID")

    second_query = cur.fetchall()

    ico_dict = ico_code_retriever(cur)

    for row in second_query:
        key1 = row[0]
        key2 = row[1]

        if row[2] is not None:
            code = row[2][0:3]
        else:
            continue

        idx = list_of_codes[code]
        if key1 in ico_dict:
            dict2 = ico_dict[key1]
            if key2 in dict2:
                cur_vect = ico_dict[key1][key2]
                cur_vect[idx] = 1
                ico_dict[key1][key2] = cur_vect


    return ico_dict


def dict_to_paddedXY(ico_dict, keys, max_length=40, feat_nums=943):
    dim = len(keys)
    X = np.zeros((dim, max_length, feat_nums))
    Y = np.zeros((dim, max_length, feat_nums))
    i = 0
    for key in ico_dict:
        if key in keys:
            j = 0
            dict_2 = ico_dict[key]
            keys2 = list(dict_2.keys())
            for key2 in dict_2:

                X[i, j, :] = dict_2[key2]
                if j == (len(keys2) - 1):
                    Y[i, j, -1] = 1
                else:
                    Y[i, j, :] = dict_2[keys2[j + 1]]
                j += 1
            i += 1
        else:
            continue

    return X, Y


def main():
    current_connection = connect()
    current_cur = current_connection[1]
    # ico_map(current_cur)
    ico_dict = ico_dict_populator(current_cur)
    print(len(ico_dict.keys()))
    close(current_connection[0], current_connection[1])

if __name__ == '__main__':
    main()

