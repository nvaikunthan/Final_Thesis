
import re
import urllib2

import json
import pickle

import time

from text_pipeline import text_dict_train as tdt
from text_pipeline import text_dict_intermediate as tint
from text_pipeline import text_dict_test as ttt
from trials_note import connect
from trials_note import close


from string import punctuation

def strip_punctuation(s):
    return ''.join(c for c in s if c not in punctuation)

REST_URL = "http://ec2-*********.amazonaws.com:8080"
API_KEY = "************************"


# borrowed
def get_json(url):
    opener = urllib2.build_opener()
    opener.addheaders = [('Authorization', 'apikey token=' + API_KEY)]
    return json.loads(opener.open(url).read())

# borrowed
def print_annotations(annotations, get_class=True):
    for result in annotations:


        print "Annotation details"
        for annotation in result["annotations"]:
            print "\tfrom: " + str(annotation["from"])
            print "\tto: " + str(annotation["to"])
            print "\tmatch type: " + annotation["matchType"]

        if result["hierarchy"]:
            print "\n\tHierarchy annotations"
            for annotation in result["hierarchy"]:
                class_details = get_json(annotation["annotatedClass"]["links"]["self"])
                pref_label = class_details["prefLabel"] or "no label"
                print "\t\tClass details"
                print "\t\t\tid: " + class_details["@id"]
                print "\t\t\tprefLabel: " + class_details["prefLabel"]
                print "\t\t\tontology: " + class_details["links"]["ontology"]
                print "\t\t\tdistance from originally annotated class: " + str(annotation["distance"])

    print "\n\n"

def divide(text):

    mid = (len(text) + 1) / 2
    firstHalf = text[:mid]
    secondHalf = text[mid:]
    return firstHalf + secondHalf

def cui_retreive(annotations, cur_list, dict, count):

    for result in annotations:
        class_details = get_json(result["annotatedClass"]["links"]["self"]) if True else result["annotatedClass"]
        cui = class_details["cui"]
        cur_list.extend(cui)
        for i in cui:
            if i not in dict:
                # print(type(i))
                count += 1
                dict[i] = count
    #print(cur_list)
    return cur_list, count


def get_cui_dicts(cur, oth_dict):

    raw_dict = ttt(cur)
    start = time.time()
    cui_idx_map = {}
    cui_dict = {}
    note_count = 0
    idx = 0
    for subj in raw_dict:
        print(note_count)
        visit_dict = raw_dict[subj]

        for visit in visit_dict:

            if subj not in cui_dict:
                cui_dict[subj] = {visit: []}
            if visit not in cui_dict[subj]:
                cui_dict[subj][visit] = []

            print(visit)
            note_list = visit_dict[visit]
            for note in note_list:
                note_count += 1
                temp_cui_list = []
                text = strip_punctuation(re.sub("[0-9]+:[0-9]+", " ", str(note)).replace('\n', ' ').replace('\r', ''))
                try:
                    annotations = get_json(REST_URL + "/annotator?" +
                                           "ontologies=SNMI&" +
                                           "semantic_types={T033,T046,T184,T037,T040,T109,T024,T191,T190,T102,T100,T099,T023,T029,T019,T038,T039,T098,T061,T080}&text="
                                           + urllib2.quote(text))
                    cui_note_list, idx = cui_retreive(annotations, temp_cui_list, cui_idx_map, idx)
                    cui_list = cui_dict[subj][visit]
                    cui_list.append(cui_note_list)
                    cui_dict[subj][visit] = cui_list
                except:

                    try:
                        pieces = divide(text)
                        annotations_first = get_json(REST_URL + "/annotator?" +
                                                     "ontologies=SNMI&" +
                                                     "semantic_types={T033,T046,T184,T037,T040,T109,T024,T191,T190,T102,T100,T099,T023,T029,T019,T038,T039,T098,T061}&text="
                                                     + urllib2.quote(pieces[0]))
                        cui_note_list, idx = cui_retreive(annotations_first, temp_cui_list, cui_idx_map, idx)
                        annotations_second = get_json(REST_URL + "/annotator?" +
                                                      "ontologies=SNMI&" +
                                                      "semantic_types={T033,T046,T184,T037,T040,T109,T024,T191,T190,T102,T100,T099,T023,T029,T019,T038,T039,T098,T061}&text="
                                                      + urllib2.quote(pieces[1]))
                        cui_note_list_2, idx = cui_retreive(annotations_second, temp_cui_list, cui_idx_map, idx)
                        cui_list = cui_dict[subj][visit]
                        cui_note_list = cui_note_list.extend(cui_note_list_2)
                        cui_list.append(cui_note_list)
                        cui_dict[subj][visit] = cui_list



                    except Exception, ex:
                        print 'Here!' + str(ex)
                        continue

    end = time.time()
    time_elapsed = end - start
    print("Done! Time elapsed: " + str(time_elapsed))
    print("Number of CUIs: " + str(idx))
    print(note_count)
    print(cui_dict)
    print(cui_idx_map)
    with open('cui_map_test.pickle', 'wb') as cuimap, open('cui_dict_test.pickle', 'wb') as cuidict:
        pickle.dump(cui_idx_map, cuimap, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(cui_dict, cuidict, protocol=pickle.HIGHEST_PROTOCOL)


def main():
    current_connection = connect()
    current_cur = current_connection[1]
    int_dict = tint(current_cur)
    get_cui_dicts(current_cur, int_dict)
    # with open ('test1.pickle', 'rb') as cuimap:
        # data = pickle.load(cuimap)
        # print(data)

    close(current_connection[0], current_connection[1])

if __name__ == '__main__':
    main()
