# coding: utf-8
#!/usr/bin/env python
######################
##  2016-02-20  Created by David Portnoy
######################

import json_delta
import json
import yaml
import os
import glob      # Wildcard search
import sys       # for stdout
import datetime  # For timestamp


# Globals
debug = False



def print_same_line(print_string):
    sys.stdout.write("\r" + str(print_string))
    sys.stdout.flush()
    

#: There are multiple versions of the schema
def make_json_data_struct_compatible(json_data_struct):
    if type(json_data_struct) is dict:
        json_data_struct = json_data_struct.get('dataset', None)
    return json_data_struct



def load_file_json(json_file_name):
    with open(json_file_name) as json_file:
        json_data_struct = json.load(json_file)
        json_data_struct = make_json_data_struct_compatible(json_data_struct)
    return json_data_struct



def parse_date(file_name):
    starting_point_of_date = "_20"
    date_pos_start = file_name.find(starting_point_of_date)+1
    return file_name[date_pos_start:date_pos_start+10]




def get_file_list(max_load=None):  
    file_pattern = "snapshots/"
    file_pattern += "HealthData.gov[_][0-9][0-9][0-9][0-9][-][0-9][0-9][-][0-9][0-9][_]data.json"
    file_list_all = sorted(glob.glob(file_pattern), reverse=True)
    list_size = len(file_list_all) if not max_load else max_load
    file_list = file_list_all[:list_size]
    return file_list
    
    

def load_file_list(file_list):
    json_data_list = []
    for index in range(len(file_list)):
        json_data_list.append(load_file_json(file_list[index]))
    return json_data_list



KEYS_TO_IGNORE = ['temporal','modified']

# Recursively sort any lists it finds (and convert dictionaries
# to lists of (key, value) pairs so that they're orderable)
# Remove values of keys that should be ignored
def ordered_json(obj):
    
    if isinstance(obj, dict):
        sort_tuples = []
        for key, value in obj.items():
            if key in KEYS_TO_IGNORE: value = ''  # Remove value of ignored keys
            sort_tuples.append((key, ordered_json(value)))
        return sorted(sort_tuples)
    
    elif isinstance(obj, list):
        return sorted(ordered_json(item) for item in obj)
    
    else:
        return obj


#=== Check status counts between consecutive days ===
# No longer needed, because get_comparison_diffs() includes this functionality
'''
Example output: {'Deleted': 0, 'Added': 33, 'Identical': 1501, 'Changed': 55}
'''
def get_comparison_counts(json_compare_dict):
    totals = {}
    for key, value in json_compare_dict.items():
        #print dataset['identifier']
        check_status = value['Status']
        if check_status in totals:
            totals[check_status] += 1
        else:
            totals[check_status]  = 1
        #break
        
    return totals




def beautify_diff(ugly_diff):
    
    pretty_diff = "\n===\n" + ugly_diff + "\n===\n"   # Break into separate section

    string_mapping = {"\n"  : "!~" ,  # Hide newline to prevent wrong yaml interpretation
                      "\\\"" : ""  ,  # Remove Double quote with backslash
                      "\\\'" : ""  ,  # Remove Single quote with backslash
                      "\""   : ""  ,  # Remove Double quote
                      "\'"   : ""  ,  # Remove Single quote
                      }
    for char_before, char_after in string_mapping.items():  
        pretty_diff = pretty_diff.replace( char_before, char_after )
        
    return pretty_diff


    
def check_differences(dataset_before, dataset_after):
    
    # Must compare sorted versions of json struct
    if ordered_json(dataset_after) == ordered_json(dataset_before):
        compare_status = "Identical"
        udiff_output = ''
    else:
        compare_status = "Changed"

        # Analyze difference only if changed
        
        if debug: print("\nStarting json_delta.udiff()" 
                        + " for " + dataset_before['identifier'] 
                        + " at "+ str(datetime.datetime.utcnow())
                       )

        udiff_list   = json_delta.udiff(dataset_before, dataset_after)
        udiff_output = '\n'.join(udiff_list)
        udiff_output = beautify_diff(udiff_output)
        

        if debug: print("Finished json_delta.udiff()"
                        + " at "+ str(datetime.datetime.utcnow()) +"\n")
    
    return (compare_status, udiff_output)



def get_comparison_diffs(dataset_list_before, dataset_list_after):

    json_compare_dict = {}
    dataset_list_diff = {"Counts":{"Added":0, "Deleted":0, "Changed":0, "Identical":0, "Duplicate":0},
                         "Diff":""}    

    
    #=== First load the "after" values ===
    keys_after     = []
    for index_after, dataset_after in enumerate(dataset_list_after):

        if not 'identifier' in dataset_after: continue

        check_key = dataset_after['identifier']
        
        #: Handle errors of duplicates
        if check_key in keys_after:
            compare_status = "Duplicate"
            json_compare_dict["Duplicate_"+check_key] = {
                                            'Status' : "Duplicate",
                                            'After'  : dataset_after,
                                           }
            dataset_list_diff["Counts"]["Duplicate"] += 1
            continue   # Don't add duplicate key
        keys_after.append(check_key)

        json_compare_dict[check_key] = {'Status'    : "Added",
                                        'After'     : dataset_after,
                                       }
        dataset_list_diff["Counts"]["Added"] += 1

        
        
    #=== Second load the "before" values ===
    keys_before     = []
    for index_before, dataset_before in enumerate(dataset_list_before):

        if debug: print_same_line(index_before)


        if not 'identifier' in dataset_before:
            print("Error: No identifier")
            continue


        check_key = dataset_before['identifier']

        
        #: Handle errors of duplicates
        if check_key in keys_before:
            continue   # Don't do anything with duplicate key
        keys_before.append(check_key)
        
        
        if check_key in json_compare_dict:

            # Not deleted, so check for differences
            dataset_after = json_compare_dict[check_key]['After']
            compare_status, compare_diff = check_differences(dataset_before, dataset_after)

            dataset_list_diff["Counts"][compare_status] += 1
            dataset_list_diff["Counts"]["Added"    ]    -= 1  # Reverses initial add for all records 

            #=== Write key entry to file only if there was a change === 
            if   compare_status == "Identical":
                json_compare_dict.pop(check_key, None)   # Delete previously written key
            else:
                # compare_status == "Changed"
                json_compare_dict[check_key] = {'Status'     : compare_status,
                                                'Difference' : compare_diff,
                                               }

        else:
            # Deleted
            compare_status = "Deleted"
            dataset_list_diff["Counts"][compare_status] += 1
            
            json_compare_dict[check_key] = {'Status' : compare_status,
                                            'Before' : dataset_before,
                                           }

        if debug: print("\n"+compare_status) 
    
    # Add on dictionary of changes
    dataset_list_diff["Diff"] = json_compare_dict
    
    return dataset_list_diff

    

def save_json_diff(comparison_diffs, file_start, file_end, file_format = 'json'):

    date_start = parse_date(file_start)
    date_end   = parse_date(file_end  )
    file_name  = './generated/dataset_diff_' + date_start + '_' + date_end + '.' + file_format

    # Replace the literals in compare and write to file
    if file_format == 'json':
        diffs_dump = json.dumps(comparison_diffs, indent=4, sort_keys=True)
    else:  # yaml
        diffs_dump = yaml.dump(comparison_diffs, default_flow_style=False, explicit_start=True)

    diffs_dump = diffs_dump.replace("!~","\n")   # Hidden newline
    with open(file_name, "w") as text_file:
        print("{}".format(diffs_dump), file=text_file)

  
    
    

# Returns result from two most recent dates
def main(max_load=2):
    file_list      = get_file_list(max_load)
    print("\n\nfile_list")
    print(file_list)
    
    json_data_list = load_file_list(file_list)
    for i in range(max_load-1):
        print("Sizes:")
        print(len(json_data_list[i+1]), len(json_data_list[i]))
                                        
        comparison_diffs = get_comparison_diffs(json_data_list[i+1], json_data_list[i])
                                        
        #print(len(comparison_diffs))
                                        
        save_json_diff(        comparison_diffs,file_list     [i+1], file_list     [i], 'json')
        save_json_diff(        comparison_diffs,file_list     [i+1], file_list     [i], 'yaml')

        print("\n\nsave_json_diff(        comparison_diffs,file_list     [i+1], file_list     [i], 'json')")
        print(str(i)+": "+file_list     [i+1]  + " ---" +  file_list     [i] + " ---" + 'json')
        print("Done")
        