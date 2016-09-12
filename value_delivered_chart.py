#!/usr/bin/env python
######################
##  2015-11-25  Created by David Portnoy
######################

# For APIs
import json
import requests

# For data manipulation
import pandas as pd
from numpy import random  # For random

# For charting
from bokeh.charts import Bar, output_file, reset_output, output_server, save, show
#from bokeh.plotting import output_file, reset_output, save, show
#from bokeh.plotting import figure
#from bokeh.plotting import reset_output
from bokeh.palettes import brewer
from bokeh.sampledata.autompg import autompg as df
from bokeh.io import output_notebook  # For interactive within iPython notebook only

import getopt, sys  # For command line args



#--- Global constants ---
OPTS = [("h","help"), ("o:","output="), ("u","url="), ("v", "verbose")]



#==============================================================================
def usage():
    print("This script generates a graph from GitHub issues")
    print("  Params: ")
    print(str(OPTS).replace('), (','\n').replace(', ',' or ').strip('[]()'))


#==============================================================================
def process_params(sys_argv):
    try:
        opts, args = getopt.getopt(sys_argv, 
                                   "".join([opt[0] for opt in OPTS]),
                                    [opt[1] for opt in OPTS]
                                  )
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        getopt.usage()
        sys.exit(2)
    #--- Clear params vars ---
    global verbose
    global source_url
    global output_filename

    verbose           = False
    source_url        = ''
    output_filename   = ''


    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-o", "--output"):
            output_filename = a
        elif o in ("-u", "--url"):
            source_url = o
        else:
            assert False, "unhandled option"



#==============================================================================
def read_data(source_url="", verbose=False, read_limit=0):

    import os
    import glob

    DEFAULT_URL = "https://api.github.com/repos/demand-driven-open-data/ddod-intake/issues?state=all&per_page=10000"
    GITHUB_INTAKE_PATTERN = 'snapshots/*intake*.json'


    # A specified url overrides everything
    if source_url:
        if verbose: print("Loading from specified url: "+source_url)
        issues_get = requests.get(source_url)
        issues_string = issues_get.text
        issues_json = json.loads(issues_string)
    # If url not specified, get last file saved.  If no file found, load from default URL
    else:
        newest_file = max(glob.iglob(GITHUB_INTAKE_PATTERN), key = os.path.getctime)
        if newest_file:
            # Load the file
            with open(newest_file) as json_data_file:
                if verbose: print("Loading from file: "+newest_file.strip())
                issues_json = json.load(json_data_file)
                json_data_file.close()
        else:
            # Load from default URL
            source_url = DEFAULT_URL
            if verbose: print("Loading from default url: "+source_url)
            issues_get = requests.get(source_url)
            issues_string = issues_get.text
            issues_json = json.loads(issues_string)



    # Define constants
    IGNORE_LABEL = "Not Use Case"  # Ignore entries not use cases
    TABLE_COLUMNS = ['use_case_id','title','value_delivered','status']
    TABLE_COLUMNS_MAP = [{'use_case_id':'number'},
               {'title':'title'},
               {'value_delivered':'name'},
               {'status':'state'}]
    OWNER_LABEL = 'Owner:'
    STATE_LABEL = 'STATE:'
    VALUE_LABEL = 'VAL:'


    # Loop through items, building array of dictionaries
    issues_table = []

    for index, item in enumerate(issues_json):

        not_use_case = any(IGNORE_LABEL in ddd['name'] for ddd in item['labels'])
        if not_use_case:
            continue  # Don't add rows for this use case

        issue_row = {}
        issue_row.update({'use_case_id':item['number']})
        issue_row.update({'title':item['title']})
        issue_row.update({'status':item['state']})

        # Create list of labels
        for label in item["labels"]:
            if VALUE_LABEL in label["name"]:

                issue_row.update({'value_delivered':
                          label["name"].replace(VALUE_LABEL,'')
                          .replace(':','-').strip()})
                
                if read_limit: print('Appending: ',issue_row)
                issues_table.append(dict(issue_row))
            
        # Limit for testing
        if read_limit:
            if index+1 >= read_limit:
                break
        
    if read_limit: print(issues_table)


    # Create dataframe for Bokeh
    issues_df = pd.DataFrame(data=issues_table, columns=TABLE_COLUMNS)
    return issues_df


ISSUES_TITLE = "Number of Use Cases by Value delivered"
ISSUES_FILE  = "./generated/value_delivered.html"
WEB_SERVER_PATH = "~/htdocs/ddod_charts/value_delivered.html"
BOKEH_SERVER_IP = 'http://172.31.52.103:5006/'
DESTINATION_FRAME_WIDTH  = 524 
DESTINATION_FRAME_HEIGHT = 398
HTML_BODY_MARGIN = 8

#==============================================================================
def output_chart(issues_df,output_mode='static'):
    import datetime
    import bokeh
    from bokeh.models import HoverTool


    # Add timestamp to title
    
    issues_chart = Bar(issues_df, label='value_delivered', 
               values='status', agg='count', stack='status',
               title=ISSUES_TITLE+" (Updated "+datetime.datetime.now().strftime('%m/%d/%Y')+")", 
               xlabel="Value Delivered",ylabel="Number of Use Cases",
               legend='top_right',
               tools='hover',
               color=brewer["GnBu"][3]
              )

    issues_chart.plot_width  = DESTINATION_FRAME_WIDTH  - (HTML_BODY_MARGIN * 2)
    issues_chart.plot_height = DESTINATION_FRAME_HEIGHT - (HTML_BODY_MARGIN * 2)
    issues_chart.logo = None
    issues_chart.toolbar_location = None

    hover = issues_chart.select(dict(type=HoverTool))
    hover.tooltips = [ ("Value Delivered", "$x")]


    #--- Configure output ---
    reset_output()

    if output_mode == 'static':
        # Static file.  CDN is most space efficient
        output_file(ISSUES_FILE, title=ISSUES_TITLE, 
            autosave=False, mode='cdn', 
            root_dir=None
               )   # Generate file
        save(issues_chart,filename=ISSUES_FILE)
    elif output_mode == 'notebook':
        output_notebook()   # Show inline
        show(issues_chart)
    else:
        # Server (using internal server IP, rather than localhost or external)
        session = bokeh.session.Session(root_url = BOKEH_SERVER_IP, load_from_config=False)
        output_server("ddod_chart", session=session)
        show(issues_chart)


#==============================================================================
# Move files to the desired destination
def move_files():
    from shutil import copy
    from os     import path

    #Need to expand path, since shutil.copy doesn't process tilde (~)
    copy(path.expanduser(ISSUES_FILE), path.expanduser(WEB_SERVER_PATH))


#==============================================================================
def main(sys_argv):
    process_params(sys_argv)
    issues_df = read_data(source_url=source_url,verbose=verbose)
    output_chart(issues_df)
    move_files()


#==============================================================================
if __name__ == "__main__":
    main(sys.argv[1:])

