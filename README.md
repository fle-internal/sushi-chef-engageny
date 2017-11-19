# sushi-chef-engageny

Chef script for common core material from engageny.org

Dependencies:
Google Cloud Translation API (https://cloud.google.com/translate/docs/)

Install
-------

    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements.txt



Run
---
    export GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_SERVICE_ACCOUNT_FILE>
    source venv/bin/activate
    ./engageny_chef.py -v --reset --token=<YOURTOKEN> lang=<LANG_CODE>

