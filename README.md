# sushi-chef-engageny

Chef script for common core material from engageny.org

Dependencies:
  - Google Cloud Translation API (https://cloud.google.com/translate/docs/)


Install
-------

    apt-get install python3.4-gdbm
    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements.txt

You will also need to obtain the google cloud translation API credentials json file,
and save it under `credentials/engageny-support-1e2669e50f3f.json`.



Run
---
    export GOOGLE_APPLICATION_CREDENTIALS=credentials/engageny-support-1e2669e50f3f.json
    source venv/bin/activate
    ./engageny_chef.py -v --reset --token=<YOURTOKEN> lang=<LANG_CODE>

