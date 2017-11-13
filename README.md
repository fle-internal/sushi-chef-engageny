# sushi-chef-engageny

Chef script for common core material from engageny.org

Dependencies:
Google Translation API (https://cloud.google.com/translate/docs/)

Install
-------

    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements.txt



Run
---
    export GOOGLE_APPLICATION_CREDENTIALS=<path_to_service_account_file>
    source venv/bin/activate
    ./engageny_chef.py -v --reset --token=<YOURTOKEN>

