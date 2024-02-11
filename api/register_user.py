'''
Add a user to the auth.db
'''

import api_utils
from app import app

app.app_context().push()
api_utils.signup_user(name = "wetterklima_app", pw = "dfdjkd739dKJouqb") # Dont use umlauts

api_utils.get_all_users()

#api_utils.delete_all_users()
