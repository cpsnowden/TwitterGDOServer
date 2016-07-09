from flask_httpauth import HTTPBasicAuth
import flask_restful
import yaml

auth = HTTPBasicAuth()

with open("auth.yml", 'r') as config_file:
    cfg = yaml.load(config_file)

users = cfg["users"]


@auth.get_password
def get_pw(username):
    if username in users:
        return users.get(username)
    return None


class Resource(flask_restful.Resource):
    method_decorators = [auth.login_required]
